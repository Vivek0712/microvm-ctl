"""`microvm.integrations.durable` end to end with the AWS durable testing SDK.

Runs only where the durable SDK imports (Python 3.11+ with `microvm-ctl[durable]`
and aws-durable-execution-sdk-python-testing); the 3.9 CI leg skips it. FleetManager
is faked, so no AWS account is touched.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("aws_durable_execution_sdk_python")
pytest.importorskip("aws_durable_execution_sdk_python_testing")

from aws_durable_execution_sdk_python import DurableContext, durable_execution  # noqa: E402
from aws_durable_execution_sdk_python.execution import ErrorObject  # noqa: E402
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner  # noqa: E402

from microvm.integrations.durable import lease_microvm, lease_with_relaunch  # noqa: E402
from microvm.lease import LeasePolicy  # noqa: E402

POLICY = LeasePolicy(budget_s=4, heartbeat_timeout_s=2, slack_s=120)
TASK = {"steps": ["echo hi"], "repo": "o/r"}


class _Vm:
    def __init__(self, i):
        self.microvm_id, self.endpoint = f"microvm-fake-{i}", f"fake{i}.lambda-microvm.us-east-1.on.aws"


class _Exceptions:
    class ResourceNotFoundException(Exception):
        pass


class FakeFleetManager:
    api = type("Api", (), {"exceptions": _Exceptions})()

    def __init__(self, missing_on_terminate=False):
        self.leases, self.terminated = [], []
        self.missing_on_terminate = missing_on_terminate

    def lease(self, image, lease, task, policy=None, *, version=None, execution_role=None,
              ingress=None, egress=None):
        lease.validate()
        self.leases.append({"image": image, "lease": lease, "task": task, "policy": policy,
                            "version": version, "execution_role": execution_role})
        return _Vm(len(self.leases))

    def terminate(self, microvm_id):
        self.terminated.append(microvm_id)
        if self.missing_on_terminate:
            raise _Exceptions.ResourceNotFoundException()


FM = FakeFleetManager()


@durable_execution
def single(event: dict, context: DurableContext) -> dict:
    return lease_microvm(context, FM, "handoff-agent", event["task"], policy=POLICY, label="job",
                         version="3", execution_role="arn:aws:iam::1:role/agent")


@durable_execution
def relaunching(event: dict, context: DurableContext) -> dict:
    return lease_with_relaunch(context, FM, "handoff-agent", event["task"], max_relaunches=1,
                               policy=POLICY, label="job")


@pytest.fixture(autouse=True)
def fm(monkeypatch):
    monkeypatch.setenv("MVM_REGION", "us-east-1")
    FM.leases.clear()
    FM.terminated.clear()
    FM.missing_on_terminate = False
    return FM


def _outcome(res):
    return json.loads(res.result) if isinstance(res.result, str) else res.result


def test_success_carries_the_lease_and_terminates_once(fm):
    runner = DurableFunctionTestRunner(handler=single)
    with runner:
        arn = runner.run_async({"task": TASK})
        cb = runner.wait_for_callback(arn, name="job-callback", timeout=30)
        launch = fm.leases[0]
        lease = launch["lease"]
        assert lease.kind == "durable" and lease.token == cb and lease.region == "us-east-1"
        assert lease.heartbeat_s == 30 and lease.id
        assert launch["image"] == "handoff-agent" and launch["task"] == TASK
        assert launch["policy"].budget_s == 4 and launch["policy"].max_duration() == 124
        assert launch["version"] == "3" and launch["execution_role"] == "arn:aws:iam::1:role/agent"
        runner.send_callback_heartbeat(cb)
        payload = {"microvm_id": "microvm-fake-1", "lease_id": lease.id, "elapsed_s": 1.5,
                   "result": {"passed": True, "steps": 1}}
        runner.send_callback_success(cb, json.dumps(payload).encode())
        out = _outcome(runner.wait_for_result(arn, timeout=30))
    assert out["status"] == "done" and out["retryable"] is False
    assert out["result"] == {"passed": True, "steps": 1}
    assert out["vm"] == {"microvm_id": "microvm-fake-1", "endpoint": "fake1.lambda-microvm.us-east-1.on.aws"}
    assert fm.terminated == ["microvm-fake-1"]


def test_failure_is_typed_and_a_missing_vm_on_terminate_is_fine(fm):
    fm.missing_on_terminate = True
    runner = DurableFunctionTestRunner(handler=single)
    with runner:
        arn = runner.run_async({"task": TASK})
        cb = runner.wait_for_callback(arn, name="job-callback", timeout=30)
        completion = {"microvm_id": "microvm-fake-1", "lease_id": None, "elapsed_s": 2,
                      "error": {"error_type": "StepFailed", "message": "exit 1", "retryable": False,
                                "data": {"step": 0}}}
        runner.send_callback_failure(cb, ErrorObject(
            message="exit 1", type="StepFailed", data=json.dumps(completion), stack_trace=None))
        out = _outcome(runner.wait_for_result(arn, timeout=30))
    assert out["status"] == "failed" and out["retryable"] is False
    assert out["error"] == {"error_type": "StepFailed", "message": "exit 1", "retryable": False,
                            "data": {"step": 0}}
    assert fm.terminated == ["microvm-fake-1"]


def test_retryable_failure_relaunches_once_then_fatal(fm):
    runner = DurableFunctionTestRunner(handler=relaunching)
    with runner:
        arn = runner.run_async({"task": TASK})
        cb1 = runner.wait_for_callback(arn, name="job-0-callback", timeout=30)
        runner.send_callback_failure(cb1, ErrorObject(
            message="clone timed out", type="CloneFailed",
            data=json.dumps({"error": {"error_type": "CloneFailed", "message": "clone timed out",
                                       "retryable": True, "data": {}}}), stack_trace=None))
        cb2 = runner.wait_for_callback(arn, name="job-1-callback", timeout=30)
        assert cb2 != cb1
        runner.send_callback_failure(cb2, ErrorObject(
            message="bandit crashed", type="ScanFailed",
            data=json.dumps({"error": {"error_type": "ScanFailed", "message": "bandit crashed",
                                       "retryable": False, "data": {}}}), stack_trace=None))
        out = _outcome(runner.wait_for_result(arn, timeout=30))
    assert out["status"] == "failed" and out["attempt"] == 1
    assert out["error"]["error_type"] == "ScanFailed" and out["error"]["retryable"] is False
    assert fm.terminated == ["microvm-fake-1", "microvm-fake-2"]


def test_failure_without_error_data_is_unknown_and_not_retried(fm):
    """The SDK does not surface ErrorObject.type; without ErrorData the type is Unknown."""
    runner = DurableFunctionTestRunner(handler=relaunching)
    with runner:
        arn = runner.run_async({"task": TASK})
        cb = runner.wait_for_callback(arn, name="job-0-callback", timeout=30)
        runner.send_callback_failure(cb, ErrorObject(
            message="bandit crashed", type="ScanFailed", data=None, stack_trace=None))
        out = _outcome(runner.wait_for_result(arn, timeout=30))
    assert out["status"] == "failed" and out["attempt"] == 0
    assert out["error"]["error_type"] == "Unknown" and out["error"]["message"] == "bandit crashed"
    assert fm.terminated == ["microvm-fake-1"]


def test_silent_vm_times_out_relaunches_then_gives_up(fm):
    runner = DurableFunctionTestRunner(handler=relaunching)
    with runner:
        arn = runner.run_async({"task": TASK})
        runner.wait_for_callback(arn, name="job-0-callback", timeout=30)  # never answered
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "timed_out" and out["attempt"] == 1 and out["retryable"] is True
    assert out["error"]["error_type"] == "CallbackTimeout"
    assert len(fm.leases) == 2 and fm.terminated == ["microvm-fake-1", "microvm-fake-2"]
