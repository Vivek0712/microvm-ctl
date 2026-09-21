"""`microvm.integrations.durable.lease_map` end to end with the AWS durable testing SDK.

Runs only where the durable SDK imports (Python 3.11+ with `microvm-ctl[durable]` and
aws-durable-execution-sdk-python-testing); the 3.9 CI leg skips it. FleetManager is
faked, including `plan`, so no AWS account is touched.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("aws_durable_execution_sdk_python")
pytest.importorskip("aws_durable_execution_sdk_python_testing")

from aws_durable_execution_sdk_python import DurableContext, durable_execution  # noqa: E402
from aws_durable_execution_sdk_python.execution import ErrorObject  # noqa: E402
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner  # noqa: E402

from microvm.integrations.durable import lease_map  # noqa: E402
from microvm.lease import LeasePolicy  # noqa: E402

POLICY = LeasePolicy(budget_s=4, heartbeat_timeout_s=2, slack_s=120)
SHARDS = [{"steps": ["echo 0"], "i": 0}, {"steps": ["echo 1"], "i": 1}]
# the SDK names wait_for_callback's inner callback "<name> create callback id"
APPROVAL_CB = "job-approval create callback id"


class _Vm:
    def __init__(self, i):
        self.microvm_id, self.endpoint = f"microvm-fake-{i}", f"fake{i}.lambda-microvm.us-east-1.on.aws"


class _Exceptions:
    class ResourceNotFoundException(Exception):
        pass


class FakePlan:
    """Stands in for `microvm.lease.LeasePlan`: the attributes `lease_map` reads."""

    def __init__(self, shards, concurrency, *, rejected=None, needs_approval=False):
        self.shards, self.concurrency = shards, concurrency
        self.rejected, self.needs_approval = rejected, needs_approval
        self.waves = -(-shards // concurrency) if concurrency else 0
        self.worst_case_usd = round(shards * 1020 * 2 * 0.0000175139, 2)

    def summary(self):
        return f"{self.shards} shards on 2 GB: {self.concurrency} at a time, {self.waves} waves"

    def to_dict(self):
        return {"shards": self.shards, "concurrency": self.concurrency, "waves": self.waves,
                "worst_case_usd": self.worst_case_usd, "needs_approval": self.needs_approval,
                "rejected": self.rejected, "summary": self.summary()}


class FakeFleetManager:
    api = type("Api", (), {"exceptions": _Exceptions})()

    def __init__(self):
        self.leases, self.terminated, self.plans = [], [], []
        self.concurrency, self.rejected, self.needs_approval = 4, None, False

    def plan(self, shards, baseline_mib, policy=None):
        self.plans.append((shards, baseline_mib, policy))
        return FakePlan(shards, self.concurrency, rejected=self.rejected, needs_approval=self.needs_approval)

    def lease(self, image, lease, task, policy=None, *, version=None, execution_role=None,
              ingress=None, egress=None):
        lease.validate()
        self.leases.append({"image": image, "lease": lease, "task": task, "policy": policy,
                            "version": version, "execution_role": execution_role})
        return _Vm(len(self.leases))

    def terminate(self, microvm_id):
        self.terminated.append(microvm_id)


FM = FakeFleetManager()
APPROVALS: list = []


def _approve(callback_id, plan):
    APPROVALS.append((callback_id, plan))


@durable_execution
def fanout(event: dict, context: DurableContext) -> dict:
    return lease_map(context, FM, "handoff-agent", event["shards"], policy=POLICY, label="job",
                     baseline_mib=2048, max_relaunches=0, approve=_approve if event.get("approve") else None,
                     approval_timeout_s=event.get("approval_timeout_s", 60), version="3")


@pytest.fixture(autouse=True)
def fm(monkeypatch):
    monkeypatch.setenv("MVM_REGION", "us-east-1")
    FM.__init__()
    APPROVALS.clear()
    return FM


def _outcome(res):
    return json.loads(res.result) if isinstance(res.result, str) else res.result


def _succeed(runner, cb, i):
    payload = {"microvm_id": f"microvm-fake-{i}", "lease_id": "x", "elapsed_s": 1.0,
               "result": {"passed": True, "shard": i}}
    runner.send_callback_success(cb, json.dumps(payload).encode())


def test_two_shards_happy_path(fm):
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"shards": SHARDS})
        cb0 = runner.wait_for_callback(arn, name="job-0-0-callback", timeout=30)
        cb1 = runner.wait_for_callback(arn, name="job-1-0-callback", timeout=30)
        assert cb0 != cb1
        assert fm.plans == [(2, 2048, POLICY)]  # the plan step ran once, with the reconstructed policy
        # map items launch concurrently, so compare as a set of shards, not in launch order
        assert sorted((launch["task"] for launch in fm.leases), key=lambda t: t["i"]) == SHARDS
        assert all(launch["version"] == "3" and launch["policy"] == POLICY for launch in fm.leases)
        _succeed(runner, cb0, 1)
        _succeed(runner, cb1, 2)
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "done" and out["succeeded"] == 2 and out["failed"] == 0 and out["errors"] == []
    assert out["plan"]["concurrency"] == 4
    assert out["plan"]["summary"].startswith("2 shards on 2 GB: 4 at a time")
    assert out["plan"]["rejected"] is None and out["plan"]["needs_approval"] is False
    assert sorted(o["result"]["shard"] for o in out["outcomes"]) == [1, 2]
    assert all(o["status"] == "done" and o["attempt"] == 0 for o in out["outcomes"])
    assert sorted(fm.terminated) == ["microvm-fake-1", "microvm-fake-2"]


def test_plan_concurrency_bounds_the_map(fm):
    fm.concurrency = 1
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"shards": SHARDS})
        cb0 = runner.wait_for_callback(arn, name="job-0-0-callback", timeout=30)
        with pytest.raises(TimeoutError):  # the second shard waits for the first (under the 2 s heartbeat)
            runner.wait_for_callback(arn, name="job-1-0-callback", timeout=0.5)
        assert len(fm.leases) == 1
        _succeed(runner, cb0, 1)
        cb1 = runner.wait_for_callback(arn, name="job-1-0-callback", timeout=30)
        _succeed(runner, cb1, 2)
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "done" and out["succeeded"] == 2 and out["plan"]["concurrency"] == 1


def test_one_shard_failing_is_counted_not_raised(fm):
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"shards": SHARDS})
        cb0 = runner.wait_for_callback(arn, name="job-0-0-callback", timeout=30)
        cb1 = runner.wait_for_callback(arn, name="job-1-0-callback", timeout=30)
        _succeed(runner, cb0, 1)
        completion = {"microvm_id": "microvm-fake-2", "lease_id": None, "elapsed_s": 2,
                      "error": {"error_type": "StepFailed", "message": "exit 1", "retryable": False,
                                "data": {"step": 0}}}
        runner.send_callback_failure(cb1, ErrorObject(
            message="exit 1", type="StepFailed", data=json.dumps(completion), stack_trace=None))
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "done" and out["succeeded"] == 1 and out["failed"] == 1
    by_status = {o["status"]: o for o in out["outcomes"]}
    assert set(by_status) == {"done", "failed"}
    assert by_status["failed"]["error"]["error_type"] == "StepFailed"
    assert out["errors"] == []  # the item returned an outcome; nothing raised
    assert sorted(fm.terminated) == ["microvm-fake-1", "microvm-fake-2"]


def test_rejected_plan_launches_nothing(fm):
    fm.rejected = "40 shards x 1020 s = 40800 VM-s exceeds max_vm_seconds 10000"
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        out = _outcome(runner.run({"shards": SHARDS}, timeout=30))
    assert out["status"] == "rejected"
    assert out["reason"] == fm.rejected and out["plan"]["rejected"] == fm.rejected
    assert fm.plans == [(2, 2048, POLICY)] and fm.leases == [] and fm.terminated == []


def test_approval_required_without_an_approver_launches_nothing(fm):
    fm.needs_approval = True
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        out = _outcome(runner.run({"shards": SHARDS}, timeout=30))
    assert out["status"] == "approval_required" and out["plan"]["needs_approval"] is True
    assert fm.leases == [] and APPROVALS == []


def test_approval_granted_then_fanout(fm):
    fm.needs_approval = True
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"shards": SHARDS, "approve": True})
        cb = runner.wait_for_callback(arn, name=APPROVAL_CB, timeout=30)
        assert len(APPROVALS) == 1
        approved_id, plan = APPROVALS[0]
        assert approved_id == cb  # the submitter got the callback id the approver must complete
        assert plan["needs_approval"] is True and plan["summary"].startswith("2 shards on 2 GB")
        assert fm.leases == []  # nothing launched while waiting
        runner.send_callback_success(cb, b'"approved by alice"')
        cb0 = runner.wait_for_callback(arn, name="job-0-0-callback", timeout=30)
        cb1 = runner.wait_for_callback(arn, name="job-1-0-callback", timeout=30)
        _succeed(runner, cb0, 1)
        _succeed(runner, cb1, 2)
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "done" and out["succeeded"] == 2 and len(fm.leases) == 2


def test_approval_refused_is_denied(fm):
    fm.needs_approval = True
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"shards": SHARDS, "approve": True})
        cb = runner.wait_for_callback(arn, name=APPROVAL_CB, timeout=30)
        runner.send_callback_failure(cb, ErrorObject(
            message="too expensive", type="Denied", data=None, stack_trace=None))
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "denied" and "too expensive" in out["reason"]
    assert fm.leases == [] and fm.terminated == []


def test_approval_timeout_is_denied(fm):
    fm.needs_approval = True
    runner = DurableFunctionTestRunner(handler=fanout, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"shards": SHARDS, "approve": True, "approval_timeout_s": 2})
        runner.wait_for_callback(arn, name=APPROVAL_CB, timeout=30)  # never answered
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "denied" and "no approval within 2s" in out["reason"]
    assert fm.leases == []
