"""Step Functions ASL and IAM generation, the lease CLI, and the log group fix (no AWS)."""

import json

import pytest

from microvm import cli
from microvm.integrations import iam_statements, lease_state_machine, orchestrator_statements
from microvm.integrations.stepfunctions import LIST, RUN_WAIT, TERMINATE
from microvm.lease import LeasePolicy

IMAGE = "arn:aws:lambda:us-east-1:123456789012:microvm-image:handoff-agent"
ROLE = "arn:aws:iam::123456789012:role/agent"


def _asl(**kw):
    return lease_state_machine(image_arn=IMAGE, execution_role_arn=ROLE, region="us-east-1", **kw)


# ---------------------------------------------------------------- ASL
def test_asl_is_jsonata_and_json_serialisable():
    asl = _asl()
    json.dumps(asl)  # no sets, no dataclasses
    assert asl["QueryLanguage"] == "JSONata"
    assert asl["StartAt"] == "Lease"
    assert set(asl["States"]) == {"Lease", "Terminate", "OnLeaseError", "TerminateFailed", "Reap",
                                  "TerminateStale", "Failed", "Done"}
    assert asl["States"]["Done"] == {"Type": "Succeed"}
    assert asl["States"]["Failed"]["Type"] == "Fail"
    assert asl["States"]["Failed"]["Error"] == "LeaseFailed"
    assert "$lease_error" in asl["States"]["Failed"]["Cause"]


def test_lease_state_resources_and_arguments():
    lease = _asl()["States"]["Lease"]
    assert lease["Type"] == "Task"
    assert lease["Resource"] == RUN_WAIT
    assert RUN_WAIT == "arn:aws:states:::aws-sdk:lambdamicrovms:runMicrovm.waitForTaskToken"
    args = lease["Arguments"]
    assert args["ImageIdentifier"] == IMAGE
    assert args["ExecutionRoleArn"] == ROLE
    assert args["IdlePolicy"] == {
        "MaxIdleDurationSeconds": 900, "SuspendedDurationSeconds": 60, "AutoResumeEnabled": False,
    }
    assert args["ClientToken"].startswith("{% $substring($states.context.Execution.Name & '-' & ")
    assert "$states.context.State.Name, 0, 128) %}" in args["ClientToken"]
    assert lease["Assign"] == {"vm": "{% $states.result.microvm_id %}"}
    assert lease["Next"] == "Terminate"
    assert lease["Retry"][0]["ErrorEquals"] == [
        "LambdaMicrovms.ThrottlingException", "LambdaMicrovms.ServiceQuotaExceededException",
    ]
    assert lease["Retry"][0]["BackoffRate"] == 2
    catch = lease["Catch"][0]
    assert catch["ErrorEquals"] == ["States.Timeout", "States.HeartbeatTimeout", "States.TaskFailed"]
    assert catch["Next"] == "OnLeaseError"
    asl = _asl()
    choice = asl["States"]["OnLeaseError"]
    assert choice["Type"] == "Choice" and choice["Default"] == "Reap"
    assert choice["Choices"][0]["Next"] == "TerminateFailed"
    assert "microvm_id" in choice["Choices"][0]["Condition"]
    tf = asl["States"]["TerminateFailed"]
    assert tf["Resource"] == asl["States"]["Terminate"]["Resource"]
    assert "$parse($lease_error.Cause).microvm_id" in tf["Arguments"]["MicrovmIdentifier"]
    assert tf["Next"] == "Reap" and tf["Catch"][0]["Next"] == "Reap"
    assert catch["Assign"] == {"lease_error": "{% $states.errorOutput %}"}


def test_run_hook_payload_builds_an_sfn_lease():
    expr = _asl(task_expr="$states.input.task")["States"]["Lease"]["Arguments"]["RunHookPayload"]
    assert expr.startswith("{% $string({") and expr.endswith("}) %}")
    assert "'kind': 'sfn'" in expr
    assert "'token': $states.context.Task.Token" in expr
    assert "'region': 'us-east-1'" in expr
    assert "'heartbeat_s': 30" in expr
    assert "'id': $states.context.Execution.Name" in expr
    assert "'task': $states.input.task" in expr
    # the default task expression is the execution input
    assert "'task': $states.input}" in _asl()["States"]["Lease"]["Arguments"]["RunHookPayload"]


def test_timeouts_follow_the_policy():
    asl = _asl(policy=LeasePolicy(budget_s=600, heartbeat_timeout_s=90, slack_s=60))
    lease = asl["States"]["Lease"]
    assert lease["TimeoutSeconds"] == 600
    assert lease["HeartbeatSeconds"] == 90
    assert lease["Arguments"]["MaximumDurationInSeconds"] == 660
    assert lease["Arguments"]["IdlePolicy"]["MaxIdleDurationSeconds"] == 600
    assert "- 660000]]" in asl["States"]["TerminateStale"]["Items"]
    capped = _asl(policy=LeasePolicy(budget_s=28_800, slack_s=600))["States"]["Lease"]
    assert capped["Arguments"]["MaximumDurationInSeconds"] == 28_800


def test_terminate_and_reap_states():
    states = _asl()["States"]
    term = states["Terminate"]
    assert term["Resource"] == TERMINATE == "arn:aws:states:::aws-sdk:lambdamicrovms:terminateMicrovm"
    assert term["Arguments"] == {"MicrovmIdentifier": "{% $vm %}"}
    assert term["Output"] == "{% $states.input %}"  # the VM's success payload passes through
    assert term["Catch"][0]["ErrorEquals"] == ["LambdaMicrovms.ResourceNotFoundException"]
    assert term["Catch"][0]["Next"] == "Done" and term["Next"] == "Done"
    reap = states["Reap"]
    assert reap["Resource"] == LIST == "arn:aws:states:::aws-sdk:lambdamicrovms:listMicrovms"
    assert reap["Arguments"] == {"ImageIdentifier": IMAGE}
    assert reap["Next"] == "TerminateStale"
    stale = states["TerminateStale"]
    assert stale["Type"] == "Map" and stale["Next"] == "Failed"
    assert "State in ['RUNNING', 'SUSPENDED', 'PENDING']" in stale["Items"]
    assert "$toMillis(StartedAt) < $toMillis($now()) - 1020000" in stale["Items"]
    one = stale["ItemProcessor"]["States"]["TerminateOne"]
    assert one["Resource"] == TERMINATE
    assert one["Arguments"] == {"MicrovmIdentifier": "{% $states.input.MicrovmId %}"}


def test_region_defaults_from_the_image_arn_and_name_is_honoured():
    asl = lease_state_machine(image_arn=IMAGE.replace("us-east-1", "eu-west-1"), execution_role_arn=ROLE,
                              name="Review")
    assert asl["StartAt"] == "Review"
    assert "'region': 'eu-west-1'" in asl["States"]["Review"]["Arguments"]["RunHookPayload"]


# ---------------------------------------------------------------- IAM
def test_iam_statements_per_kind():
    sm = "arn:aws:states:us-east-1:1:stateMachine:lease"
    assert iam_statements("sfn", orchestrator_arn=sm) == [{
        "Effect": "Allow",
        "Action": ["states:SendTaskSuccess", "states:SendTaskFailure", "states:SendTaskHeartbeat"],
        "Resource": sm,
    }]
    fn = "arn:aws:lambda:us-east-1:1:function:orchestrator"
    durable = iam_statements("durable", orchestrator_arn=fn)
    assert durable[0]["Resource"] == fn + ":*"
    assert durable[0]["Action"] == [
        "lambda:SendDurableExecutionCallbackSuccess", "lambda:SendDurableExecutionCallbackFailure",
        "lambda:SendDurableExecutionCallbackHeartbeat",
    ]
    assert iam_statements("durable", orchestrator_arn=fn + ":*")[0]["Resource"] == fn + ":*"
    sqs = iam_statements("sqs", orchestrator_arn="arn:aws:sqs:us-east-1:1:q")[0]
    assert sqs["Action"] == ["sqs:SendMessage"] and sqs["Resource"] == "arn:aws:sqs:us-east-1:1:q"
    assert iam_statements("eventbridge")[0] == {
        "Effect": "Allow", "Action": ["events:PutEvents"], "Resource": "*",
    }
    assert iam_statements("http") == [] and iam_statements("none") == []
    with pytest.raises(ValueError):
        iam_statements("carrier-pigeon")


def test_orchestrator_statements():
    stmts = orchestrator_statements(ROLE)
    assert stmts[0]["Action"] == [
        "lambda:RunMicrovm", "lambda:TerminateMicrovm", "lambda:ListMicrovms", "lambda:GetMicrovm",
    ]
    assert stmts[1] == {"Effect": "Allow", "Action": "iam:PassRole", "Resource": ROLE}
    assert orchestrator_statements()[1]["Resource"] == "*"
    assert stmts[2]["Action"] == "lambda:PassNetworkConnector"


# ---------------------------------------------------------------- CLI
def test_cli_lease_asl_prints_json_without_aws(capsys, monkeypatch):
    monkeypatch.delenv("MVM_REGION", raising=False)
    cli.main(["lease", "asl", "--image", "x", "--execution-role", "arn:aws:iam::1:role/r", "--budget", "900"])
    asl = json.loads(capsys.readouterr().out)
    lease = asl["States"]["Lease"]
    assert lease["Arguments"]["ImageIdentifier"] == "arn:aws:lambda:us-east-1:1:microvm-image:x"
    assert lease["Arguments"]["ExecutionRoleArn"] == "arn:aws:iam::1:role/r"
    assert lease["TimeoutSeconds"] == 900 and lease["HeartbeatSeconds"] == 120
    assert lease["Arguments"]["MaximumDurationInSeconds"] == 1020


def test_cli_lease_asl_requires_a_role(monkeypatch):
    monkeypatch.delenv("MVM_EXECUTION_ROLE_ARN", raising=False)
    with pytest.raises(SystemExit):
        cli.main(["lease", "asl", "--image", "x"])


def test_cli_lease_policy_prints_both_documents(capsys):
    cli.main(["lease", "policy", "--kind", "durable", "--orchestrator",
              "arn:aws:lambda:us-east-1:1:function:f", "--execution-role", ROLE])
    out = json.loads(capsys.readouterr().out)
    assert out["execution_role"]["Version"] == "2012-10-17"
    assert out["execution_role"]["Statement"][0]["Resource"] == "arn:aws:lambda:us-east-1:1:function:f:*"
    assert out["orchestrator_role"]["Statement"][1]["Resource"] == ROLE


class _Vm:
    microvm_id, state, endpoint = "microvm-1", "PENDING", "m1.lambda-microvm.us-east-1.on.aws"


class FakeFleetManager:
    calls = []

    def __init__(self, cfg):
        self.cfg = cfg

    def lease(self, image, lease, task, policy=None, **kw):
        self.calls.append((image, lease, task, policy, kw))
        return _Vm()

    def wait_until(self, microvm_id, state, timeout=120):
        vm = _Vm()
        vm.state = state
        return vm


def test_cli_lease_run_builds_a_lease_from_flags(monkeypatch, capsys):
    monkeypatch.setattr(cli, "FleetManager", FakeFleetManager)
    FakeFleetManager.calls.clear()
    cli.main(["lease", "run", "img", "--kind", "http", "--token", "t0k", "--target", "https://x/y",
              "--task", '{"steps": ["echo hi"]}', "--budget", "300", "--id", "manual", "--wait"])
    image, lease, task, policy, kw = FakeFleetManager.calls[0]
    assert image == "img" and lease.kind == "http" and lease.token == "t0k" and lease.target == "https://x/y"
    assert lease.id == "manual" and task == {"steps": ["echo hi"]}
    assert policy.budget_s == 300 and kw == {"version": None, "execution_role": None}
    out = capsys.readouterr().out
    assert "microvm-1" in out and "RUNNING" in out


def test_cli_lease_run_kind_none_needs_no_token(monkeypatch):
    monkeypatch.setattr(cli, "FleetManager", FakeFleetManager)
    FakeFleetManager.calls.clear()
    cli.main(["lease", "run", "img", "--kind", "none"])
    assert FakeFleetManager.calls[0][1].kind == "none"
    with pytest.raises(SystemExit):
        cli.main(["lease", "run", "img", "--kind", "sfn"])


SNAPSHOT = {
    "phase": "step 2/3", "started": 1.0, "elapsed_s": 12.5, "progress": {"done": 2, "total": 3},
    "counters": {"files": 4},
    "log_tail": [{"t": 1.0, "level": "info", "msg": "phase: step 2/3", "phase": "step 2/3"}],
    "lease": {"kind": "none", "id": None, "heartbeats": 0, "lost": False, "done": False, "error": None},
    "microvm_id": "microvm-1", "seq": 3,
}


class FakeEndpoint:
    def __init__(self, cfg, microvm_id, **kw):
        self.microvm_id = microvm_id

    def status(self, since=None):
        return SNAPSHOT

    def watch(self, timeout=600):
        yield {"t": 2.0, "level": "warning", "msg": "retrying", "phase": "step 3/3", "attempt": 2}
        yield {**SNAPSHOT, "phase": "step 3/3", "lease": {**SNAPSHOT["lease"], "done": True}}


def test_cli_status_and_watch_render_the_snapshot(monkeypatch, capsys):
    monkeypatch.setattr(cli, "EndpointClient", FakeEndpoint)
    cli.main(["status", "microvm-1"])
    out = capsys.readouterr().out
    assert "step 2/3" in out and "2/3" in out and "files" in out and "phase: step 2/3" in out
    cli.main(["watch", "microvm-1", "--timeout", "5"])
    out = capsys.readouterr().out
    assert "retrying" in out and "step 3/3" in out


# ---------------------------------------------------------------- log group fix
class FakeLogs:
    class exceptions:
        class ResourceNotFoundException(Exception):
            pass

    def __init__(self, existing):
        self.existing, self.asked = existing, []

    def filter_log_events(self, logGroupName, **kw):
        self.asked.append(logGroupName)
        if logGroupName not in self.existing:
            raise self.exceptions.ResourceNotFoundException()
        return {"events": [{"logStreamName": "s1", "timestamp": 1, "message": "hello\n"}]}


def _monitor(existing):
    from microvm.monitor import FleetMonitor
    mon = FleetMonitor.__new__(FleetMonitor)
    mon.logs = FakeLogs(existing)
    return mon


def test_tail_logs_tries_the_service_group_then_the_old_name():
    mon = _monitor({"/aws/lambda-microvms/img"})
    assert mon.tail_logs("img") == [{"stream": "s1", "ts": 1, "message": "hello"}]
    assert mon.logs.asked == ["/aws/lambda-microvms/img"]
    mon = _monitor({"/aws/lambda/microvms/img"})
    assert mon.tail_logs("img")[0]["message"] == "hello"
    assert mon.logs.asked == ["/aws/lambda-microvms/img", "/aws/lambda/microvms/img"]
    assert _monitor(set()).tail_logs("img") == []


def test_bootstrap_grants_logs_on_both_prefixes(monkeypatch):
    from microvm import bootstrap as bs
    from microvm.config import PlaneConfig

    policies = {}

    class Iam:
        class exceptions:
            class NoSuchEntityException(Exception):
                pass

        def get_role(self, RoleName):
            return {"Role": {"Arn": f"arn:aws:iam::1:role/{RoleName}"}}

        def put_role_policy(self, RoleName, PolicyName, PolicyDocument):
            policies[RoleName] = json.loads(PolicyDocument)

    class Sts:
        def get_caller_identity(self):
            return {"Account": "1"}

    class S3:
        def head_bucket(self, Bucket):
            return {}

    clients = {"sts": Sts(), "s3": S3(), "iam": Iam()}
    monkeypatch.setattr(bs, "lambda_client", lambda service, region, profile: clients[service])
    out = bs.bootstrap(PlaneConfig(region="us-east-1", profile=None, artifact_bucket="b"))
    assert out["execution_role_arn"].endswith("microvm-ctl-execution-role")
    stmt = policies["microvm-ctl-execution-role"]["Statement"][0]
    assert stmt["Resource"] == [
        "arn:aws:logs:us-east-1:1:log-group:/aws/lambda-microvms/*",
        "arn:aws:logs:us-east-1:1:log-group:/aws/lambda/microvms/*",
    ]


# ---------------------------------------------------------------- durable import guard
def test_durable_module_imports_without_the_sdk():
    from microvm.integrations import durable

    assert callable(durable.lease_microvm) and callable(durable.lease_with_relaunch)
    if durable._SDK_ERROR is not None:
        with pytest.raises(ImportError, match=r"microvm-ctl\[durable\]"):
            durable.lease_microvm(None, None, "img", {})

    class Ctx:
        class execution_context:
            durable_execution_arn = "arn:aws:lambda:eu-west-1:1:function:f:1/durable-execution/run-7"

    assert durable.function_region(Ctx()) == "eu-west-1"
    assert durable.execution_name(Ctx()) == "run-7"
