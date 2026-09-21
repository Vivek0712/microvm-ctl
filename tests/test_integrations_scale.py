"""Step Functions Map fan-out emitter (`FanoutSpec`) and the durable plan view (no AWS)."""

import dataclasses
import json

from microvm.integrations import FanoutSpec, lease_state_machine
from microvm.integrations.durable import plan_dict
from microvm.integrations.stepfunctions import LIST, RUN_WAIT, SNS_WAIT, TERMINATE
from microvm.lease import LeasePolicy

IMAGE = "arn:aws:lambda:us-east-1:123456789012:microvm-image:handoff-agent"
ROLE = "arn:aws:iam::123456789012:role/agent"
TOPIC = "arn:aws:sns:us-east-1:1:t"
LEASE_STATES = {"Lease", "Terminate", "OnLeaseError", "TerminateFailed", "Reap", "TerminateStale", "Failed",
                "Done"}

def _indexed(expr: str) -> str:
    return "{% $map(" + expr + ", function($v, $i) { {'index': $i, 'task': $v} }) %}"



def _asl(**kw):
    return lease_state_machine(image_arn=IMAGE, execution_role_arn=ROLE, region="us-east-1", **kw)


def _map():
    return _asl(fanout=FanoutSpec(max_concurrency=4))


def _gated():
    return _asl(fanout=FanoutSpec(max_concurrency=4, approval_topic_arn=TOPIC, approve_above_shards=8))


# ---------------------------------------------------------------- the spec
def test_fanout_spec_defaults():
    spec = FanoutSpec()
    assert spec.items_expr == "$states.input.shards" and spec.max_concurrency == 4
    assert spec.approval_topic_arn is None and spec.approve_above_shards is None
    assert spec.approval is False
    assert FanoutSpec(approval_topic_arn=TOPIC).approval is True


# ---------------------------------------------------------------- the Map
def test_single_lease_output_is_unchanged_when_fanout_is_none():
    assert _asl(fanout=None) == _asl()
    single = _asl()
    assert single["StartAt"] == "Lease" and set(single["States"]) == LEASE_STATES
    assert "Map.Item" not in json.dumps(single)


def test_map_machine_shape():
    asl = _map()
    json.dumps(asl)
    assert asl["QueryLanguage"] == "JSONata"
    assert asl["StartAt"] == "Fanout"
    assert set(asl["States"]) == {"Fanout", "Done"}  # the Gate exists only with approval
    assert asl["States"]["Done"] == {"Type": "Succeed"}
    assert "4 at a time" in asl["Comment"] and "approval" not in asl["Comment"]
    fanout = asl["States"]["Fanout"]
    assert fanout["Type"] == "Map"
    assert fanout["Items"] == _indexed("$states.input.shards")
    assert fanout["MaxConcurrency"] == 4
    assert fanout["Next"] == "Done"
    proc = fanout["ItemProcessor"]
    assert proc["ProcessorConfig"] == {"Mode": "INLINE"}
    assert proc["StartAt"] == "Lease"
    # the shard's terminal state is renamed: state names must be unique across the whole machine
    assert set(proc["States"]) == (LEASE_STATES - {"Done"}) | {"ShardDone"}


def test_map_items_expr_and_concurrency_are_honoured():
    fanout = _asl(fanout=FanoutSpec(items_expr="$states.input.jobs", max_concurrency=12))["States"]["Fanout"]
    assert fanout["Items"] == _indexed("$states.input.jobs") and fanout["MaxConcurrency"] == 12


def test_map_item_processor_is_the_lease_flow_with_the_item_as_task():
    proc = _map()["States"]["Fanout"]["ItemProcessor"]["States"]
    single = _asl()["States"]
    lease = proc["Lease"]
    assert lease["Resource"] == RUN_WAIT
    payload = lease["Arguments"]["RunHookPayload"]
    assert "'task': $states.input.task}" in payload  # each Map item is the task
    assert "'token': $states.context.Task.Token" in payload
    assert ("'id': $states.context.Execution.Name & '-' & $string($states.input.index)}"
            in payload)
    assert lease["Arguments"]["ClientToken"] == (
        "{% $substring($states.context.Execution.Name & '-' & $states.context.State.Name & '-' & "
        "$string($states.input.index), 0, 128) %}"
    )
    # everything else about the lease is the single-lease flow
    varying = {"RunHookPayload", "ClientToken"}
    assert {k: v for k, v in lease["Arguments"].items() if k not in varying} == {
        k: v for k, v in single["Lease"]["Arguments"].items() if k not in varying
    }
    assert {k: v for k, v in lease.items() if k != "Arguments"} == {
        k: v for k, v in single["Lease"].items() if k != "Arguments"
    }
    for state in ("Terminate", "Reap", "TerminateStale", "Failed"):
        assert json.loads(json.dumps(proc[state]).replace("ShardDone", "Done")) == single[state]
    assert proc["ShardDone"] == {"Type": "Succeed"}
    assert proc["Terminate"]["Resource"] == TERMINATE and proc["Reap"]["Resource"] == LIST
    assert "$toMillis(StartedAt) < $toMillis($now()) - 1020000" in proc["TerminateStale"]["Items"]


def test_map_ignores_task_expr_and_follows_the_policy():
    asl = _asl(task_expr="$states.input.task", heartbeat_s=15, name="Go",
               policy=LeasePolicy(budget_s=300, heartbeat_timeout_s=90, slack_s=60),
               fanout=FanoutSpec(max_concurrency=2))
    proc = asl["States"]["Fanout"]["ItemProcessor"]
    assert proc["StartAt"] == "Go" and "Go" in proc["States"]
    go = proc["States"]["Go"]
    assert "'task': $states.input.task}" in go["Arguments"]["RunHookPayload"]
    assert "'heartbeat_s': 15" in go["Arguments"]["RunHookPayload"]
    assert go["TimeoutSeconds"] == 300 and go["HeartbeatSeconds"] == 90
    assert go["Arguments"]["MaximumDurationInSeconds"] == 360
    assert "- 360000]]" in proc["States"]["TerminateStale"]["Items"]


# ---------------------------------------------------------------- the Gate
def test_gate_only_when_approval_is_configured():
    asl = _gated()
    json.dumps(asl)
    assert asl["StartAt"] == "Gate"
    assert set(asl["States"]) == {"Gate", "RequestApproval", "Denied", "Fanout", "Done"}
    assert "approval above 8 shards" in asl["Comment"]
    gate = asl["States"]["Gate"]
    assert gate["Type"] == "Choice"
    assert gate["Choices"] == [
        {"Condition": "{% $count($states.input.shards) > 8 %}", "Next": "RequestApproval"},
    ]
    assert gate["Default"] == "Fanout"


def test_request_approval_publishes_the_task_token_and_times_out_to_denied():
    asl = _gated()
    req = asl["States"]["RequestApproval"]
    assert req["Type"] == "Task"
    assert req["Resource"] == SNS_WAIT == "arn:aws:states:::sns:publish.waitForTaskToken"
    assert req["Arguments"]["TopicArn"] == TOPIC
    msg = req["Arguments"]["Message"]
    assert msg.startswith("{% $string({") and msg.endswith("}) %}")
    assert "'shards': $count($states.input.shards)" in msg
    assert f"'image': '{IMAGE}'" in msg
    assert "'execution': $states.context.Execution.Name" in msg
    assert "'task_token': $states.context.Task.Token" in msg
    assert req["TimeoutSeconds"] == 3600
    assert req["Catch"] == [{"ErrorEquals": ["States.Timeout", "States.TaskFailed"], "Next": "Denied"}]
    assert req["Output"] == "{% $states.input %}"  # the Map still sees the execution input
    assert req["Next"] == "Fanout"
    denied = asl["States"]["Denied"]
    assert denied["Type"] == "Fail" and denied["Error"] == "ApprovalDenied"
    assert asl["States"]["Fanout"]["MaxConcurrency"] == 4


def test_approval_topic_without_threshold_gates_every_fanout():
    asl = _asl(fanout=FanoutSpec(approval_topic_arn=TOPIC))
    assert asl["States"]["Gate"]["Choices"][0]["Condition"] == "{% $count($states.input.shards) > 0 %}"
    assert asl["States"]["RequestApproval"]["Arguments"]["Message"].count("$states.input.shards") == 1


def test_gate_condition_follows_a_custom_items_expr():
    asl = _asl(fanout=FanoutSpec(items_expr="$states.input.jobs", approval_topic_arn=TOPIC,
                                 approve_above_shards=2))
    assert asl["States"]["Gate"]["Choices"][0]["Condition"] == "{% $count($states.input.jobs) > 2 %}"
    assert "'shards': $count($states.input.jobs)" in asl["States"]["RequestApproval"]["Arguments"]["Message"]


# ---------------------------------------------------------------- the durable plan view
@dataclasses.dataclass
class _Limit:
    limit: int
    reason: str


@dataclasses.dataclass
class _Plan:
    shards: int
    concurrency: int
    limit: _Limit
    needs_approval: bool = False
    rejected: "str | None" = None
    worst_case_usd: float = 0.29

    def summary(self) -> str:
        return f"{self.shards} shards: {self.concurrency} at a time"


class _DictPlan:
    rejected = None
    needs_approval = True
    concurrency = 2

    def to_dict(self):
        return {"shards": 3, "concurrency": 2, "summary": "3 shards: 2 at a time"}


def test_plan_dict_keeps_the_fields_lease_map_reads_back():
    d = plan_dict(_Plan(shards=8, concurrency=4, limit=_Limit(4, "memory quota 8 GB / 2 GB baseline")))
    json.dumps(d)
    assert d["shards"] == 8 and d["concurrency"] == 4 and d["rejected"] is None
    assert d["needs_approval"] is False
    assert d["limit"] == {"limit": 4, "reason": "memory quota 8 GB / 2 GB baseline"}
    assert d["summary"] == "8 shards: 4 at a time"
    rejected = plan_dict(_Plan(shards=0, concurrency=0, limit=_Limit(4, "default"), rejected="shards < 1"))
    assert rejected["rejected"] == "shards < 1"
    d = plan_dict(_DictPlan())
    assert d == {"shards": 3, "concurrency": 2, "summary": "3 shards: 2 at a time", "rejected": None,
                 "needs_approval": True}


def test_plan_dict_of_a_real_lease_plan_is_json_and_complete():
    from microvm.lease import plan_fanout

    plan = plan_fanout(8, 2048, LeasePolicy(approval_usd=0.1), memory_quota_gb=8.0, launch_rate=4.0)
    d = plan_dict(plan)
    json.dumps(d)
    assert d["concurrency"] == 4 and d["waves"] == 2 and d["rejected"] is None
    assert d["needs_approval"] is True and d["policy"]["approval_usd"] == 0.1
    assert d["limit"]["by_memory"] == 4 and d["summary"] == plan.summary()
