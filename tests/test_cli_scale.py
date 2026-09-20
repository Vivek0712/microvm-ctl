"""`mvm lease plan`, `mvm lease run --shards`, `mvm lease asl --map`, `mvm watch --image`, `mvm quotas`
tiers, all against fakes (no AWS). The Step Functions FanoutSpec and FleetMonitor.job_status are
replaced on their modules so these tests do not depend on the other halves of the feature."""

from __future__ import annotations

import json

import pytest

from microvm import cli
from microvm.lease import LeasePlanRejected, LeasePolicy, fanout_limit, plan_fanout

ENV_VARS = ("MVM_LEASE_BUDGET_S", "MVM_LEASE_HEARTBEAT_TIMEOUT_S", "MVM_LEASE_SLACK_S",
            "MVM_LEASE_MAX_CONCURRENCY", "MVM_LEASE_MAX_VM_SECONDS", "MVM_LEASE_APPROVAL_USD")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in ENV_VARS + ("MVM_REGION",):
        monkeypatch.delenv(var, raising=False)


class _Vm:
    def __init__(self, i):
        self.microvm_id, self.state = f"microvm-{i}", "PENDING"
        self.endpoint = f"m{i}.lambda-microvm.us-east-1.on.aws"


class FakeFleetManager:
    """Real plan arithmetic over a fake quota; records lease_many calls."""

    quota = 8.0
    rate = 4.0
    calls: list = []

    def __init__(self, cfg):
        self.cfg = cfg

    def fanout_limit(self, baseline_mib, policy=None):
        return fanout_limit(baseline_mib, policy or LeasePolicy(), memory_quota_gb=self.quota,
                            launch_rate=self.rate)

    def plan(self, shards, baseline_mib, policy=None):
        return plan_fanout(shards, baseline_mib, policy or LeasePolicy(), memory_quota_gb=self.quota,
                           launch_rate=self.rate)

    def lease_many(self, image, leases, tasks, policy=None, *, baseline_mib, **kw):
        plan = self.plan(len(leases), baseline_mib, policy)
        plan.check()
        if len(leases) > plan.concurrency:
            raise LeasePlanRejected(f"{len(leases)} leases exceed the concurrency limit {plan.concurrency}")
        self.calls.append({"image": image, "leases": leases, "tasks": tasks, "policy": policy,
                           "baseline_mib": baseline_mib, **kw})
        return [_Vm(i) for i in range(len(leases))]

    def wait_until(self, microvm_id, state, timeout=120):
        vm = _Vm(microvm_id.rsplit("-", 1)[-1])
        vm.state = state
        return vm


class FakeImageBuilder:
    baseline = 2048
    asked: list = []

    def __init__(self, cfg):
        pass

    def baseline_mib(self, name, version=None):
        self.asked.append((name, version))
        return self.baseline


@pytest.fixture
def fakes(monkeypatch):
    monkeypatch.setattr(cli, "FleetManager", FakeFleetManager)
    monkeypatch.setattr(cli, "ImageBuilder", FakeImageBuilder)
    monkeypatch.setattr(FakeFleetManager, "quota", 8.0)
    monkeypatch.setattr(FakeImageBuilder, "baseline", 2048)
    FakeFleetManager.calls.clear()
    FakeImageBuilder.asked.clear()
    return FakeFleetManager


# ---------------------------------------------------------------- mvm lease plan
def test_lease_plan_prints_the_table_and_exits_zero(fakes, capsys):
    cli.main(["lease", "plan", "--image", "x", "--shards", "8"])
    out = capsys.readouterr().out
    for needle in ("shards", "2048 MiB", "8 GB", "4/s", "memory quota 8 GB / 2 GB baseline", "waves",
                   "~8.5 s", "8160", "$0.2858", "approval needed", "rejected"):
        assert needle in out, needle
    assert "8 shards on 2 GB: 4 at a time" in out  # the summary sentence follows the table
    assert FakeImageBuilder.asked == [("x", None)]  # baseline read from the image's ACTIVE version


def test_lease_plan_json_and_explicit_baseline(fakes, capsys):
    cli.main(["lease", "plan", "--image", "x", "--shards", "8", "--baseline-mib", "512", "--json"])
    plan = json.loads(capsys.readouterr().out)
    assert plan["baseline_mib"] == 512 and plan["concurrency"] == 8 and plan["waves"] == 1
    assert plan["limit"]["by_memory"] == 16 and plan["rejected"] is None
    assert plan["summary"].startswith("8 shards on 0.5 GB: 8 at a time")
    assert FakeImageBuilder.asked == []  # --baseline-mib skips the image lookup


def test_lease_plan_exit_codes(fakes, capsys):
    FakeImageBuilder.baseline = 16384
    with pytest.raises(SystemExit) as e:
        cli.main(["lease", "plan", "--image", "x", "--shards", "2"])
    assert e.value.code == 2
    assert "exceeds the memory quota 8 GB" in capsys.readouterr().out
    FakeImageBuilder.baseline = 2048
    with pytest.raises(SystemExit) as e:
        cli.main(["lease", "plan", "--image", "x", "--shards", "8", "--approval-usd", "0.1", "--json"])
    assert e.value.code == 3
    plan = json.loads(capsys.readouterr().out)
    assert plan["needs_approval"] is True and plan["policy"]["approval_usd"] == 0.1
    with pytest.raises(SystemExit) as e:
        cli.main(["lease", "plan", "--image", "x", "--shards", "8", "--max-vm-seconds", "100"])
    assert e.value.code == 2


def test_lease_plan_policy_from_env_then_flags(fakes, capsys, monkeypatch):
    monkeypatch.setenv("MVM_LEASE_MAX_CONCURRENCY", "2")
    monkeypatch.setenv("MVM_LEASE_BUDGET_S", "300")
    cli.main(["lease", "plan", "--image", "x", "--shards", "8", "--json"])
    plan = json.loads(capsys.readouterr().out)
    assert plan["concurrency"] == 2 and plan["waves"] == 4
    assert plan["limit"]["reason"] == "policy max_concurrency 2"
    assert plan["worst_case_vm_seconds"] == 8 * (300 + 120)
    cli.main(["lease", "plan", "--image", "x", "--shards", "8", "--max-concurrency", "3", "--slack", "0",
              "--json"])
    plan = json.loads(capsys.readouterr().out)
    assert plan["concurrency"] == 3 and plan["worst_case_vm_seconds"] == 8 * 300


def test_lease_plan_unknown_quota_uses_the_documented_default(fakes, capsys):
    FakeFleetManager.quota = None
    cli.main(["lease", "plan", "--image", "x", "--shards", "20", "--json"])
    plan = json.loads(capsys.readouterr().out)
    assert plan["limit"]["memory_quota_gb"] is None and plan["concurrency"] == 8 and plan["waves"] == 3
    assert plan["limit"]["reason"] == "default 8: memory quota unknown and no policy max_concurrency"


# ---------------------------------------------------------------- mvm lease run --shards
def test_lease_run_shards_plans_then_launches_filled_tasks(fakes, capsys):
    cli.main(["lease", "run", "img", "--shards", "3", "--task-template",
              '{"steps": ["echo shard {i}"], "n": 7, "nested": {"k": "{i}"}}', "--id", "job-{i}", "--wait"])
    (call,) = FakeFleetManager.calls
    assert call["image"] == "img" and call["baseline_mib"] == 2048
    assert [lease.kind for lease in call["leases"]] == ["none"] * 3
    assert [lease.id for lease in call["leases"]] == ["job-0", "job-1", "job-2"]
    assert call["tasks"] == [{"steps": [f"echo shard {i}"], "n": 7, "nested": {"k": str(i)}}
                             for i in range(3)]
    assert call["policy"].budget_s == 900 and call["version"] is None and call["execution_role"] is None
    out = capsys.readouterr().out
    assert out.startswith("3 shards on 2 GB: 3 at a time")  # the plan summary comes first
    assert "shard 0: microvm-0" in out and "shard 2: microvm-2" in out
    assert "mvm watch --image img" in out


def test_lease_run_shards_refuses_above_the_limit(fakes, capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["lease", "run", "img", "--shards", "8", "--task-template", '{"steps": ["true"]}'])
    assert e.value.code == 2
    assert FakeFleetManager.calls == []
    captured = capsys.readouterr()
    assert "8 shards on 2 GB: 4 at a time" in captured.out
    assert "8 leases exceed the concurrency limit 4; launch in waves" in captured.err


def test_lease_run_shards_rejected_plan_and_approval(fakes, capsys):
    FakeImageBuilder.baseline = 16384
    with pytest.raises(SystemExit) as e:
        cli.main(["lease", "run", "img", "--shards", "2"])
    assert e.value.code == 2 and "rejected" in capsys.readouterr().out and FakeFleetManager.calls == []
    FakeImageBuilder.baseline = 2048
    with pytest.raises(SystemExit) as e:
        cli.main(["lease", "run", "img", "--shards", "2", "--approval-usd", "0.01"])
    assert e.value.code == 3 and FakeFleetManager.calls == []
    assert "needs approval" in capsys.readouterr().err
    cli.main(["lease", "run", "img", "--shards", "2", "--approval-usd", "0.01", "--approve"])
    assert len(FakeFleetManager.calls) == 1


def test_lease_run_shards_with_a_token_template(fakes):
    cli.main(["lease", "run", "img", "--shards", "2", "--kind", "sfn", "--token-template", "tok-{i}",
              "--baseline-mib", "1024", "--version", "4", "--execution-role", "arn:role"])
    (call,) = FakeFleetManager.calls
    assert [(lease.kind, lease.token) for lease in call["leases"]] == [("sfn", "tok-0"), ("sfn", "tok-1")]
    assert call["baseline_mib"] == 1024 and call["version"] == "4" and call["execution_role"] == "arn:role"
    assert call["tasks"] == [{}, {}]
    with pytest.raises(SystemExit):  # a kind with a token needs a template
        cli.main(["lease", "run", "img", "--shards", "2", "--kind", "sfn"])
    with pytest.raises(SystemExit):
        cli.main(["lease", "run", "img", "--shards", "0"])
    with pytest.raises(SystemExit):
        cli.main(["lease", "run", "img", "--shards", "2", "--task-template", "[1, 2]"])


def test_lease_run_single_lease_kind_defaults_to_none(fakes, monkeypatch):
    seen = []

    class SingleFM(FakeFleetManager):
        def lease(self, image, lease, task, policy=None, **kw):
            seen.append((lease, task, policy))
            return _Vm(0)

    monkeypatch.setattr(cli, "FleetManager", SingleFM)
    cli.main(["lease", "run", "img", "--task", '{"a": 1}'])
    lease, task, policy = seen[0]
    assert lease.kind == "none" and task == {"a": 1} and policy == LeasePolicy()


# ---------------------------------------------------------------- mvm lease asl --map
@pytest.fixture
def fake_sfn(monkeypatch):
    import microvm.integrations.stepfunctions as sfn

    class FakeFanoutSpec:
        def __init__(self, items_expr="$states.input.shards", max_concurrency=4, approval_topic_arn=None,
                     approve_above_shards=None):
            self.items_expr, self.max_concurrency = items_expr, max_concurrency
            self.approval_topic_arn, self.approve_above_shards = approval_topic_arn, approve_above_shards

    calls = []

    def fake_machine(**kw):
        calls.append(kw)
        return {"StartAt": "Gate" if kw.get("fanout") else "Lease", "States": {}}

    monkeypatch.setattr(sfn, "FanoutSpec", FakeFanoutSpec, raising=False)
    monkeypatch.setattr(sfn, "lease_state_machine", fake_machine)
    return calls


ROLE = "arn:aws:iam::123456789012:role/agent"


def test_lease_asl_map_passes_a_fanout_spec(fake_sfn, fakes, capsys):
    cli.main(["lease", "asl", "--image", "x", "--execution-role", ROLE, "--map", "--max-concurrency", "3",
              "--items-expr", "$states.input.jobs", "--approval-topic", "arn:aws:sns:us-east-1:1:ok",
              "--approve-above-shards", "10"])
    (kw,) = fake_sfn
    spec = kw["fanout"]
    assert (spec.items_expr, spec.max_concurrency) == ("$states.input.jobs", 3)
    assert (spec.approval_topic_arn, spec.approve_above_shards) == ("arn:aws:sns:us-east-1:1:ok", 10)
    assert kw["image_arn"] == "arn:aws:lambda:us-east-1:123456789012:microvm-image:x"
    captured = capsys.readouterr()
    assert json.loads(captured.out)["StartAt"] == "Gate"
    assert captured.err == ""  # nothing computed, nothing explained
    assert FakeImageBuilder.asked == []


def test_lease_asl_map_computes_max_concurrency_from_the_plane(fake_sfn, fakes, capsys):
    cli.main(["lease", "asl", "--image", "x", "--execution-role", ROLE, "--map"])
    (kw,) = fake_sfn
    assert kw["fanout"].max_concurrency == 4 and kw["fanout"].items_expr == "$states.input.shards"
    assert kw["fanout"].approval_topic_arn is None and kw["fanout"].approve_above_shards is None
    captured = capsys.readouterr()
    assert "MaxConcurrency 4 (memory quota 8 GB / 2 GB baseline; 2048 MiB baseline)" in captured.err
    assert FakeImageBuilder.asked == [("x", None)]
    # the policy's max_concurrency bounds it too, and --baseline-mib skips the image read
    FakeImageBuilder.asked.clear()
    FakeFleetManager.quota = 1.0
    cli.main(["lease", "asl", "--image", "x", "--execution-role", ROLE, "--map", "--baseline-mib", "512"])
    assert fake_sfn[-1]["fanout"].max_concurrency == 2 and FakeImageBuilder.asked == []
    err = capsys.readouterr().err
    assert "MaxConcurrency 2 (memory quota 1 GB / 0.5 GB baseline; 512 MiB baseline)" in err


def test_lease_asl_map_falls_back_when_the_plane_is_unreachable(fake_sfn, fakes, capsys, monkeypatch):
    class Broken(FakeImageBuilder):
        def baseline_mib(self, name, version=None):
            raise RuntimeError("no credentials")

    monkeypatch.setattr(cli, "ImageBuilder", Broken)
    cli.main(["lease", "asl", "--image", "x", "--execution-role", ROLE, "--map"])
    assert fake_sfn[-1]["fanout"].max_concurrency == 8
    err = capsys.readouterr().err
    assert "could not compute the fan-out limit (no credentials)" in err and "MaxConcurrency 8" in err


def test_lease_asl_without_map_does_not_pass_fanout(fake_sfn, capsys):
    cli.main(["lease", "asl", "--image", "x", "--execution-role", ROLE])
    (kw,) = fake_sfn
    assert "fanout" not in kw and kw["policy"] == LeasePolicy()
    assert json.loads(capsys.readouterr().out)["StartAt"] == "Lease"


# ---------------------------------------------------------------- mvm watch --image
def _row(i, phase, elapsed, done=False, lost=False, error=None, lease_id=None):
    return {"microvm_id": f"microvm-{i}", "lease_id": lease_id or f"job-{i}", "phase": phase,
            "progress": {"done": 1 if done else 0, "total": 2}, "elapsed_s": elapsed,
            "done": done, "lost": lost, "error": error}


class FakeMonitor:
    polls: list = []
    calls: list = []

    def __init__(self, cfg):
        pass

    def job_status(self, image, **kw):
        FakeMonitor.calls.append((image, kw))
        return FakeMonitor.polls.pop(0) if len(FakeMonitor.polls) > 1 else FakeMonitor.polls[0]


def test_watch_image_renders_rows_and_footer_until_all_done(monkeypatch, capsys):
    monkeypatch.setattr(cli, "FleetMonitor", FakeMonitor)
    FakeMonitor.calls = []
    FakeMonitor.polls = [
        [_row(1, "step 1/2", 4.0), _row(2, "step 2/2", 9.0), {"microvm_id": "microvm-3", "error": "timeout"}],
        [_row(1, "done", 12.0, done=True), _row(2, "done", 15.0, done=True, error="StepFailed: exit 1"),
         _row(3, "step 1/2", 3.0, lost=True)],
        [_row(1, "done", 12.0, done=True), _row(2, "done", 15.0, done=True),
         _row(3, "done", 20.0, done=True)],
    ]
    cli.main(["watch", "--image", "agent", "--interval", "0", "--timeout", "5", "--port", "9000"])
    out = capsys.readouterr().out
    assert FakeMonitor.calls[0] == ("agent", {"port": 9000}) and len(FakeMonitor.calls) == 3
    assert "fleet job: agent" in out and "microvm-1" in out and "job-2" in out
    assert "done 3/3, running 0, lost 0, slowest microvm-3 done 20 s" in out


def test_watch_image_footer_shapes():
    rows = [_row(1, "step 1/2", 4.0), _row(2, "step 2/2", 9.0), {"microvm_id": "microvm-3", "error": "x"}]
    assert cli.fleet_job_footer(rows) == ("done 0/3, running 2, lost 0, unreachable 1, "
                                          "slowest microvm-2 step 2/2 9 s")
    rows = [_row(1, "done", 12.0, done=True), _row(2, "step 2/2", 30.0, lost=True)]
    assert cli.fleet_job_footer(rows) == "done 1/2, running 0, lost 1, slowest microvm-2 step 2/2 30 s"
    assert cli.fleet_job_footer([]) == "done 0/0, running 0, lost 0"
    assert cli.fleet_job_finished([], seen_members=False) is False  # nothing RUNNING yet: keep polling
    assert cli.fleet_job_finished([], seen_members=True) is True  # members were there, now gone
    assert cli.fleet_job_finished([_row(1, "step", 1.0)], seen_members=True) is False
    assert cli.fleet_job_finished([_row(1, "done", 1.0, done=True)], seen_members=True) is True
    assert cli._row_state({"microvm_id": "m", "error": "x"}) == "[red]unreachable[/]"
    assert cli._row_state(_row(1, "done", 1.0, done=True, error="StepFailed")) == "[red]failed[/]"
    assert cli._row_state(_row(1, "s", 1.0, lost=True)) == "[yellow]lost[/]"
    assert cli._row_state(_row(1, "s", 1.0)) == "[cyan]running[/]"


def test_watch_image_stops_on_timeout_and_when_members_are_gone(monkeypatch, capsys):
    monkeypatch.setattr(cli, "FleetMonitor", FakeMonitor)
    FakeMonitor.calls = []
    FakeMonitor.polls = [[_row(1, "step 1/2", 4.0)], []]
    cli.main(["watch", "--image", "agent", "--interval", "0", "--timeout", "5"])
    assert len(FakeMonitor.calls) == 2  # seen once, then gone
    assert "done 0/0" in capsys.readouterr().out
    FakeMonitor.calls = []
    FakeMonitor.polls = [[_row(1, "step 1/2", 4.0)]]  # never finishes
    cli.main(["watch", "--image", "agent", "--interval", "0.01", "--timeout", "0.05"])
    assert 2 <= len(FakeMonitor.calls) <= 20


def test_watch_needs_an_id_or_an_image(monkeypatch):
    with pytest.raises(SystemExit):
        cli.main(["watch"])


# ---------------------------------------------------------------- mvm quotas tiers
def test_quotas_prints_one_line_per_tier(monkeypatch, capsys):
    monkeypatch.setattr(cli, "applied_quotas", lambda cfg: {"MaxMemoryGb": 8.0, "RunMicrovm": 1.0})
    cli.main(["quotas"])
    out = capsys.readouterr().out
    for line in ("0.5 GB images: 16 at once", "1 GB images: 8 at once", "2 GB images: 4 at once",
                 "4 GB images: 2 at once", "8 GB images: 1 at once"):
        assert line in out, line
    monkeypatch.setattr(cli, "applied_quotas", lambda cfg: {})
    cli.main(["quotas"])
    out = capsys.readouterr().out
    assert "memory quota unknown: fan-outs default to 8 at once" in out
    assert cli.fanout_tier_lines(3.0) == [
        "0.5 GB images: [bold]6[/] at once", "1 GB images: [bold]3[/] at once",
        "2 GB images: [bold]1[/] at once", "4 GB images: [bold]0[/] at once",
        "8 GB images: [bold]0[/] at once",
    ]
