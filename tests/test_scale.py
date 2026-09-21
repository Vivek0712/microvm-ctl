"""Leases at scale, library side: plan arithmetic, rejection rules, policy env, lease_many, baseline."""

from __future__ import annotations

import threading
import time

import pytest

from microvm.config import PlaneConfig
from microvm.fleet import FleetManager
from microvm.images import DEFAULT_BASELINE_MIB, ImageBuilder, ImageBuildError
from microvm.lease import (
    DEFAULT_FANOUT_LIMIT,
    RESTORE_S,
    VM_USD_PER_GB_S,
    FanoutLimit,
    Lease,
    LeasePlan,
    LeasePlanRejected,
    LeasePolicy,
    fanout_limit,
    plan_fanout,
)


def _plan(shards=8, baseline=2048, policy=None, quota=8.0, rate=4.0):
    return plan_fanout(shards, baseline, policy or LeasePolicy(), memory_quota_gb=quota, launch_rate=rate)


# ---------------------------------------------------------------- constants
def test_vm_rate_reuses_the_monitor_constants():
    from microvm.monitor import RATE_GB_SECOND, RATE_VCPU_SECOND

    assert VM_USD_PER_GB_S == pytest.approx(RATE_VCPU_SECOND / 2 + RATE_GB_SECOND)
    assert RESTORE_S == 3.5 and DEFAULT_FANOUT_LIMIT == 8


# ---------------------------------------------------------------- fanout_limit
def test_fanout_limit_by_memory():
    lim = fanout_limit(2048, LeasePolicy(), memory_quota_gb=8, launch_rate=4.0)
    assert lim == FanoutLimit(baseline_mib=2048, memory_quota_gb=8, launch_rate=4.0, by_memory=4,
                              by_policy=None, limit=4, reason="memory quota 8 GB / 2 GB baseline")
    assert fanout_limit(512, LeasePolicy(), memory_quota_gb=8, launch_rate=4.0).by_memory == 16
    assert fanout_limit(3000, LeasePolicy(), memory_quota_gb=8, launch_rate=4.0).by_memory == 2  # floor


def test_fanout_limit_by_policy_and_default():
    lim = fanout_limit(2048, LeasePolicy(max_concurrency=3), memory_quota_gb=8, launch_rate=4.0)
    assert (lim.by_memory, lim.by_policy, lim.limit) == (4, 3, 3)
    assert lim.reason == "policy max_concurrency 3"
    # the memory bound wins when it is the smaller one, and on a tie
    assert fanout_limit(2048, LeasePolicy(max_concurrency=9), memory_quota_gb=8, launch_rate=4.0).limit == 4
    assert fanout_limit(2048, LeasePolicy(max_concurrency=4), memory_quota_gb=8,
                        launch_rate=4.0).reason.startswith("memory quota")
    # quota unknown: the policy if set, else the documented default 8
    assert fanout_limit(2048, LeasePolicy(max_concurrency=2), memory_quota_gb=None,
                        launch_rate=4.0).limit == 2
    lim = fanout_limit(2048, LeasePolicy(), memory_quota_gb=None, launch_rate=4.0)
    assert lim.by_memory is None and lim.by_policy is None and lim.limit == 8
    assert lim.reason == "default 8: memory quota unknown and no policy max_concurrency"
    with pytest.raises(ValueError):
        fanout_limit(0, LeasePolicy(), memory_quota_gb=8, launch_rate=4.0)


# ---------------------------------------------------------------- plan_fanout
def test_plan_arithmetic_matches_the_spec_example():
    plan = _plan(shards=8, baseline=2048, quota=8, rate=4.0)
    assert isinstance(plan, LeasePlan)
    assert plan.limit.by_memory == 4
    assert plan.concurrency == 4 and plan.waves == 2
    assert plan.launch_to_all_running_s == pytest.approx(((4 - 1) / 4.0 + 3.5) * 2)  # 8.5
    assert plan.worst_case_vm_seconds == 8 * 1020  # budget 900 + slack 120 per shard
    assert plan.worst_case_usd == pytest.approx(8160 * 2 * VM_USD_PER_GB_S, rel=1e-6)
    assert round(plan.worst_case_usd, 2) == 0.29
    assert plan.needs_approval is False and plan.rejected is None
    plan.check()  # does not raise


def test_plan_fewer_shards_than_the_limit_is_one_wave():
    plan = _plan(shards=3, quota=8, rate=0.8)
    assert plan.concurrency == 3 and plan.waves == 1
    assert plan.launch_to_all_running_s == pytest.approx(2 / 0.8 + 3.5)
    assert _plan(shards=1).launch_to_all_running_s == pytest.approx(RESTORE_S)
    assert _plan(shards=9, quota=8).waves == 3  # 4, 4, 1


def test_plan_more_shards_than_the_limit_is_waves_not_a_rejection():
    plan = _plan(shards=40, quota=8)
    assert plan.rejected is None and plan.concurrency == 4 and plan.waves == 10
    assert plan.worst_case_vm_seconds == 40 * 1020


@pytest.mark.parametrize("kw,needle", [
    (dict(shards=0), "shards must be at least 1, got 0"),
    (dict(shards=8, baseline=16384, quota=8), "baseline 16384 MiB exceeds the memory quota 8 GB"),
    (dict(shards=8, policy=LeasePolicy(max_vm_seconds=8000)), "worst case 8160 VM-seconds exceeds"),
    (dict(shards=8, policy=LeasePolicy(max_concurrency=0)), "policy max_concurrency is 0"),
])
def test_rejection_rules(kw, needle):
    plan = _plan(**kw)
    assert plan.rejected and needle in plan.rejected
    with pytest.raises(LeasePlanRejected, match=needle.replace("(", r"\(")):
        plan.check()
    assert "rejected" in plan.summary()


def test_rejection_order_and_non_rejections():
    # shards < 1 wins over an oversized baseline; the memory rule wins over max_vm_seconds
    assert _plan(shards=0, baseline=16384).rejected.startswith("shards must be")
    big = _plan(shards=8, baseline=16384, policy=LeasePolicy(max_vm_seconds=1))
    assert big.rejected.startswith("baseline")
    # max_vm_seconds exactly met is fine; unknown quota never rejects on memory
    assert _plan(shards=8, policy=LeasePolicy(max_vm_seconds=8160)).rejected is None
    assert _plan(shards=8, baseline=16384, quota=None).rejected is None


def test_needs_approval_and_summary():
    plan = _plan(shards=8, policy=LeasePolicy(approval_usd=0.10))
    assert plan.needs_approval is True and plan.rejected is None
    text = plan.summary()
    assert text.startswith("8 shards on 2 GB: 4 at a time (memory quota 8 GB / 2 GB baseline), 2 waves, ")
    assert "all running in ~8 s, worst case 8160 VM-s = $0.29" in text
    assert text.endswith(", needs approval (above $0.1)")
    assert _plan(shards=8, policy=LeasePolicy(approval_usd=1.0)).needs_approval is False
    assert "1 wave," in _plan(shards=2).summary()


def test_plan_to_dict_is_json_safe():
    import json

    d = _plan().to_dict()
    json.dumps(d)
    assert d["limit"]["limit"] == 4 and d["policy"]["budget_s"] == 900
    assert d["summary"] == _plan().summary()
    assert set(d) >= {"shards", "baseline_mib", "policy", "limit", "concurrency", "waves",
                      "launch_to_all_running_s", "worst_case_vm_seconds", "worst_case_usd",
                      "needs_approval", "rejected", "summary"}


# ---------------------------------------------------------------- LeasePolicy.from_env
def test_policy_from_env(monkeypatch):
    for var in ("MVM_LEASE_BUDGET_S", "MVM_LEASE_HEARTBEAT_TIMEOUT_S", "MVM_LEASE_SLACK_S",
                "MVM_LEASE_MAX_CONCURRENCY", "MVM_LEASE_MAX_VM_SECONDS", "MVM_LEASE_APPROVAL_USD"):
        monkeypatch.delenv(var, raising=False)
    assert LeasePolicy.from_env() == LeasePolicy()
    monkeypatch.setenv("MVM_LEASE_BUDGET_S", "600")
    monkeypatch.setenv("MVM_LEASE_HEARTBEAT_TIMEOUT_S", "60")
    monkeypatch.setenv("MVM_LEASE_SLACK_S", "30")
    monkeypatch.setenv("MVM_LEASE_MAX_CONCURRENCY", "6")
    monkeypatch.setenv("MVM_LEASE_MAX_VM_SECONDS", "50000")
    monkeypatch.setenv("MVM_LEASE_APPROVAL_USD", "2.5")
    assert LeasePolicy.from_env() == LeasePolicy(budget_s=600, heartbeat_timeout_s=60, slack_s=30,
                                                 max_concurrency=6, max_vm_seconds=50000, approval_usd=2.5)
    monkeypatch.setenv("MVM_LEASE_MAX_CONCURRENCY", "  ")  # blank counts as unset
    assert LeasePolicy.from_env().max_concurrency is None
    monkeypatch.setenv("MVM_LEASE_APPROVAL_USD", "lots")
    with pytest.raises(ValueError, match="MVM_LEASE_APPROVAL_USD='lots' is not a valid float"):
        LeasePolicy.from_env()


def test_policy_defaults_keep_the_0_2_shape():
    policy = LeasePolicy(budget_s=300)
    assert (policy.max_concurrency, policy.max_vm_seconds, policy.approval_usd) == (None, None, None)
    assert policy.max_duration() == 420
    assert policy.to_dict()["approval_usd"] is None
    assert LeasePolicy(**{"budget_s": 900, "heartbeat_timeout_s": 120, "slack_s": 120}) == LeasePolicy()


# ---------------------------------------------------------------- FleetManager
def _fm(quotas, run=None):
    fm = FleetManager.__new__(FleetManager)
    fm.cfg = PlaneConfig(region="us-east-1")
    fm.quotas = quotas
    if run is not None:
        fm._run = run
    return fm


def test_fleet_manager_plan_uses_the_quota_and_the_launch_rate():
    fm = _fm({"MaxMemoryGb": 8, "RunMicrovm": 1})
    lim = fm.fanout_limit(2048)
    assert lim.memory_quota_gb == 8 and lim.launch_rate == pytest.approx(0.8) and lim.limit == 4
    assert fm.fanout_limit(2048, LeasePolicy(max_concurrency=2)).limit == 2
    plan = fm.plan(8, 2048)
    assert plan.concurrency == 4 and plan.waves == 2
    assert plan.launch_to_all_running_s == pytest.approx((3 / 0.8 + 3.5) * 2)
    # quota unknown, published RunMicrovm default with headroom, default limit 8
    plan = _fm({}).plan(8, 2048)
    assert plan.limit.launch_rate == pytest.approx(4.0) and plan.concurrency == 8 and plan.waves == 1
    assert plan.limit.reason.startswith("default 8")


def test_lease_many_refuses_above_the_concurrency_limit(monkeypatch):
    monkeypatch.setattr("microvm.fleet.image_arn", lambda name, region, profile: f"arn:img:{name}")
    sent = []
    fm = _fm({"MaxMemoryGb": 8, "RunMicrovm": 5}, run=lambda **p: sent.append(p))
    leases = [Lease("none", id=f"s{i}") for i in range(5)]
    tasks = [{"i": i} for i in range(5)]
    with pytest.raises(LeasePlanRejected, match="5 leases exceed the concurrency limit 4; launch in waves"):
        fm.lease_many("img", leases, tasks, baseline_mib=2048)
    assert sent == []
    # a rejected plan is refused with the plan's own reason, before anything launches
    with pytest.raises(LeasePlanRejected, match="exceeds the memory quota"):
        fm.lease_many("img", leases[:1], tasks[:1], baseline_mib=16384)
    with pytest.raises(LeasePlanRejected, match="policy max_concurrency is 0"):
        fm.lease_many("img", leases[:1], tasks[:1], LeasePolicy(max_concurrency=0), baseline_mib=2048)
    with pytest.raises(ValueError, match="3 leases but 2 tasks"):
        fm.lease_many("img", leases[:3], tasks[:2], baseline_mib=2048)
    assert sent == []


def test_lease_many_launches_in_parallel_and_preserves_order(monkeypatch):
    monkeypatch.setattr("microvm.fleet.image_arn", lambda name, region, profile: f"arn:img:{name}")
    started, lock = [], threading.Lock()

    def fake_run(**params):
        payload = params["runHookPayload"]
        i = int(payload.split('"i":')[1].split("}")[0])
        with lock:
            started.append((i, threading.current_thread().name))
        time.sleep(0.05 * (4 - i))  # the first shard finishes last
        return {"microvmId": f"mvm-{i}", "state": "PENDING", "imageArn": params["imageIdentifier"],
                "imageVersion": "1"}

    fm = _fm({"MaxMemoryGb": 8, "RunMicrovm": 5}, run=fake_run)
    leases = [Lease("sfn", token=f"tok-{i}", id=f"s{i}") for i in range(4)]
    tasks = [{"i": i} for i in range(4)]
    policy = LeasePolicy(budget_s=300, slack_s=60)
    t0 = time.time()
    vms = fm.lease_many("img", leases, tasks, policy, baseline_mib=2048, version="7",
                        execution_role="arn:role", egress=["arn:egress"])
    assert time.time() - t0 < 0.4  # 0.2 + 0.15 + 0.1 + 0.05 sequential would be 0.5
    assert [vm.microvm_id for vm in vms] == ["mvm-0", "mvm-1", "mvm-2", "mvm-3"]  # input order kept
    assert sorted(i for i, _ in started) == [0, 1, 2, 3]
    assert len({name for _, name in started}) > 1  # more than one worker thread


def test_lease_many_carries_the_lease_call_shape(monkeypatch):
    monkeypatch.setattr("microvm.fleet.image_arn", lambda name, region, profile: f"arn:img:{name}")
    sent = []

    def fake_run(**params):
        sent.append(params)
        return {"microvmId": params["clientToken"][:8], "state": "PENDING",
                "imageArn": params["imageIdentifier"], "imageVersion": "1"}

    fm = _fm({"MaxMemoryGb": 8, "RunMicrovm": 5}, run=fake_run)
    leases = [Lease("durable", token=f"cb-{i}") for i in range(2)]
    fm.lease_many("img", leases, [{"a": 1}, {"a": 2}], LeasePolicy(budget_s=300, slack_s=60),
                  baseline_mib=2048, version="7", execution_role="arn:role", ingress=["arn:in"])
    from microvm.lease import client_token, encode_payload

    by_token = {p["clientToken"]: p for p in sent}
    for lease, task in zip(leases, [{"a": 1}, {"a": 2}]):
        p = by_token[client_token(lease)]
        assert p["runHookPayload"] == encode_payload(lease, task)
        assert p["maximumDurationInSeconds"] == 360 and p["imageVersion"] == "7"
        assert p["executionRoleArn"] == "arn:role" and p["ingressNetworkConnectors"] == ["arn:in"]
        assert p["idlePolicy"]["autoResumeEnabled"] is False


# ---------------------------------------------------------------- ImageBuilder.baseline_mib
class FakeImageApi:
    def __init__(self, image, versions, details):
        self.image, self.versions, self.details, self.asked = image, versions, details, []

    def get_microvm_image(self, imageIdentifier):
        return dict(self.image)

    def get_paginator(self, name):
        assert name == "list_microvm_image_versions"
        versions = self.versions

        class P:
            def paginate(self, **kw):
                return self

            def build_full_result(self):
                return {"items": versions}
        return P()

    def get_microvm_image_version(self, imageIdentifier, imageVersion):
        self.asked.append(imageVersion)
        return self.details[imageVersion]


def _builder(api):
    b = ImageBuilder.__new__(ImageBuilder)
    b.cfg = PlaneConfig(region="us-east-1")
    b.api = api
    b.arn = lambda name: f"arn:aws:lambda:us-east-1:1:microvm-image:{name}"
    return b


def test_baseline_mib_reads_the_latest_active_version():
    api = FakeImageApi({"latestActiveImageVersion": "3"}, [], {
        "3": {"resources": [{"minimumMemoryInMiB": 4096}]},
        "2": {"resources": [{"minimumMemoryInMiB": 512}]},
    })
    b = _builder(api)
    assert b.baseline_mib("img") == 4096 and api.asked == ["3"]
    assert b.baseline_mib("img", version="2") == 512 and api.asked == ["3", "2"]


def test_baseline_mib_falls_back_to_2048_with_a_note(caplog):
    api = FakeImageApi({"latestActiveImageVersion": "1"}, [], {"1": {"resources": []}, "9": {}})
    b = _builder(api)
    with caplog.at_level("INFO", logger="microvm.images"):
        assert b.baseline_mib("img") == DEFAULT_BASELINE_MIB == 2048
        assert b.baseline_mib("img", version="9") == 2048
    assert "assuming 2048 MiB" in caplog.text


def test_baseline_mib_scans_versions_when_the_image_has_no_active_pointer():
    versions = [
        {"imageVersion": "1", "status": "ACTIVE", "createdAt": "2026-01-01"},
        {"imageVersion": "2", "status": "INACTIVE", "createdAt": "2026-02-01"},
        {"imageVersion": "3", "status": "ACTIVE", "createdAt": "2026-03-01"},
    ]
    api = FakeImageApi({}, versions, {"3": {"resources": [{"minimumMemoryInMiB": 1024}]}})
    assert _builder(api).baseline_mib("img") == 1024 and api.asked == ["3"]
    with pytest.raises(ImageBuildError, match="no ACTIVE version"):
        _builder(FakeImageApi({}, [versions[1]], {})).baseline_mib("img")


def test_package_exports():
    import microvm

    assert microvm.LeasePlan is LeasePlan and microvm.LeasePlanRejected is LeasePlanRejected
    assert microvm.FanoutLimit is FanoutLimit and microvm.plan_fanout is plan_fanout


def test_heartbeat_every_clamps_to_a_third_of_the_timeout():
    from microvm.lease import LeasePolicy

    assert LeasePolicy().heartbeat_every() == 30            # 120 s timeout: the default 30 s stands
    assert LeasePolicy(heartbeat_timeout_s=30).heartbeat_every() == 10
    assert LeasePolicy(heartbeat_timeout_s=30).heartbeat_every(30) == 10
    assert LeasePolicy(heartbeat_timeout_s=30).heartbeat_every(4) == 5
    assert LeasePolicy(heartbeat_timeout_s=9).heartbeat_every() == 5
    assert LeasePolicy(heartbeat_timeout_s=600).heartbeat_every(45) == 45


def test_state_machine_and_durable_lease_use_the_clamped_interval():
    from microvm.integrations.stepfunctions import lease_state_machine
    from microvm.lease import LeasePolicy

    asl = lease_state_machine(image_arn="arn:aws:lambda:us-east-1:1:microvm-image:x",
                              execution_role_arn="arn:aws:iam::1:role/r",
                              policy=LeasePolicy(heartbeat_timeout_s=30), region="us-east-1", heartbeat_s=30)
    assert "'heartbeat_s': 10" in asl["States"]["Lease"]["Arguments"]["RunHookPayload"]
