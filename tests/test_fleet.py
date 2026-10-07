"""Fleet scaling decisions with a fake manager (no AWS)."""

from datetime import datetime, timedelta, timezone

from microvm.fleet import Fleet, IdlePolicy, Microvm


def _vm(i, state, age_s):
    return Microvm(
        microvm_id=f"vm-{i}", state=state, image_arn="arn:x:img", image_version="1.0",
        started_at=datetime.now(timezone.utc) - timedelta(seconds=age_s),
    )


class FakeManager:
    def __init__(self, members):
        self.members = members
        self.terminated = []
        self.launched = []

    def list(self, image=None, version=None):
        return self.members

    def terminate(self, microvm_id):
        self.terminated.append(microvm_id)

    def run(self, image, **kw):
        self.launched.append(kw)
        return _vm(len(self.launched), "PENDING", 0)


def test_scale_down_terminates_suspended_then_youngest_running():
    members = [_vm(1, "RUNNING", 600), _vm(2, "SUSPENDED", 300), _vm(3, "RUNNING", 10),
               _vm(4, "RUNNING", 100)]
    victims = Fleet.scale_down_victims(members, 2)
    assert [v.microvm_id for v in victims] == ["vm-2", "vm-3"]


def test_scale_down_handles_unknown_start_time():
    members = [Microvm("a", "RUNNING", "arn", "1.0"), _vm(2, "RUNNING", 5)]
    victims = Fleet.scale_down_victims(members, 1)
    assert victims[0].microvm_id == "vm-2"  # the one with a known, recent start goes first


def test_scale_to_converges_down_and_ignores_terminated():
    fm = FakeManager([_vm(1, "RUNNING", 50), _vm(2, "TERMINATED", 50), _vm(3, "RUNNING", 5)])
    fleet = Fleet(fm, "img")
    assert fleet.size() == 2
    fleet.scale_to(1)
    assert fm.terminated == ["vm-3"]


def test_scale_up_uses_payload_factory_and_passthrough():
    fm = FakeManager([])
    fleet = Fleet(fm, "img", idle_policy=IdlePolicy(max_idle=60), max_duration=900,
                  run_payload_factory=lambda i: f'{{"shard": {i}}}', execution_role="arn:role")
    launched = fleet.scale_to(3)
    assert len(launched) == 3
    payloads = sorted(kw["run_payload"] for kw in fm.launched)
    assert payloads == ['{"shard": 0}', '{"shard": 1}', '{"shard": 2}']
    assert all(kw["execution_role"] == "arn:role" and kw["max_duration"] == 900 for kw in fm.launched)


def test_idle_policy_serialises_to_api_names():
    assert IdlePolicy(max_idle=120, suspended_for=3600, auto_resume=False).to_api() == {
        "maxIdleDurationSeconds": 120,
        "suspendedDurationSeconds": 3600,
        "autoResumeEnabled": False,
    }


def test_run_params_carries_client_token(monkeypatch):
    from microvm.config import PlaneConfig
    from microvm.fleet import FleetManager

    monkeypatch.setattr("microvm.fleet.microvm_client", lambda region, profile: object())
    monkeypatch.setattr("microvm.fleet.applied_quotas", lambda cfg: {})
    monkeypatch.setattr("microvm.fleet.image_arn", lambda name, region, profile: f"arn:img:{name}")
    fm = FleetManager.__new__(FleetManager)
    fm.cfg = PlaneConfig(region="us-east-1")
    params = fm.run_params("img", run_payload='{"callback_id": "abc"}', max_duration=900,
                           client_token="lease-abc")
    assert params["clientToken"] == "lease-abc"
    assert params["runHookPayload"] == '{"callback_id": "abc"}'
    assert params["maximumDurationInSeconds"] == 900
    assert "clientToken" not in fm.run_params("img")


def test_scale_down_victim_is_not_counted_while_list_still_shows_it():
    # ListMicrovms reports a terminated VM in its old state for a moment; the fleet must not count it.
    members = [_vm(1, "RUNNING", 600), _vm(2, "SUSPENDED", 300), _vm(3, "RUNNING", 10)]
    fm = FakeManager(members)
    fleet = Fleet(fm, "img")
    fleet.scale_to(2)
    assert fm.terminated == ["vm-2"]
    assert fleet.size() == 2
    assert fleet.drain() == 2
    assert sorted(fm.terminated) == ["vm-1", "vm-2", "vm-3"]


class FakeResponse:
    def __init__(self, status, payload):
        self.status_code, self._payload = status, payload

    def json(self):
        return self._payload


class FakeClient:
    def __init__(self, microvm_id, log, fail_on=None, delay=0.0):
        self.microvm_id, self.log, self.fail_on, self.delay = microvm_id, log, fail_on, delay

    def request(self, method, path, json=None, timeout=None, port=None):
        import time as _t
        _t.sleep(self.delay)
        self.log.append((self.microvm_id, method, path, json))
        if self.fail_on is not None and json == self.fail_on:
            raise ConnectionError("boom")
        return FakeResponse(200, {"echo": json, "vm": self.microvm_id})


def test_dispatch_spreads_bodies_over_running_members_and_keeps_order():
    fm = FakeManager([_vm(1, "RUNNING", 60), _vm(2, "RUNNING", 60), _vm(3, "SUSPENDED", 60)])
    log = []
    fleet = Fleet(fm, "img")
    seen = []
    results = fleet.dispatch("/task", [{"n": i} for i in range(10)], per_vm=2,
                             client_factory=lambda vm_id: FakeClient(vm_id, log, delay=0.01),
                             on_result=seen.append)
    assert [r["index"] for r in results] == list(range(10))
    assert [r["body"]["echo"] for r in results] == [{"n": i} for i in range(10)]
    assert {r["microvm_id"] for r in results} == {"vm-1", "vm-2"}  # the SUSPENDED member gets nothing
    assert all(m == "POST" and p == "/task" for _, m, p, _ in log)
    assert len(seen) == 10


def test_dispatch_records_a_failed_request_and_finishes_the_rest():
    fm = FakeManager([_vm(1, "RUNNING", 60)])
    fleet = Fleet(fm, "img")
    results = fleet.dispatch("/task", [{"n": 0}, {"n": 1}, {"n": 2}], per_vm=1,
                             client_factory=lambda vm_id: FakeClient(vm_id, [], fail_on={"n": 1}))
    assert results[0]["status"] == 200 and results[2]["status"] == 200
    assert results[1]["status"] is None and "ConnectionError" in results[1]["error"]


def test_dispatch_needs_a_running_member():
    import pytest
    fleet = Fleet(FakeManager([_vm(1, "SUSPENDED", 60)]), "img")
    with pytest.raises(RuntimeError, match="no RUNNING members"):
        fleet.dispatch("/task", [{}], client_factory=lambda vm_id: None)
