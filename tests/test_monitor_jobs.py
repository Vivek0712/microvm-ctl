"""FleetMonitor.job_status: one /status per RUNNING member, in parallel, never failing the sweep."""

import time
from datetime import datetime, timezone

import pytest

from microvm import monitor
from microvm.config import PlaneConfig
from microvm.fleet import Microvm
from microvm.monitor import FleetMonitor, job_row, job_summary


def _vm(i, state="RUNNING"):
    return Microvm(microvm_id=f"vm-{i}", state=state, image_arn="arn:x:img", image_version="1.0",
                   started_at=datetime.now(timezone.utc), endpoint=f"vm-{i}.lambda-microvm.us-east-1.on.aws")


def _snap(phase, done, total, *, lease_done=False, lost=False, error=None, elapsed=12.5, lease_id="job-1"):
    return {"phase": phase, "started": "2026-09-20T00:00:00Z", "elapsed_s": elapsed,
            "progress": {"done": done, "total": total}, "counters": {}, "log_tail": [],
            "lease": {"kind": "none", "id": lease_id, "heartbeats": 3, "lost": lost, "done": lease_done,
                      "error": error},
            "microvm_id": "x", "seq": 9}


class FakeManager:
    members = [_vm(1), _vm(2, "SUSPENDED"), _vm(3), _vm(4), _vm(5)]

    def __init__(self, cfg):
        self.cfg = cfg

    def list(self, image=None, version=None):
        assert image == "handoff-agent"
        return list(self.members)


class FakeClient:
    """Stands in for EndpointClient: vm-1 is mid-job, vm-3 raises, vm-4 is done, vm-5 hangs."""

    built = []

    def __init__(self, cfg, microvm_id, endpoint=None, *, ports=None, token_ttl_minutes=15):
        self.microvm_id, self.endpoint, self.ports, self.ttl = microvm_id, endpoint, ports, token_ttl_minutes
        type(self).built.append((microvm_id, endpoint, tuple(ports or ()), token_ttl_minutes))

    def status(self, since=None, **kw):
        if self.microvm_id == "vm-3":
            raise ConnectionError("connection refused")
        if self.microvm_id == "vm-5":
            time.sleep(5)
            return _snap("work", 1, 4)
        if self.microvm_id == "vm-4":
            return _snap("done", 4, 4, lease_done=True, elapsed=40.0, lease_id="job-4")
        return _snap("step 2/4", 1, 4, elapsed=12.5)


@pytest.fixture()
def mon(monkeypatch):
    monkeypatch.setattr(monitor, "FleetManager", FakeManager)
    monkeypatch.setattr(monitor, "EndpointClient", FakeClient)
    monkeypatch.setattr(FleetMonitor, "MEMBER_TIMEOUT_S", 0.5)
    FakeClient.built = []
    return FleetMonitor(PlaneConfig(region="us-east-1"))


def test_job_status_polls_running_members_only_and_survives_bad_ones(mon):
    t0 = time.time()
    rows = mon.job_status("handoff-agent", port=9000, ttl=5)
    assert time.time() - t0 < 3, "members are polled in parallel and the hung one is cut at the timeout"
    assert [r["microvm_id"] for r in rows] == ["vm-1", "vm-3", "vm-4", "vm-5"]  # list order, no SUSPENDED
    ok = rows[0]
    assert ok == {"microvm_id": "vm-1", "lease_id": "job-1", "phase": "step 2/4",
                  "progress": {"done": 1, "total": 4}, "elapsed_s": 12.5, "done": False, "lost": False,
                  "error": None}
    assert rows[1] == {"microvm_id": "vm-3", "error": "ConnectionError: connection refused"}
    assert rows[2]["done"] is True and rows[2]["lease_id"] == "job-4"
    assert set(rows[3]) == {"microvm_id", "error"} and "within 0.5 s" in rows[3]["error"]
    # one client per member with the record's endpoint (no GetMicrovm), the requested port and TTL
    assert sorted(FakeClient.built) == [("vm-1", "vm-1.lambda-microvm.us-east-1.on.aws", (9000,), 5),
                                        ("vm-3", "vm-3.lambda-microvm.us-east-1.on.aws", (9000,), 5),
                                        ("vm-4", "vm-4.lambda-microvm.us-east-1.on.aws", (9000,), 5),
                                        ("vm-5", "vm-5.lambda-microvm.us-east-1.on.aws", (9000,), 5)]
    # a second sweep reuses the cached clients (tokens minted once per VM)
    mon.job_status("handoff-agent", port=9000, ttl=5)
    assert len(FakeClient.built) == 4


def test_job_status_is_empty_without_running_members(mon, monkeypatch):
    monkeypatch.setattr(FakeManager, "members", [_vm(2, "SUSPENDED"), _vm(6, "TERMINATED")])
    assert mon.job_status("handoff-agent") == []


def test_job_row_and_summary():
    failed = job_row("vm-9", _snap("done", 2, 4, lease_done=True,
                                   error={"error_type": "StepFailed", "message": "step 3/4 exited 1"}))
    assert failed["done"] and failed["error"] == "StepFailed: step 3/4 exited 1"
    no_lease = job_row("vm-8", {"phase": "done", "elapsed_s": 1.0, "progress": {"done": 1, "total": 1}})
    assert no_lease["done"] is True and no_lease["lease_id"] is None
    rows = [
        job_row("vm-1", _snap("step 2/4", 1, 4, elapsed=12.5)),
        job_row("vm-2", _snap("step 3/4", 2, 4, elapsed=30.0)),
        job_row("vm-3", _snap("hang", 4, 4, lost=True, elapsed=99.0)),
        failed,
        {"microvm_id": "vm-4", "error": "no answer"},
    ]
    s = job_summary(rows)
    assert s == {"total": 5, "done": 1, "running": 2, "lost": 1, "failed": 1, "unanswered": 1,
                 "slowest": {"microvm_id": "vm-2", "phase": "step 3/4", "elapsed_s": 30.0}}
    assert job_summary([])["slowest"] is None
