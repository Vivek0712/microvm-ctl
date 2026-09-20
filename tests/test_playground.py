"""Playground API dispatcher in dry-run mode, with the AWS-facing pieces faked."""

import json
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from microvm.config import PlaneConfig
from microvm.fleet import IdlePolicy, Microvm
from microvm.playground.server import Playground, _redact


class FakeManager:
    def __init__(self, members):
        self.members = members
        self.cfg = PlaneConfig(region="us-east-1")

    def list(self, image=None, version=None):
        return self.members

    def tps(self, op):
        return 0.8

    def run_params(self, image, **kw):
        params = {"imageIdentifier": f"arn:aws:lambda:us-east-1:1:microvm-image:{image}",
                  "idlePolicy": (kw.get("idle_policy") or IdlePolicy()).to_api()}
        for src, dst in (("run_payload", "runHookPayload"), ("max_duration", "maximumDurationInSeconds"),
                         ("client_token", "clientToken")):
            if kw.get(src):
                params[dst] = kw[src]
        return params


def _vm(i, state, age_s=600):
    return Microvm(microvm_id=f"vm-{i}", state=state, image_arn="arn:x:img", image_version="1.0",
                   started_at=datetime.now(timezone.utc) - timedelta(seconds=age_s))


@pytest.fixture()
def pg(monkeypatch, tmp_path):
    monkeypatch.setattr(Playground, "STATE_DIR", tmp_path)  # never touch ~/.microvm-ctl from tests
    p = Playground(PlaneConfig(region="us-east-1"), dry_run=True)
    members = [_vm(1, "RUNNING", 600), _vm(2, "SUSPENDED", 300), _vm(3, "RUNNING", 10)]
    monkeypatch.setattr(p, "fm", lambda: FakeManager(members))
    return p


def _wait(pg, job_id, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = pg.jobs[job_id]
        if job.status != "running":
            return job
        time.sleep(0.02)
    raise AssertionError("job did not finish")


def test_unknown_route_is_404(pg):
    status, body = pg.api("GET", "/api/nope", {}, None)
    assert status == 404 and "no route" in body["error"]


def test_cost_matches_cost_model(pg):
    status, s = pg.api("POST", "/api/cost", {}, {"memory_gb": 2, "snapshot_gb": 0.61,
                                                  "active_s": 1800, "suspended_s": 8 * 3600, "cycles": 1})
    assert status == 200
    assert s["savings_pct"] == 93.8 and s["vcpu"] == 1.0


def test_dry_run_scale_down_reports_victims_and_sends_nothing(pg):
    status, job = pg.api("POST", "/api/fleet/scale", {}, {"image": "img", "n": 1})
    assert status == 200
    job = _wait(pg, job["id"])
    assert job.status == "done"
    assert job.result["dry_run"] is True and job.result["operation"] == "TerminateMicrovm"
    victims = [e for e in job.events if e["msg"].startswith("scale-down victims")][0]["data"]["victims"]
    assert [v["id"] for v in victims] == ["vm-2", "vm-3"]  # SUSPENDED first, then youngest RUNNING
    assert pg.trace[-1]["service"] == "dry-run"


def test_dry_run_run_returns_exact_request(pg):
    body = {"image": "img", "idle": 120, "suspended_for": 600, "count": 2}
    status, job = pg.api("POST", "/api/vms/run", {}, body)
    job = _wait(pg, job["id"])
    assert job.status == "done"
    params = job.result["params"]
    assert params["idlePolicy"] == {"maxIdleDurationSeconds": 120, "suspendedDurationSeconds": 600,
                                    "autoResumeEnabled": True}
    assert "would launch 2" in job.result["note"]


def test_lifecycle_verbs_are_dry(pg):
    status, body = pg.api("POST", "/api/vms/vm-1/terminate", {}, {})
    assert status == 200 and body["dry_run"] and body["params"] == {"microvmIdentifier": "vm-1"}


def test_jobs_listing_and_trace_cursor(pg):
    pg.api("POST", "/api/vms/vm-1/suspend", {}, {})
    pg.api("POST", "/api/vms/vm-1/resume", {}, {})
    _, t = pg.api("GET", "/api/trace", {"since": ["1"]}, None)
    assert t["seq"] == 2 and len(t["items"]) == 1
    _, jobs = pg.api("GET", "/api/jobs", {}, None)
    assert isinstance(jobs["items"], list)


def test_redact_masks_tokens_and_drops_metadata():
    out = _redact({"authToken": {"X-aws-proxy-auth": "x" * 300}, "ResponseMetadata": {"a": 1}, "n": 1})
    assert "ResponseMetadata" not in out
    assert out["authToken"]["X-aws-proxy-auth"].startswith("xxxxxxxxxxxx…")
    assert out["n"] == 1


def test_set_config_rejects_bad_region(pg):
    status, body = pg.api("POST", "/api/config", {}, {"region": "mars-1"})
    assert status == 400 and "not available" in body["error"]


def test_dry_run_reads_fall_back_to_sample_data(monkeypatch, tmp_path):
    """Without credentials, dry run still answers every read with the flagged sample set."""
    monkeypatch.setattr(Playground, "STATE_DIR", tmp_path)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "")
    p = Playground(PlaneConfig(region="us-east-1", profile=None), dry_run=True)
    status, imgs = p.api("GET", "/api/images", {}, None)
    assert status == 200 and imgs.get("sample") is True and imgs["items"][0]["name"] == "code-sandbox"
    status, vms = p.api("GET", "/api/vms", {"image": ["code-sandbox"]}, None)
    assert status == 200 and vms["sample"] is True and all(v["image"] == "code-sandbox" for v in vms["items"])
    status, logs = p.api("GET", "/api/logs", {"image": ["code-sandbox"]}, None)
    assert status == 200 and logs["sample"] is True and logs["events"]
    status, call = p.api("POST", "/api/call", {}, {"id": vms["items"][0]["microvm_id"], "method": "POST",
                                                   "path": "/execute", "body": {"code": "print(1)"}})
    assert status == 200 and call["dry_run"] and call["params"]["url"].startswith("https://")


def _no_aws(*a, **k):
    raise ConnectionError("no AWS in tests")


def test_job_endpoint_returns_sample_snapshot_in_dry_run(pg, monkeypatch):
    """Dry run without AWS: /status is answered by the sample job, flagged and cursor-aware."""
    monkeypatch.setattr(pg, "ep", _no_aws)  # EndpointClient would discover the endpoint via GetMicrovm
    status, snap = pg.api("GET", "/api/vms/vm-1/job", {}, None)
    assert status == 200 and snap["sample"] is True and snap["microvm_id"] == "vm-1"
    assert snap["phase"] in {"init", "load", "work", "flush", "done"}
    assert set(snap) >= {"phase", "started", "elapsed_s", "progress", "counters", "log_tail", "lease", "seq"}
    assert snap["log_tail"] and snap["log_tail"][-1]["seq"] == snap["seq"]
    assert snap["lease"]["kind"] == "none" and snap["lease"]["done"] == (snap["phase"] == "done")
    status, more = pg.api("GET", "/api/vms/vm-1/job", {"since": [str(snap["seq"])], "port": ["9000"]}, None)
    assert status == 200 and all(ln["seq"] > snap["seq"] for ln in more["log_tail"])


def test_job_endpoint_reports_missing_telemetry_without_raising(pg, monkeypatch):
    from microvm.endpoint import EndpointError

    class NoStatus:
        def status(self, since=None):
            raise EndpointError("vm-1 answered 404 on /status")

    monkeypatch.setattr(pg, "ep", lambda *a, **k: NoStatus())
    status, body = pg.api("GET", "/api/vms/vm-1/job", {}, None)
    assert status == 200 and body["status"] == 404 and "404" in body["error"]


def test_dry_run_lease_returns_request_with_payload_and_client_token(pg):
    body = {"image": "img", "kind": "none", "task": {"steps": ["echo hi", "sleep 2", "echo done"]},
            "id": "demo-lease-1", "budget": 600, "slack": 60, "heartbeat_every": 15}
    status, job = pg.api("POST", "/api/lease/run", {}, body)
    assert status == 200 and job["kind"] == "lease"
    job = _wait(pg, job["id"])
    assert job.status == "done", job.error
    assert job.result["dry_run"] is True and job.result["operation"] == "RunMicrovm"
    params = job.result["params"]
    payload = json.loads(params["runHookPayload"])
    assert payload["lease"]["kind"] == "none" and payload["lease"]["id"] == "demo-lease-1"
    assert payload["lease"]["heartbeat_s"] == 15 and payload["task"] == body["task"]
    assert len(params["clientToken"]) == 64 and params["clientToken"] == job.params["client_token"]
    assert params["maximumDurationInSeconds"] == 660  # budget + slack
    assert params["idlePolicy"] == {"maxIdleDurationSeconds": 600, "suspendedDurationSeconds": 60,
                                    "autoResumeEnabled": False}
    first = job.events[0]["data"]
    assert first["payload"] == params["runHookPayload"] and first["client_token"] == params["clientToken"]
    assert pg.trace[-1]["service"] == "dry-run"


class _Limit:
    def __init__(self, limit, reason):
        self.limit, self.reason, self.by_memory, self.by_policy = limit, reason, 4, limit
        self.memory_quota_gb, self.launch_rate = 8.0, 0.8


class _Plan:
    """What FleetManager.plan (section A) returns, reduced to the fields the route reads."""

    def __init__(self, shards, baseline_mib, policy):
        limit = policy.max_concurrency if policy.max_concurrency is not None else 4
        self.shards, self.baseline_mib, self.policy = shards, baseline_mib, policy
        reason = "policy max_concurrency" if policy.max_concurrency is not None else "default"
        self.limit = _Limit(limit, reason)
        self.concurrency = min(shards, limit)
        self.waves = -(-shards // self.concurrency) if self.concurrency else 0
        self.launch_to_all_running_s = 3.5
        self.worst_case_vm_seconds = shards * policy.max_duration()
        self.worst_case_usd = round(self.worst_case_vm_seconds * baseline_mib / 1024 * 0.0000175139, 4)
        self.needs_approval = policy.approval_usd is not None and self.worst_case_usd > policy.approval_usd
        self.rejected = "baseline 16384 MiB exceeds the memory quota 8 GB" if baseline_mib > 8192 else None

    def summary(self):
        return (f"{self.shards} shards on {self.baseline_mib} MiB: {self.concurrency} at a time "
                f"({self.limit.reason}), {self.waves} waves, worst case ${self.worst_case_usd}")


class PlanningManager(FakeManager):
    def __init__(self, members):
        super().__init__(members)
        self.launched = []

    def plan(self, shards, baseline_mib, policy=None):
        from microvm.lease import LeasePolicy
        return _Plan(shards, baseline_mib, policy or LeasePolicy())

    def lease_many(self, image, leases, tasks, policy=None, *, baseline_mib, **kw):
        self.launched.append((image, [ls.id for ls in leases], tasks, baseline_mib))
        return [_vm(i, "RUNNING") for i in range(len(leases))]


@pytest.fixture()
def pgs(pg, monkeypatch):
    fm = PlanningManager([])
    monkeypatch.setattr(pg, "fm", lambda: fm)
    return pg


def test_dry_run_shards_logs_the_plan_and_returns_the_first_request(pgs, monkeypatch):
    monkeypatch.delenv("MVM_LEASE_MAX_CONCURRENCY", raising=False)
    body = {"image": "img", "kind": "none", "shards": 3, "id": "fan", "baseline_mib": 2048, "budget": 600,
            "slack": 60, "task_template": {"steps": ["echo shard {i}", "sleep 1"], "env": {"SHARD": "{i}"}}}
    status, job = pgs.api("POST", "/api/lease/run", {}, body)
    assert status == 200 and job["kind"] == "lease" and job["params"]["shards"] == 3
    job = _wait(pgs, job["id"])
    assert job.status == "done", job.error
    first = job.events[0]
    assert first["msg"].startswith("3 shards on 2048 MiB: 3 at a time (default)")  # the plan sentence, first
    assert first["data"]["plan"]["concurrency"] == 3 and first["data"]["plan"]["rejected"] is None
    leases = job.events[1]["data"]
    assert [ls["id"] for ls in leases["leases"]] == ["fan-0", "fan-1", "fan-2"]
    assert leases["tasks"][2] == {"steps": ["echo shard 2", "sleep 1"], "env": {"SHARD": "2"}}
    assert len(set(leases["client_tokens"])) == 3  # kind none: one clientToken per shard, not one VM
    r = job.result
    assert r["dry_run"] is True and r["operation"] == "RunMicrovm" and r["shards"] == 3
    assert r["lease_ids"] == ["fan-0", "fan-1", "fan-2"] and r["plan"]["waves"] == 1
    payload = json.loads(r["params"]["runHookPayload"])
    assert payload["lease"]["id"] == "fan-0" and payload["task"]["steps"][0] == "echo shard 0"
    assert r["params"]["maximumDurationInSeconds"] == 660
    assert pgs.trace[-1]["service"] == "dry-run" and pgs.fm().launched == []


def test_shards_above_the_concurrency_limit_are_refused_before_launch(pgs, monkeypatch):
    """MVM_LEASE_MAX_CONCURRENCY=2 (LeasePolicy.from_env) caps the fan-out: 3 shards is a 400."""
    monkeypatch.setenv("MVM_LEASE_MAX_CONCURRENCY", "2")
    body = {"image": "img", "shards": 3, "baseline_mib": 2048, "task": {"steps": ["echo {i}"]}}
    status, r = pgs.api("POST", "/api/lease/run", {}, body)
    assert status == 400
    assert r["error"] == "3 leases exceed the concurrency limit 2 (policy max_concurrency); launch in waves"
    assert pgs.fm().launched == [] and not [j for j in pgs.jobs.values() if j.params.get("shards") == 3]
    # the body can lift the ceiling explicitly; a rejected plan is still a 400 with the reason
    status, _ = pgs.api("POST", "/api/lease/run", {}, {**body, "max_concurrency": 3})
    assert status == 200
    status, r = pgs.api("POST", "/api/lease/run", {}, {**body, "max_concurrency": 3, "baseline_mib": 16384})
    assert status == 400 and r["error"] == "plan rejected: baseline 16384 MiB exceeds the memory quota 8 GB"


def test_single_lease_path_ignores_shards_of_one(pgs):
    """shards=1 is the unchanged single-lease path: FleetManager.lease, no plan."""
    body = {"image": "img", "kind": "none", "shards": 1, "task": {"steps": ["echo hi"]}, "id": "one"}
    status, job = pgs.api("POST", "/api/lease/run", {}, body)
    job = _wait(pgs, job["id"])
    assert job.status == "done" and "shards" not in job.params
    assert json.loads(job.result["params"]["runHookPayload"])["lease"]["id"] == "one"


def test_fleet_jobs_endpoint_answers_with_the_sample_fleet_in_dry_run(pg, monkeypatch):
    import microvm.playground.server as srv
    monkeypatch.setattr(srv.FleetMonitor, "job_status", _no_aws)  # ListMicrovms cannot answer
    status, r = pg.api("GET", "/api/fleet/jobs", {"image": ["code-sandbox"]}, None)
    assert status == 200 and r["sample"] is True and r["image"] == "code-sandbox"
    ids = [row["microvm_id"] for row in r["items"]]
    assert ids == ["mvm-0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d", "mvm-1b2c3d4e-5f6a-7b8c-9d0e-1f2a3b4c5d6e"]
    for row in r["items"]:
        assert set(row) == {"microvm_id", "lease_id", "phase", "progress", "elapsed_s", "done", "lost",
                            "error"}
        assert row["phase"] in {"init", "load", "work", "flush", "done"}
        assert row["lease_id"].startswith("sample-")
    s = r["summary"]
    assert s["total"] == 2 and s["done"] + s["running"] + s["lost"] == 2 and s["slowest"]["microvm_id"] in ids
    status, r = pg.api("GET", "/api/fleet/jobs", {"image": ["notebook"]}, None)  # only a SUSPENDED member
    assert status == 200 and r["items"] == [] and r["summary"]["slowest"] is None
    status, r = pg.api("GET", "/api/fleet/jobs", {}, None)
    assert status == 400 and "image is required" in r["error"]


def test_lease_validation_errors_are_400(pg):
    status, body = pg.api("POST", "/api/lease/run", {}, {"image": "img", "kind": "sfn"})
    assert status == 400 and "token is required" in body["error"]
    status, body = pg.api("POST", "/api/lease/run", {}, {"image": "img", "task": "[1, 2]"})
    assert status == 400 and "JSON object" in body["error"]
    bad_target = {"image": "img", "kind": "http", "token": "t", "target": "x"}
    status, body = pg.api("POST", "/api/lease/run", {}, bad_target)
    assert status == 400 and "http(s) URL" in body["error"]


def test_parse_exports_handles_shell_variants():
    from microvm.playground.server import parse_exports
    text = """
    # from the SSO console
    export AWS_ACCESS_KEY_ID=ASIAEXAMPLE
    export AWS_SECRET_ACCESS_KEY="s3cr3t/with=equals"
    export AWS_SESSION_TOKEN='tok'   # comment inside quotes is kept out
    set MVM_ARTIFACT_BUCKET=my-bucket
    $env:MVM_REGION="us-west-2"
    UNRELATED=ignored
    """
    got = parse_exports(text)
    assert got == {"AWS_ACCESS_KEY_ID": "ASIAEXAMPLE", "AWS_SECRET_ACCESS_KEY": "s3cr3t/with=equals",
                   "AWS_SESSION_TOKEN": "tok", "MVM_ARTIFACT_BUCKET": "my-bucket", "MVM_REGION": "us-west-2"}


def test_credentials_endpoint_applies_env_and_reports_verification(pg, monkeypatch):
    import microvm.playground.server as srv
    who = {"account": "111122223333", "arn": "arn:aws:iam::111122223333:user/x", "user_id": "AID"}
    monkeypatch.setattr(srv, "_whoami", lambda p: who)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    body = {"text": "export AWS_ACCESS_KEY_ID=AKIA1\nexport AWS_SECRET_ACCESS_KEY=S3CR3TVALUE\n"
                    "export MVM_REGION=eu-west-1"}
    status, r = pg.api("POST", "/api/credentials", {}, body)
    assert status == 200 and r["ok"] and r["account"] == "111122223333"
    assert pg.cfg.region == "eu-west-1" and pg.cfg.profile is None
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AKIA1"
    assert "S3CR3TVALUE" not in json.dumps(r)  # the secret is never echoed
    status, r = pg.api("POST", "/api/credentials", {}, {"text": "export AWS_ACCESS_KEY_ID=only"})
    assert status == 400
    pg.api("POST", "/api/credentials/clear", {}, {})
    assert "AWS_ACCESS_KEY_ID" not in os.environ


def test_state_is_only_written_by_a_serving_process(tmp_path, monkeypatch):
    monkeypatch.setattr(Playground, "STATE_DIR", tmp_path)
    p = Playground(PlaneConfig(region="us-east-1"), dry_run=True)
    p.save_state()
    assert not (tmp_path / "state.json").exists()
    p.persist = True
    p.save_state()
    assert (tmp_path / "state.json").exists()
