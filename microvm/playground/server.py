"""Playground backend: a JSON API over the SDK plus a static single-page UI.

Everything the `mvm` command can do is reachable here, parameterised, and
observable: long operations run as jobs with a live event log, every AWS API
call the process makes is recorded in a trace ring (operation, duration,
status, redacted params and response), and a dry-run switch turns each
mutating action into a printout of the exact request it would have sent.

    mvm playground                 # http://127.0.0.1:8765, opens a browser tab
    mvm playground --dry-run       # never mutate, still read

The API is transport-agnostic: `Playground.api(method, path, query, body)`
returns `(status, json)`. `serve()` wraps it in the stdlib HTTP server for
local use; `lambda_handler()` wraps the same function for a Lambda Function
URL when the UI is hosted behind CloudFront (see docs/playground.md).
"""

from __future__ import annotations

import io
import json
import os
import re
import statistics
import threading
import time
import traceback
import uuid
import webbrowser
import zipfile
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from microvm import __version__
from microvm.client import _session, account_id, lambda_client
from microvm.config import SUPPORTED_REGIONS, TPS, PlaneConfig
from microvm.endpoint import EndpointClient, EndpointError
from microvm.fleet import Fleet, FleetManager, IdlePolicy, applied_quotas
from microvm.images import ImageBuilder, default_hooks
from microvm.lease import Lease, LeasePolicy, client_token, encode_payload
from microvm.monitor import (
    RATE_GB_SECOND,
    RATE_SNAPSHOT_READ_GB,
    RATE_SNAPSHOT_WRITE_GB,
    RATE_STORAGE_GB_MONTH,
    RATE_VCPU_SECOND,
    CostModel,
    FleetMonitor,
)

STATIC_DIR = Path(__file__).parent / "static"
SIZE_TIERS = [512, 1024, 2048, 4096, 8192]


# --------------------------------------------------------------------------- jobs
class Job:
    """One background operation with an append-only event log."""

    def __init__(self, kind: str, params: dict):
        self.id = uuid.uuid4().hex[:10]
        self.kind = kind
        self.params = params
        self.status = "running"
        self.events: list[dict] = []
        self.result: Any = None
        self.error: str | None = None
        self.started = time.time()
        self.finished: float | None = None
        self._lock = threading.Lock()

    def log(self, msg: str, level: str = "info", **data: Any) -> None:
        with self._lock:
            self.events.append({"t": time.time(), "level": level, "msg": msg, "data": data or None})

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "id": self.id,
                "kind": self.kind,
                "params": self.params,
                "status": self.status,
                "events": list(self.events),
                "result": self.result,
                "error": self.error,
                "started": self.started,
                "finished": self.finished,
            }


def _pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def _redact(obj: Any, depth: int = 0) -> Any:
    """Mask token values and truncate long strings for the trace view."""
    if depth > 6:
        return "…"
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "ResponseMetadata":
                continue
            if k == "authToken" and isinstance(v, dict):
                out[k] = {hk: _mask(hv) for hk, hv in v.items()}
            else:
                out[k] = _redact(v, depth + 1)
        return out
    if isinstance(obj, list):
        return [_redact(v, depth + 1) for v in obj[:50]] + (["…"] if len(obj) > 50 else [])
    if isinstance(obj, (bytes, bytearray)):
        try:
            return _redact(json.loads(obj), depth + 1)
        except Exception:
            return f"<{len(obj)} bytes>"
    if isinstance(obj, str):
        return obj if len(obj) <= 400 else obj[:400] + f"… (+{len(obj) - 400})"
    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj
    return str(obj)


def _mask(v: Any) -> str:
    s = str(v)
    return s[:12] + "…" + f"({len(s)} chars)" if len(s) > 16 else "***"


# --------------------------------------------------------------------------- sample data
# Dry run without credentials still has to be a usable playground, so reads that
# cannot reach AWS fall back to this dataset, flagged `sample: true` in the API.
SAMPLE_ACCOUNT = "123456789012"
SAMPLE_BUCKET = "microvm-ctl-artifacts-123456789012-us-east-1"
SAMPLE_QUOTAS = {
    "RunMicrovm": 1.0,
    "SuspendMicrovm": 2.0,
    "ResumeMicrovm": 5.0,
    "TerminateMicrovm": 10.0,
    "MaxMemoryGb": 8.0,
}
_SAMPLE_IMAGE_NAMES = [
    "code-sandbox",
    "ai-code-runner",
    "agent-eval",
    "notebook",
    "data-analytics",
    "ci-runner",
    "pdf-service",
    "multi-tenant-agents",
]


def _sample_images(region: str) -> list[dict]:
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    return [
        {
            "name": n,
            "state": "CREATED",
            "latestActiveImageVersion": "1.0",
            "imageArn": f"arn:aws:lambda:{region}:{SAMPLE_ACCOUNT}:microvm-image:{n}",
            "createdAt": (now - timedelta(days=4, hours=i)).isoformat(),
        }
        for i, n in enumerate(_SAMPLE_IMAGE_NAMES)
    ]


def _sample_versions(name: str) -> list[dict]:
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    return [
        {
            "imageVersion": "1.0",
            "state": "SUCCESSFUL",
            "status": "ACTIVE",
            "createdAt": (now - timedelta(days=4)).isoformat(),
        }
    ]


def _sample_vms(region: str) -> list:
    from datetime import datetime, timedelta, timezone

    from microvm.fleet import Microvm

    now = datetime.now(timezone.utc)
    rows = [
        ("mvm-0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d", "RUNNING", "code-sandbox", 1260),
        ("mvm-1b2c3d4e-5f6a-7b8c-9d0e-1f2a3b4c5d6e", "RUNNING", "code-sandbox", 95),
        ("mvm-2c3d4e5f-6a7b-8c9d-0e1f-2a3b4c5d6e7f", "SUSPENDED", "notebook", 5400),
        ("mvm-3d4e5f6a-7b8c-9d0e-1f2a-3b4c5d6e7f8a", "PENDING", "agent-eval", 2),
        ("mvm-4e5f6a7b-8c9d-0e1f-2a3b-4c5d6e7f8a9b", "TERMINATED", "ci-runner", 9000),
    ]
    return [
        Microvm(
            microvm_id=vid,
            state=st,
            image_arn=f"arn:aws:lambda:{region}:{SAMPLE_ACCOUNT}:microvm-image:{img}",
            image_version="1.0",
            started_at=now - timedelta(seconds=age),
            endpoint=f"{vid.split('-')[1]}{vid.split('-')[2]}.lambda-microvm.{region}.on.aws",
        )
        for vid, st, img, age in rows
    ]


def _sample_logs(image: str) -> list[dict]:
    t = int(time.time() * 1000)
    lines = [
        ("build-7f3a", -840_000, "Step 1/7 : FROM public.ecr.aws/lambda/microvms:al2023-minimal"),
        ("build-7f3a", -812_000, "Step 5/7 : COPY microvm_hooks.py app.py /app/"),
        ("build-7f3a", -790_000, "GET /aws/lambda-microvms/runtime/v1/ready -> 200"),
        ("build-7f3a", -701_000, "POST /aws/lambda-microvms/runtime/v1/validate -> 200 (numpy warm path)"),
        ("mvm-0a1b2c3d", -300_000, "POST /aws/lambda-microvms/runtime/v1/run -> 200 (rng reseeded)"),
        ("mvm-0a1b2c3d", -120_000, 'POST /execute {"code": "print(41+1)"} -> 200 in 108 ms'),
        ("mvm-2c3d4e5f", -60_000, "POST /aws/lambda-microvms/runtime/v1/suspend -> 200 (buffers flushed)"),
    ]
    return [{"stream": f"{image}/{s}", "ts": t + d, "message": m} for s, d, m in lines]


# One looping 60 s job: (phase, seconds). The snapshot is a pure function of the wall
# clock so a panel polling it every 2 s sees phases, progress and log lines move.
_SAMPLE_PHASES = (("init", 4), ("load", 8), ("work", 34), ("flush", 8), ("done", 6))
_SAMPLE_STEPS = 12


def _iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + "Z"


def _sample_job(microvm_id: str, since: int | None = None) -> dict:
    """What the hook runtime's `GET /status` would answer, for dry run without a VM.
    Same keys as `microvm.hooks.server.Job.snapshot`."""
    cycle = sum(d for _, d in _SAMPLE_PHASES)
    now = time.time()
    t0 = now - now % cycle
    elapsed = now - t0
    spans, at = [], 0
    for name, d in _SAMPLE_PHASES:
        spans.append((name, at, at + d))
        at += d
    w_start, w_end = spans[2][1], spans[2][2]

    def phase_at(offset: float) -> str:
        return next((n for n, _s, e in spans if offset < e), spans[-1][0])

    def done_at(offset: float) -> int:
        return int(max(0.0, min(1.0, (offset - w_start) / (w_end - w_start))) * _SAMPLE_STEPS)

    lines = []
    for tick in range(int(elapsed // 2) + 1):
        offset = tick * 2
        p, level = phase_at(offset), "info"
        if any(s == offset for _n, s, _e in spans):
            msg = f"phase: {p}"
        elif p == "init":
            msg = "lease accepted: kind none, 3 task steps, heartbeat every 30 s"
        elif p == "load":
            msg = f"fetched task inputs: s3://{SAMPLE_BUCKET}/tasks/{microvm_id[4:12]}.json (2.1 KB)"
        elif p == "work":
            step = done_at(offset)
            if step == 7:
                msg, level = "step 7: retrying after ECONNRESET (attempt 2)", "warning"
            else:
                msg = f"step {step}/{_SAMPLE_STEPS}: echo hi"
        elif p == "flush":
            msg = f"writing result to s3://{SAMPLE_BUCKET}/results/{microvm_id[4:12]}.json"
        else:
            msg = "lease success delivered"
        lines.append(
            {"seq": int((t0 + offset) // 2), "t": _iso(t0 + offset), "level": level, "msg": msg, "phase": p}
        )
    phase = phase_at(elapsed)
    done = done_at(elapsed)
    tail = [ln for ln in lines if since is None or ln["seq"] > since][-50:]
    return {
        "phase": phase,
        "started": _iso(t0),
        "elapsed_s": round(elapsed, 3),
        "progress": {"done": done, "total": None if phase == "init" else _SAMPLE_STEPS},
        "counters": {"steps_ok": done, "bytes_out": done * 4096, "retries": 1 if done >= 7 else 0},
        "log_tail": tail,
        "lease": {
            "kind": "none",
            "id": f"sample-{microvm_id[4:12]}",
            "heartbeats": int(elapsed // 30),
            "lost": False,
            "done": phase == "done",
            "error": None,
        },
        "microvm_id": microvm_id,
        "seq": lines[-1]["seq"],
    }


class _SampleAwareManager(FleetManager):
    """FleetManager whose reads fall back to the sample dataset when AWS cannot answer.
    Only used while dry run is on; every mutating call is intercepted before it gets here."""

    def __init__(self, config: PlaneConfig):
        super().__init__(config)
        self.sample = False

    def list(self, image=None, version=None):
        try:
            return super().list(image, version)
        except Exception:
            self.sample = True
            vms = _sample_vms(self.cfg.region)
            if image:
                vms = [v for v in vms if v.image_arn.split(":")[-1] == image or v.image_arn == image]
            return vms

    def run_params(self, image, **kw):
        try:
            return super().run_params(image, **kw)
        except Exception:
            self.sample = True
            arn = (
                image
                if image.startswith("arn:")
                else (f"arn:aws:lambda:{self.cfg.region}:{SAMPLE_ACCOUNT}:microvm-image:{image}")
            )
            saved = self.cfg
            try:
                params = {"imageIdentifier": arn}
                params["idlePolicy"] = (kw.get("idle_policy") or IdlePolicy()).to_api()
                for src, dst in (
                    ("version", "imageVersion"),
                    ("run_payload", "runHookPayload"),
                    ("max_duration", "maximumDurationInSeconds"),
                    ("ingress", "ingressNetworkConnectors"),
                    ("egress", "egressNetworkConnectors"),
                ):
                    if kw.get(src):
                        params[dst] = kw[src]
                role = kw.get("execution_role") or saved.execution_role_arn
                if role:
                    params["executionRoleArn"] = role
                if kw.get("client_token"):
                    params["clientToken"] = kw["client_token"]
                return params
            finally:
                self.cfg = saved


# --------------------------------------------------------------------------- playground
class Playground:
    def __init__(self, cfg: PlaneConfig | None = None, dry_run: bool = False):
        self.cfg = cfg or PlaneConfig()
        self.dry_run = dry_run
        self.trace: deque = deque(maxlen=500)
        self._trace_seq = 0
        self.jobs: dict[str, Job] = {}
        self._traced: set = set()
        self._ep_clients: dict = {}
        self._lock = threading.RLock()
        self._ensure_trace()
        self._load_state()

    # -- AWS call tracing ------------------------------------------------------
    def _ensure_trace(self) -> None:
        key = (self.cfg.profile, self.cfg.region)
        if key in self._traced:
            return
        events = _session(*key).events
        events.register("before-call", self._before_call)
        events.register("after-call", self._after_call)
        events.register("after-call-error", self._after_call_error)
        self._traced.add(key)

    def _before_call(self, model, params, context, **kw) -> None:
        context["_pg"] = {
            "t0": time.time(),
            "service": model.service_model.service_name,
            "op": model.name,
            "params": _redact(params.get("body") if params.get("body") else params.get("query_string")),
            "url": params.get("url"),
        }

    def _after_call(self, http_response, parsed, model, context, **kw) -> None:
        pg = context.get("_pg") or {}
        self._push_trace(
            {
                "service": pg.get("service", model.service_model.service_name),
                "op": pg.get("op", model.name),
                "ms": round((time.time() - pg.get("t0", time.time())) * 1000),
                "status": getattr(http_response, "status_code", None),
                "params": pg.get("params"),
                "response": _redact(parsed),
                "error": None,
            }
        )

    def _after_call_error(self, exception, context, **kw) -> None:
        pg = context.get("_pg") or {}
        code = (
            getattr(exception, "response", {}).get("Error", {}).get("Code")
            if hasattr(exception, "response")
            else None
        )
        self._push_trace(
            {
                "service": pg.get("service", "?"),
                "op": pg.get("op", "?"),
                "ms": round((time.time() - pg.get("t0", time.time())) * 1000),
                "status": None,
                "params": pg.get("params"),
                "response": None,
                "error": f"{code or type(exception).__name__}: {str(exception)[:300]}",
            }
        )

    def _push_trace(self, entry: dict) -> None:
        with self._lock:
            self._trace_seq += 1
            entry["seq"] = self._trace_seq
            entry["t"] = time.time()
            self.trace.append(entry)
        now = time.time()
        if now - getattr(self, "_last_save", 0) > 2:
            self._last_save = now
            self.save_state()

    def _dry(self, op: str, params: dict, note: str = "") -> dict:
        """Record what a mutating call would have sent, without sending it."""
        self._push_trace(
            {
                "service": "dry-run",
                "op": op,
                "ms": 0,
                "status": None,
                "params": _redact(params),
                "response": {"note": note or "not sent"},
                "error": None,
            }
        )
        return {"dry_run": True, "operation": op, "params": params, "note": note or "not sent"}

    # -- helpers -----------------------------------------------------------------
    def fm(self) -> FleetManager:
        return _SampleAwareManager(self.cfg) if self.dry_run else FleetManager(self.cfg)

    def sample_endpoint(self, microvm_id: str) -> str:
        for vm in _sample_vms(self.cfg.region):
            if vm.microvm_id == microvm_id:
                return vm.endpoint
        return f"{microvm_id[:12].replace('-', '')}.lambda-microvm.{self.cfg.region}.on.aws"

    def ep(
        self,
        microvm_id: str,
        ports: list[int] | None = None,
        ttl: int = 15,
        endpoint: str | None = None,
    ) -> EndpointClient:
        key = (self.cfg.profile, self.cfg.region, microvm_id, tuple(ports or [8080]), ttl)
        with self._lock:
            client = self._ep_clients.get(key)
        if client is None:
            # built outside the lock: endpoint discovery is an API call, and the trace hook needs the lock
            client = EndpointClient(
                self.cfg, microvm_id, endpoint=endpoint, ports=ports, token_ttl_minutes=ttl
            )
            with self._lock:
                client = self._ep_clients.setdefault(key, client)
        return client

    # -- persistence: jobs and session settings survive a restart ----------------
    STATE_DIR = Path(os.environ.get("MVM_PLAYGROUND_STATE") or Path.home() / ".microvm-ctl" / "playground")

    def _settings(self) -> dict:
        c = self.cfg
        return {
            "region": c.region,
            "profile": c.profile,
            "artifact_bucket": c.artifact_bucket,
            "build_role_arn": c.build_role_arn,
            "execution_role_arn": c.execution_role_arn,
            "dry_run": self.dry_run,
        }

    def save_state(self) -> None:
        try:
            self.STATE_DIR.mkdir(parents=True, exist_ok=True)
            jobs = sorted(self.jobs.values(), key=lambda j: j.started, reverse=True)[:100]
            with self._lock:
                trace = list(self.trace)
            data = {
                "settings": self._settings(),
                "jobs": [j.to_dict() for j in jobs],
                "trace": trace,
                "trace_seq": self._trace_seq,
            }
            tmp = self.STATE_DIR / "state.json.tmp"
            tmp.write_text(json.dumps(data, default=str))
            tmp.replace(self.STATE_DIR / "state.json")
        except Exception:
            pass  # persistence is best effort; never fail an operation over it

    def _load_state(self) -> None:
        path = self.STATE_DIR / "state.json"
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
        except Exception:
            return
        st = data.get("settings") or {}
        env_set = any(os.environ.get(k) for k in ("MVM_REGION", "MVM_PROFILE", "MVM_ARTIFACT_BUCKET"))
        if st and not env_set:
            try:
                self.cfg = PlaneConfig(
                    region=st.get("region") or self.cfg.region,
                    profile=st.get("profile"),
                    artifact_bucket=st.get("artifact_bucket"),
                    build_role_arn=st.get("build_role_arn"),
                    execution_role_arn=st.get("execution_role_arn"),
                )
                self._ensure_trace()
            except ValueError:
                pass
        with self._lock:
            for e in data.get("trace") or []:
                self.trace.append(e)
            self._trace_seq = max(int(data.get("trace_seq") or 0), self._trace_seq)
        for jd in data.get("jobs") or []:
            job = Job(jd["kind"], jd.get("params") or {})
            job.id, job.status, job.events = jd["id"], jd["status"], jd.get("events") or []
            job.result, job.error = jd.get("result"), jd.get("error")
            job.started, job.finished = jd.get("started") or 0, jd.get("finished")
            if job.status == "running":  # the process that ran it is gone
                job.status, job.error = "failed", "interrupted by a playground restart"
                job.finished = job.finished or time.time()
            self.jobs[job.id] = job

    def start_job(self, kind: str, params: dict, fn: Callable[[Job], Any]) -> Job:
        job = Job(kind, params)
        self.jobs[job.id] = job

        def runner():
            try:
                job.result = fn(job)
                job.status = "done"
                job.log("done", level="ok")
            except Exception as e:  # surfaced to the UI, never swallowed
                job.status = "failed"
                job.error = f"{type(e).__name__}: {e}"
                job.log(job.error, level="error", traceback=traceback.format_exc(limit=6))
            finally:
                job.finished = time.time()
                self.save_state()

        threading.Thread(target=runner, daemon=True, name=f"job-{kind}-{job.id}").start()
        self.save_state()
        return job

    # -- routing -----------------------------------------------------------------
    ROUTES: list = []

    @classmethod
    def route(cls, method: str, pattern: str):
        rx = re.compile("^" + pattern + "$")

        def deco(fn):
            cls.ROUTES.append((method, rx, fn))
            return fn

        return deco

    def api(self, method: str, path: str, query: dict, body: dict | None) -> tuple[int, Any]:
        for m, rx, fn in self.ROUTES:
            if m != method:
                continue
            mt = rx.match(path)
            if mt:
                try:
                    return 200, fn(self, query, body or {}, **mt.groupdict())
                except ApiError as e:
                    return e.status, {"error": str(e)}
                except Exception as e:
                    return 500, {
                        "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc(limit=8),
                    }
        return 404, {"error": f"no route {method} {path}"}


class ApiError(Exception):
    def __init__(self, msg: str, status: int = 400):
        super().__init__(msg)
        self.status = status


def _int(body: dict, key: str, default: Any = None) -> int | None:
    v = body.get(key, default)
    if v in (None, ""):
        return None
    return int(v)


def _need(body: dict, key: str) -> Any:
    v = body.get(key)
    if v in (None, ""):
        raise ApiError(f"missing required field '{key}'")
    return v


# ---------------------------------------------------------------- config / account
@Playground.route("GET", "/api/config")
def get_config(pg: Playground, q, b):
    cfg = pg.cfg
    acct = None
    try:
        acct = account_id(cfg.region, cfg.profile)
    except Exception as e:
        acct = f"{SAMPLE_ACCOUNT} (sample)" if pg.dry_run else f"unavailable: {type(e).__name__}"
    return {
        "sample": acct.endswith("(sample)"),
        "version": __version__,
        "dry_run": pg.dry_run,
        "region": cfg.region,
        "profile": cfg.profile,
        "artifact_bucket": cfg.artifact_bucket,
        "build_role_arn": cfg.build_role_arn,
        "execution_role_arn": cfg.execution_role_arn,
        "account": acct,
        "regions": list(SUPPORTED_REGIONS),
        "base_image_arn": cfg.base_image_arn,
        "connectors": {
            "ALL_INGRESS": cfg.ingress_all,
            "NO_INGRESS": cfg.ingress_none,
            "SHELL_INGRESS": cfg.ingress_shell,
            "INTERNET_EGRESS": cfg.egress_internet,
        },
        "size_tiers": SIZE_TIERS,
        "rates": {
            "vcpu_second": RATE_VCPU_SECOND,
            "gb_second": RATE_GB_SECOND,
            "snapshot_write_gb": RATE_SNAPSHOT_WRITE_GB,
            "snapshot_read_gb": RATE_SNAPSHOT_READ_GB,
            "storage_gb_month": RATE_STORAGE_GB_MONTH,
        },
        "cwd": os.getcwd(),
    }


@Playground.route("POST", "/api/config")
def set_config(pg: Playground, q, b):
    kw = {}
    for k in ("region", "profile", "artifact_bucket", "build_role_arn", "execution_role_arn"):
        if k in b:
            kw[k] = b[k] or None
    if "region" in kw and kw["region"] is None:
        kw["region"] = "us-east-1"
    base = {
        k: getattr(pg.cfg, k)
        for k in ("region", "profile", "artifact_bucket", "build_role_arn", "execution_role_arn")
    }
    base.update(kw)
    try:
        pg.cfg = PlaneConfig(**base)
    except ValueError as e:
        raise ApiError(str(e)) from e
    if "dry_run" in b:
        pg.dry_run = bool(b["dry_run"])
    _reset_aws_sessions(pg)
    pg.save_state()
    return get_config(pg, q, b)


_CRED_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
_ENV_KEYS = _CRED_KEYS + (
    "AWS_PROFILE",
    "AWS_DEFAULT_REGION",
    "AWS_REGION",
    "MVM_REGION",
    "MVM_PROFILE",
    "MVM_ARTIFACT_BUCKET",
    "MVM_BUILD_ROLE_ARN",
    "MVM_EXECUTION_ROLE_ARN",
)


def parse_exports(text: str) -> dict[str, str]:
    """Pull KEY=value pairs out of pasted shell lines: `export K=v`, `K=v`, `set K=v`,
    `$env:K="v"`, with optional quotes and trailing comments."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip().rstrip(";")
        if not line or line.startswith("#"):
            continue
        for prefix in ("export ", "set ", "setx ", "$env:"):
            if line.lower().startswith(prefix):
                line = line[len(prefix) :].strip()
                break
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip().upper()
        val = val.strip()
        if val[:1] in ("'", '"'):
            end = val.find(val[0], 1)
            val = val[1:end] if end > 0 else val[1:]
        elif " #" in val:
            val = val.split(" #", 1)[0].strip()
        if key in _ENV_KEYS and val:
            out[key] = val
    return out


def _reset_aws_sessions(pg: Playground) -> None:
    """Forget cached boto3 sessions and clients so the next call resolves credentials afresh."""
    from microvm import client as _client

    with _client._lock:
        _client._sessions.clear()
    _client._accounts.clear()
    with pg._lock:
        pg._ep_clients.clear()
    pg._traced.clear()
    pg._ensure_trace()


def _whoami(pg: Playground) -> dict:
    sts = lambda_client("sts", pg.cfg.region, pg.cfg.profile)
    ident = sts.get_caller_identity()
    return {"account": ident["Account"], "arn": ident["Arn"], "user_id": ident["UserId"]}


@Playground.route("GET", "/api/profiles")
def get_profiles(pg: Playground, q, b):
    """Profile names from ~/.aws/config and ~/.aws/credentials, plus what the process currently sees."""
    import botocore.session

    try:
        profiles = sorted(botocore.session.Session().available_profiles)
    except Exception:
        profiles = []
    present = {k: (k in os.environ and bool(os.environ[k])) for k in _CRED_KEYS}
    return {"profiles": profiles, "env_credentials": present, "active_profile": pg.cfg.profile}


@Playground.route("POST", "/api/credentials")
def post_credentials(pg: Playground, q, b):
    """Apply credentials for this process only: a profile name, explicit keys, or pasted export lines.
    Nothing is written to disk. Keys are never echoed back."""
    values = parse_exports(b.get("text") or "")
    for k in ("access_key_id", "secret_access_key", "session_token"):
        if b.get(k):
            values["AWS_" + k.upper()] = b[k].strip()
    profile = b.get("profile")
    if profile is not None and profile != "":
        # a chosen profile wins over any static keys sitting in the environment
        for k in _CRED_KEYS:
            os.environ.pop(k, None)
        values.pop("AWS_PROFILE", None)
        chosen = profile
    elif any(k in values for k in _CRED_KEYS):
        if not ("AWS_ACCESS_KEY_ID" in values and "AWS_SECRET_ACCESS_KEY" in values):
            raise ApiError(
                "need both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (AWS_SESSION_TOKEN optional)"
            )
        if "AWS_SESSION_TOKEN" not in values:
            os.environ.pop("AWS_SESSION_TOKEN", None)
        chosen = None  # static keys: drop the profile so the env provider is what boto3 uses
    else:
        chosen = values.get("AWS_PROFILE") or values.get("MVM_PROFILE") or pg.cfg.profile
    applied = []
    for k, v in values.items():
        os.environ[k] = v
        applied.append(k)
    region = (
        values.get("MVM_REGION")
        or values.get("AWS_REGION")
        or values.get("AWS_DEFAULT_REGION")
        or pg.cfg.region
    )
    try:
        pg.cfg = PlaneConfig(
            region=region,
            profile=chosen,
            artifact_bucket=values.get("MVM_ARTIFACT_BUCKET", pg.cfg.artifact_bucket),
            build_role_arn=values.get("MVM_BUILD_ROLE_ARN", pg.cfg.build_role_arn),
            execution_role_arn=values.get("MVM_EXECUTION_ROLE_ARN", pg.cfg.execution_role_arn),
        )
    except ValueError as e:
        raise ApiError(str(e)) from e
    _reset_aws_sessions(pg)
    try:
        who = _whoami(pg)
    except Exception as e:
        return {
            "ok": False,
            "applied": applied,
            "profile": chosen,
            "region": pg.cfg.region,
            "error": f"{type(e).__name__}: {str(e)[:300]}",
        }
    if b.get("dry_run") is not None:
        pg.dry_run = bool(b["dry_run"])
    return {
        "ok": True,
        "applied": applied,
        "profile": chosen,
        "region": pg.cfg.region,
        "session_token": "AWS_SESSION_TOKEN" in values,
        **who,
    }


@Playground.route("POST", "/api/credentials/clear")
def clear_credentials(pg: Playground, q, b):
    for k in _CRED_KEYS:
        os.environ.pop(k, None)
    _reset_aws_sessions(pg)
    return {"ok": True}


@Playground.route("GET", "/api/quotas")
def get_quotas(pg: Playground, q, b):
    applied = applied_quotas(pg.cfg)
    sample = False
    if not applied and pg.dry_run:
        applied, sample = dict(SAMPLE_QUOTAS), True
    rows = []
    for op, default in TPS.items():
        if op in ("GetMicrovm", "CreateMicrovmAuthToken"):
            continue
        got = applied.get(op)
        rows.append(
            {
                "quota": f"{op} (TPS)",
                "published": default,
                "applied": got,
                "throttle": round((got or default) * FleetManager.QUOTA_HEADROOM, 3),
            }
        )
    rows.append(
        {
            "quota": "Max allocated microVM memory (GB)",
            "published": 1024,
            "applied": applied.get("MaxMemoryGb"),
            "throttle": None,
        }
    )
    return {
        "rows": rows,
        "answered": bool(applied),
        "headroom": FleetManager.QUOTA_HEADROOM,
        "sample": sample,
    }


@Playground.route("POST", "/api/bootstrap")
def post_bootstrap(pg: Playground, q, b):
    prefix = b.get("prefix") or "microvm-ctl"

    def run(job: Job):
        from microvm.bootstrap import bootstrap

        if pg.dry_run:
            acct = account_id(pg.cfg.region, pg.cfg.profile)
            plan = {
                "bucket": pg.cfg.artifact_bucket or f"{prefix}-artifacts-{acct}-{pg.cfg.region}",
                "build_role": f"{prefix}-build-role",
                "execution_role": f"{prefix}-execution-role",
            }
            job.log("dry run: would create (idempotently) the bucket and two roles", **plan)
            return pg._dry("bootstrap", plan)
        job.log(f"bootstrapping with prefix {prefix} in {pg.cfg.region}")
        out = bootstrap(pg.cfg, prefix=prefix)
        for k, v in out.items():
            job.log(f"{k} = {v}")
        pg.cfg = PlaneConfig(
            region=pg.cfg.region,
            profile=pg.cfg.profile,
            artifact_bucket=out["artifact_bucket"],
            build_role_arn=out["build_role_arn"],
            execution_role_arn=out["execution_role_arn"],
        )
        job.log("config updated in this session; export these to make it permanent", level="ok")
        out["exports"] = "\n".join(f"export MVM_{k.upper()}={v}" for k, v in out.items())
        return out

    return pg.start_job("bootstrap", {"prefix": prefix}, run).to_dict()


# ---------------------------------------------------------------- images
@Playground.route("GET", "/api/images")
def get_images(pg: Playground, q, b):
    try:
        items = ImageBuilder(pg.cfg).list_images()
    except Exception:
        if not pg.dry_run:
            raise
        return {"items": _sample_images(pg.cfg.region), "sample": True}
    return {"items": [_redact(i) for i in items]}


@Playground.route("GET", r"/api/images/(?P<name>[^/]+)/versions")
def get_versions(pg: Playground, q, b, name: str):
    try:
        return {"items": [_redact(v) for v in ImageBuilder(pg.cfg).list_versions(name)]}
    except Exception:
        if not pg.dry_run:
            raise
        return {"items": _sample_versions(name), "sample": True}


@Playground.route("GET", "/api/examples")
def get_examples(pg: Playground, q, b):
    """Directories with a Dockerfile at their root, near the working directory."""
    roots = [Path.cwd(), Path.cwd().parent / "awesome-microvm" / "examples"]
    for extra in (os.environ.get("MVM_PLAYGROUND_DIRS") or "").split(os.pathsep):
        if extra:
            roots.append(Path(extra).expanduser())
    found = []
    for root in roots:
        if not root.exists():
            continue
        for p in sorted(root.glob("**/Dockerfile")):
            if any(part in {".git", "node_modules", ".venv", "__pycache__"} for part in p.parts):
                continue
            if len(p.relative_to(root).parts) > 3:
                continue
            found.append(str(p.parent))
    return {"dirs": sorted(set(found))}


@Playground.route("POST", "/api/images/build")
def post_build(pg: Playground, q, b):
    name = _need(b, "name")
    app_dir = _need(b, "dir")
    memory = _int(b, "memory_mib", 2048)
    env = b.get("environment") or None
    caps = bool(b.get("caps_all"))
    desc = b.get("description") or None
    port = _int(b, "hooks_port", 8080)
    params = {
        "name": name,
        "dir": app_dir,
        "memory_mib": memory,
        "environment": env,
        "caps_all": caps,
        "description": desc,
        "hooks_port": port,
    }

    def run(job: Job):
        builder = ImageBuilder(pg.cfg)
        payload = builder.package(app_dir)
        names = zipfile.ZipFile(io.BytesIO(payload)).namelist()
        job.log(f"packaged {len(names)} files, {len(payload) / 1e6:.2f} MB", files=names[:60])
        hooks = default_hooks(port)
        if pg.dry_run:
            req = {
                "baseImageArn": pg.cfg.base_image_arn,
                "buildRoleArn": pg.cfg.build_role_arn,
                "codeArtifact": {
                    "uri": f"s3://{pg.cfg.artifact_bucket or SAMPLE_BUCKET}/microvm-images/{name}/<ts>.zip"
                },
                "resources": [{"minimumMemoryInMiB": memory}],
                "cpuConfigurations": [{"architecture": "ARM_64"}],
                "hooks": hooks,
                "egressNetworkConnectors": [pg.cfg.egress_internet],
            }
            if env:
                req["environmentVariables"] = env
            if caps:
                req["additionalOsCapabilities"] = ["ALL"]
            job.log("dry run: would upload the zip and call Create/UpdateMicrovmImage", request=req)
            return pg._dry("CreateMicrovmImage", req, "zip packaged locally, nothing uploaded")
        built = builder.build(
            name,
            app_dir,
            memory_mib=memory,
            hooks=hooks,
            environment=env,
            os_capabilities_all=caps,
            description=desc,
            log=job.log,
        )
        return built.__dict__

    return pg.start_job("build", params, run).to_dict()


# ---------------------------------------------------------------- vms
def _vm_dict(vm) -> dict:
    return {
        "microvm_id": vm.microvm_id,
        "state": vm.state,
        "image": vm.image_arn.split(":")[-1],
        "image_arn": vm.image_arn,
        "version": vm.image_version,
        "started_at": vm.started_at.isoformat() if hasattr(vm.started_at, "isoformat") else None,
        "started_epoch": vm.started_epoch,
        "endpoint": vm.endpoint,
    }


@Playground.route("GET", "/api/vms")
def get_vms(pg: Playground, q, b):
    image = (q.get("image") or [None])[0]
    show_all = (q.get("all") or ["0"])[0] in ("1", "true")
    fm = pg.fm()
    vms = fm.list(image or None)
    if not show_all:
        vms = [v for v in vms if v.state != "TERMINATED"]
    by_state: dict[str, int] = {}
    for v in vms:
        by_state[v.state] = by_state.get(v.state, 0) + 1
    return {"items": [_vm_dict(v) for v in vms], "by_state": by_state, "sample": getattr(fm, "sample", False)}


@Playground.route("GET", r"/api/vms/(?P<vid>[^/]+)")
def get_vm(pg: Playground, q, b, vid: str):
    try:
        raw = pg.fm().api.get_microvm(microvmIdentifier=vid)
    except Exception:
        if not pg.dry_run:
            raise
        for vm in _sample_vms(pg.cfg.region):
            if vm.microvm_id == vid:
                d = _vm_dict(vm)
                return {
                    "raw": {
                        "microvmId": vid,
                        "state": vm.state,
                        "endpoint": vm.endpoint,
                        "imageArn": vm.image_arn,
                        "imageVersion": "1.0",
                        "startedAt": d["started_at"],
                        "maximumDurationInSeconds": 28800,
                        "idlePolicy": IdlePolicy().to_api(),
                        "sample": True,
                    },
                    "sample": True,
                }
        raise
    return {"raw": _redact(raw)}


@Playground.route("POST", "/api/vms/run")
def post_run(pg: Playground, q, b):
    image = _need(b, "image")
    count = max(1, _int(b, "count", 1) or 1)
    policy = IdlePolicy(
        max_idle=_int(b, "idle", 300) or 300,
        suspended_for=_int(b, "suspended_for", 3600) or 3600,
        auto_resume=bool(b.get("auto_resume", True)),
    )
    kw = dict(
        version=b.get("version") or None,
        idle_policy=policy,
        run_payload=b.get("payload") or None,
        max_duration=_int(b, "max_duration"),
        execution_role=b.get("execution_role") or None,
        ingress=b.get("ingress") or None,
        egress=b.get("egress") or None,
    )
    wait = bool(b.get("wait", True))
    probe = bool(b.get("probe_health", False))
    health = b.get("health_path") or "/healthz"

    def run(job: Job):
        fm = pg.fm()
        params = fm.run_params(image, **kw)
        job.log(f"RunMicrovm request (x{count})", request=params, throttle=f"{fm.tps('RunMicrovm'):g}/s")
        if pg.dry_run:
            return pg._dry("RunMicrovm", params, f"would launch {count} VM(s)")
        launched = []
        for _i in range(count):
            t0 = time.time()
            vm = fm.run(image, **kw)
            job.log(f"launched {vm.microvm_id} ({vm.state}) in {time.time() - t0:.2f}s", endpoint=vm.endpoint)
            rec = _vm_dict(vm)
            rec["api_s"] = round(time.time() - t0, 2)
            if wait:
                vm = fm.wait_until(vm.microvm_id, "RUNNING", timeout=180)
                rec["running_s"] = round(time.time() - t0, 2)
                job.log(f"{vm.microvm_id} is RUNNING after {rec['running_s']}s")
                rec.update(_vm_dict(vm))
            if wait and probe:
                client = pg.ep(vm.microvm_id, endpoint=vm.endpoint)
                ttfb = client.wait_ready(health, timeout=120)
                rec["first_byte_s"] = round(time.time() - t0, 2)
                job.log(f"{vm.microvm_id} answered {health} after {rec['first_byte_s']}s (poll took {ttfb}s)")
            launched.append(rec)
        return {"launched": launched}

    return pg.start_job(
        "run",
        {
            "image": image,
            "count": count,
            "idle": policy.max_idle,
            "suspended_for": policy.suspended_for,
            "wait": wait,
            **{k: v for k, v in kw.items() if k != "idle_policy"},
        },
        run,
    ).to_dict()


@Playground.route("POST", r"/api/vms/(?P<vid>[^/]+)/(?P<verb>suspend|resume|terminate)")
def post_lifecycle(pg: Playground, q, b, vid: str, verb: str):
    if pg.dry_run:
        return pg._dry(f"{verb.capitalize()}Microvm", {"microvmIdentifier": vid})
    fm = pg.fm()
    t0 = time.time()
    getattr(fm, verb)(vid)
    out = {"ok": True, "verb": verb, "microvm_id": vid, "ms": round((time.time() - t0) * 1000)}
    if b.get("wait"):
        target = {"suspend": "SUSPENDED", "resume": "RUNNING", "terminate": "TERMINATED"}[verb]
        vm = fm.wait_until(vid, target, timeout=180)
        out["state"] = vm.state
        out["wait_s"] = round(time.time() - t0, 2)
    return out


# ---------------------------------------------------------------- job inside the VM
def _http_status_of(e: Exception) -> int | None:
    """EndpointError messages read '<id> answered 404 on /status'; pull the code out."""
    m = re.search(r"answered (\d{3})", str(e))
    return int(m.group(1)) if m else None


@Playground.route("GET", r"/api/vms/(?P<vid>[^/]+)/job")
def get_vm_job(pg: Playground, q, b, vid: str):
    """The hook runtime's `GET /status` snapshot for one VM: phase, elapsed, progress, counters,
    the log tail (only lines above `since` when given) and the lease state. A VM that cannot
    answer (not RUNNING, no telemetry runtime on the image) comes back as `{"error", "status"}`
    with HTTP 200, so the panel shows the reason instead of raising."""
    since_raw = (q.get("since") or [""])[0]
    since = int(since_raw) if since_raw not in ("", None) else None
    port_raw = (q.get("port") or [""])[0]
    port = int(port_raw) if port_raw not in ("", None) else None
    try:
        return pg.ep(vid, ports=[port] if port else None).status(since)
    except EndpointError as e:
        return {"error": str(e), "status": _http_status_of(e), "microvm_id": vid}
    except Exception as e:
        if pg.dry_run:  # AWS did not answer: the sample job, moving with the clock
            return {**_sample_job(vid, since), "sample": True}
        return {"error": f"{type(e).__name__}: {str(e)[:300]}", "status": None, "microvm_id": vid}


_HOOK_LEVELS = {"warning": "warn", "warn": "warn", "error": "error", "ok": "ok"}


def _follow_status(pg: Playground, job: Job, vm, timeout: float = 300.0) -> dict | None:
    """Poll `/status` every 2 s, copying each new phase and log line into the job's events,
    until the lease reports done or lost, the VM stops answering, or `timeout` passes.
    Returns the last snapshot seen."""
    client = pg.ep(vm.microvm_id, endpoint=vm.endpoint)
    deadline = time.time() + timeout
    since: int | None = None
    phase: str | None = None
    snap: dict | None = None
    misses = 0
    while time.time() < deadline:
        try:
            snap = client.status(since)
            misses = 0
        except EndpointError as e:
            job.log(f"/status unavailable, not following: {e}", level="warn", status=_http_status_of(e))
            return snap
        except Exception as e:
            misses += 1
            if misses >= 5:
                msg = f"/status unreachable after {misses} attempts: {type(e).__name__}: {e}"
                job.log(msg, level="error")
                return snap
            time.sleep(2)
            continue
        if snap.get("phase") != phase:
            phase = snap.get("phase")
            job.log(
                f"phase {phase} at {snap.get('elapsed_s')}s in the VM",
                level="ok",
                progress=snap.get("progress"),
                counters=snap.get("counters"),
            )
        for line in snap.get("log_tail") or []:
            extra = {k: v for k, v in line.items() if k not in ("seq", "t", "level", "msg", "phase")}
            job.log(
                f"[{line.get('phase')}] {line.get('msg')}",
                level=_HOOK_LEVELS.get(str(line.get("level")), "info"),
                vm_time=line.get("t"),
                **extra,
            )
        since = snap.get("seq", since)
        lease = snap.get("lease") or {}
        if lease.get("done"):
            err = lease.get("error")
            if err:
                msg = f"lease failed: {err.get('error_type')}: {err.get('message')}"
                job.log(msg, level="error", error=err)
            else:
                beats = lease.get("heartbeats", 0)
                job.log(f"lease done after {snap.get('elapsed_s')}s, {beats} heartbeats", level="ok")
            return snap
        if lease.get("lost"):
            job.log("lease lost: the orchestrator stopped waiting", level="warn")
            return snap
        time.sleep(2)
    job.log(f"stopped following after {timeout:g}s; the VM keeps working", level="warn")
    return snap


@Playground.route("POST", "/api/lease/run")
def post_lease_run(pg: Playground, q, b):
    """RunMicrovm with a lease in runHookPayload (`FleetManager.lease`), then follow the
    job inside the VM through `/status` until the lease completes."""
    image = _need(b, "image")
    kind = b.get("kind") or "none"
    task = b.get("task")
    if isinstance(task, str):
        try:
            task = json.loads(task) if task.strip() else {}
        except ValueError as e:
            raise ApiError(f"task is not valid JSON: {e}") from e
    if task is None:
        task = {}
    if not isinstance(task, dict):
        raise ApiError("task must be a JSON object")
    lease = Lease(
        kind=kind,
        token=b.get("token") or "",
        region=pg.cfg.region,
        target=b.get("target") or None,
        heartbeat_s=_int(b, "heartbeat_every", 30) or 30,
        id=b.get("id") or None,
    )
    policy = LeasePolicy(
        budget_s=_int(b, "budget", 900) or 900,
        heartbeat_timeout_s=_int(b, "heartbeat_timeout", 120) or 120,
        slack_s=_int(b, "slack", 120) or 120,
    )
    try:
        payload = encode_payload(lease, task)
    except ValueError as e:
        raise ApiError(str(e)) from e
    token = client_token(lease)
    version = b.get("version") or None
    role = b.get("execution_role") or None
    wait = bool(b.get("wait", True))
    params = {
        "image": image,
        "kind": kind,
        "id": lease.id,
        "target": lease.target,
        "token": _mask(lease.token) if lease.token else None,
        "budget": policy.budget_s,
        "heartbeat_timeout": policy.heartbeat_timeout_s,
        "slack": policy.slack_s,
        "heartbeat_every": lease.heartbeat_s,
        "wait": wait,
        "client_token": token,
    }

    def run(job: Job):
        fm = pg.fm()
        job.log(
            f"lease {kind}: runHookPayload is {len(payload)} chars (limit 4096), clientToken {token[:12]}…",
            lease=lease.to_dict(),
            task=task,
            payload=payload,
            client_token=token,
            policy={
                "budget_s": policy.budget_s,
                "heartbeat_timeout_s": policy.heartbeat_timeout_s,
                "slack_s": policy.slack_s,
                "max_duration_s": policy.max_duration(),
            },
        )
        req = fm.run_params(
            image,
            version=version,
            idle_policy=policy.idle_policy(),
            run_payload=payload,
            max_duration=policy.max_duration(),
            execution_role=role,
            client_token=token,
        )
        job.log("RunMicrovm request", request=req, throttle=f"{fm.tps('RunMicrovm'):g}/s")
        if pg.dry_run:
            return pg._dry(
                "RunMicrovm",
                req,
                f"would lease one VM ({kind}) with a {policy.budget_s}s budget, cap {policy.max_duration()}s",
            )
        t0 = time.time()
        vm = fm.lease(image, lease, task, policy, version=version, execution_role=role)
        job.log(f"launched {vm.microvm_id} ({vm.state}) in {time.time() - t0:.2f}s", endpoint=vm.endpoint)
        rec = _vm_dict(vm)
        rec["api_s"] = round(time.time() - t0, 2)
        final = None
        if wait:
            vm = fm.wait_until(vm.microvm_id, "RUNNING", timeout=180)
            rec["running_s"] = round(time.time() - t0, 2)
            rec.update(_vm_dict(vm))
            job.log(f"{vm.microvm_id} is RUNNING after {rec['running_s']}s; following /status every 2 s")
            final = _follow_status(pg, job, vm)
            rec["followed_s"] = round(time.time() - t0, 2)
        return {"launched": [rec], "lease": lease.to_dict(), "client_token": token, "final": final}

    return pg.start_job("lease", params, run).to_dict()


# ---------------------------------------------------------------- fleet
def _fleet(pg: Playground, b: dict) -> Fleet:
    tmpl = b.get("payload_template") or None
    factory = (lambda i: tmpl.replace("{i}", str(i))) if tmpl else None
    return Fleet(
        pg.fm(),
        _need(b, "image"),
        version=b.get("version") or None,
        idle_policy=IdlePolicy(
            max_idle=_int(b, "idle", 300) or 300,
            suspended_for=_int(b, "suspended_for", 3600) or 3600,
            auto_resume=bool(b.get("auto_resume", True)),
        ),
        max_duration=_int(b, "max_duration"),
        run_payload_factory=factory,
        execution_role=b.get("execution_role") or None,
    )


@Playground.route("POST", "/api/fleet/scale")
def post_scale(pg: Playground, q, b):
    n = _int(b, "n", 0) or 0
    wait = bool(b.get("wait", False))

    def run(job: Job):
        fleet = _fleet(pg, b)
        members = fleet.members()
        delta = n - len(members)
        job.log(
            f"fleet {fleet.image}: {len(members)} active, desired {n}, delta {delta:+d}",
            members=[_vm_dict(v) for v in members],
            throttle=f"{fleet.manager.tps('RunMicrovm'):g}/s",
        )
        if delta < 0:
            victims = Fleet.scale_down_victims(members, -delta)
            job.log(
                "scale-down victims (SUSPENDED first, then youngest RUNNING)",
                victims=[{"id": v.microvm_id, "state": v.state} for v in victims],
            )
        if pg.dry_run:
            if delta > 0:
                params = fleet.manager.run_params(
                    fleet.image,
                    version=fleet.version,
                    idle_policy=fleet.idle_policy,
                    run_payload=fleet.run_payload_factory(0) if fleet.run_payload_factory else None,
                    max_duration=fleet.max_duration,
                    execution_role=fleet.execution_role,
                )
                return pg._dry("RunMicrovm", params, f"would launch {delta} VM(s) through the bucket")
            if delta < 0:
                return pg._dry(
                    "TerminateMicrovm",
                    {"victims": [v.microvm_id for v in victims]},
                    f"would terminate {-delta} VM(s)",
                )
            return pg._dry("scale_to", {"delta": 0}, "already converged")
        t0 = time.time()
        launched = fleet.scale_to(n, wait_running=wait)
        if delta < 0:
            time.sleep(3)  # TerminateMicrovm is asynchronous; let the states change before counting
        after = fleet.size()
        job.log(f"converged to {after} in {time.time() - t0:.1f}s wall", level="ok")
        return {
            "before": len(members),
            "after": after,
            "wall_s": round(time.time() - t0, 1),
            "launched": [_vm_dict(v) for v in launched],
        }

    return pg.start_job("scale", {k: v for k, v in b.items()}, run).to_dict()


@Playground.route("POST", r"/api/fleet/(?P<verb>drain|suspend_all|resume_all|reap)")
def post_fleet_verb(pg: Playground, q, b, verb: str):
    def run(job: Job):
        fleet = _fleet(pg, b)
        members = fleet.members()
        if verb == "reap":
            age = _int(b, "max_age", 7200) or 7200
            now = time.time()
            targets = [v for v in members if v.started_epoch and now - v.started_epoch > age]
        elif verb == "suspend_all":
            targets = [v for v in members if v.state == "RUNNING"]
        elif verb == "resume_all":
            targets = [v for v in members if v.state == "SUSPENDED"]
        else:
            targets = members
        job.log(
            f"{verb} on {fleet.image}: {len(targets)} of {len(members)} members affected",
            targets=[{"id": v.microvm_id, "state": v.state} for v in targets],
        )
        if pg.dry_run:
            return pg._dry(verb, {"targets": [v.microvm_id for v in targets]})
        t0 = time.time()
        n = fleet.reap(_int(b, "max_age", 7200) or 7200) if verb == "reap" else getattr(fleet, verb)()
        count = len(n) if isinstance(n, list) else n
        job.log(f"{verb}: {count} VM(s) in {time.time() - t0:.1f}s", level="ok")
        return {"count": count, "wall_s": round(time.time() - t0, 1)}

    return pg.start_job(verb, {k: v for k, v in b.items()}, run).to_dict()


@Playground.route("GET", "/api/fleet/cost")
def get_fleet_cost(pg: Playground, q, b):
    image = (q.get("image") or [None])[0] or None
    mem = float((q.get("memory_gb") or ["2"])[0])
    try:
        return FleetMonitor(pg.cfg).estimate_fleet_cost_per_hour(image, mem)
    except Exception:
        if not pg.dry_run:
            raise
        vms = [v for v in _sample_vms(pg.cfg.region) if not image or v.image_arn.split(":")[-1] == image]
        model = CostModel(memory_gb=mem)
        running = sum(v.state in ("RUNNING", "PENDING") for v in vms)
        suspended = sum(v.state == "SUSPENDED" for v in vms)
        return {
            "running_vms": running,
            "suspended_vms": suspended,
            "running_usd_per_hour": round(running * model.running_cost(3600), 4),
            "suspended_usd_per_hour": round(suspended * model.suspended_cost(3600), 6),
            "sample": True,
        }


# ---------------------------------------------------------------- execution plane
@Playground.route("POST", "/api/call")
def post_call(pg: Playground, q, b):
    vid = _need(b, "id")
    method = (b.get("method") or "GET").upper()
    path = b.get("path") or "/healthz"
    port = _int(b, "port")
    ttl = _int(b, "ttl", 15) or 15
    ports = [port] if port else None
    body = b.get("body")
    headers = dict(b.get("headers") or {})
    data = None
    if body not in (None, ""):
        data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        headers.setdefault("Content-Type", "application/json")
    if pg.dry_run:
        url = f"https://{pg.sample_endpoint(vid)}{path if path.startswith('/') else '/' + path}"
        return pg._dry(
            "endpoint request",
            {
                "method": method,
                "url": url,
                "headers": {
                    **headers,
                    "X-aws-proxy-auth": "<JWE minted via CreateMicrovmAuthToken>",
                    **({"X-aws-proxy-port": str(port)} if port and port != 8080 else {}),
                },
                "body": body,
            },
        )
    client = pg.ep(vid, ports=ports, ttl=ttl)
    url = f"https://{client.endpoint}{path if path.startswith('/') else '/' + path}"
    t0 = time.time()
    minted_before = client._token_expiry
    resp = client.request(
        method,
        path,
        port=port,
        data=data,
        headers=headers,
        timeout=float(b.get("timeout") or 60),
        max_attempts=_int(b, "max_attempts", 6) or 6,
        resume_patience=float(b.get("resume_patience") or 30),
    )
    ms = round((time.time() - t0) * 1000)
    try:
        parsed = resp.json()
    except ValueError:
        parsed = None
    return {
        "status": resp.status_code,
        "ms": ms,
        "url": url,
        "method": method,
        "request_headers": {
            k: (_mask(v) if k.lower() == "x-aws-proxy-auth" else v) for k, v in resp.request.headers.items()
        },
        "response_headers": dict(resp.headers),
        "json": parsed,
        "text": None if parsed is not None else resp.text[:20000],
        "token": {
            "minted_this_call": client._token_expiry != minted_before,
            "expires_in_s": round(client._token_expiry - time.time()),
            "ports": client.ports,
        },
    }


@Playground.route("POST", "/api/token")
def post_token(pg: Playground, q, b):
    vid = _need(b, "id")
    ttl = _int(b, "ttl", 15) or 15
    ports = b.get("ports") or [8080]
    all_ports = bool(b.get("all_ports"))
    spec = [{"allPorts": {}}] if all_ports else [{"port": int(p)} for p in ports]
    params = {"microvmIdentifier": vid, "expirationInMinutes": ttl, "allowedPorts": spec}
    if pg.dry_run:
        return pg._dry("CreateMicrovmAuthToken", params)
    api = lambda_client("lambda-microvms", pg.cfg.region, pg.cfg.profile)
    t0 = time.time()
    resp = api.create_microvm_auth_token(**params)
    tok = dict(resp["authToken"])
    return {
        "headers": tok,
        "masked": {k: _mask(v) for k, v in tok.items()},
        "ttl_minutes": ttl,
        "cache_until_s": round(ttl * 60 * 0.8),
        "ms": round((time.time() - t0) * 1000),
        "request": params,
    }


# ---------------------------------------------------------------- logs / cost
@Playground.route("GET", "/api/logs")
def get_logs(pg: Playground, q, b):
    image = (q.get("image") or [None])[0]
    if not image:
        raise ApiError("image is required")
    minutes = int((q.get("minutes") or ["15"])[0])
    limit = int((q.get("limit") or ["200"])[0])
    try:
        events = FleetMonitor(pg.cfg).tail_logs(image, minutes=minutes, limit=limit)
    except Exception:
        if not pg.dry_run:
            raise
        return {"group": f"/aws/lambda/microvms/{image}", "events": _sample_logs(image), "sample": True}
    return {"group": f"/aws/lambda/microvms/{image}", "events": events}


@Playground.route("POST", "/api/cost")
def post_cost(pg: Playground, q, b):
    model = CostModel(
        memory_gb=float(b.get("memory_gb") or 2),
        snapshot_gb=float(b["snapshot_gb"]) if b.get("snapshot_gb") else None,
    )
    s = model.session(
        float(b.get("active_s") or 0), float(b.get("suspended_s") or 0), int(b.get("cycles") or 0)
    )
    s["vcpu"] = model.vcpu
    return s


# ---------------------------------------------------------------- probe (mini benchmark)
@Playground.route("POST", "/api/probe")
def post_probe(pg: Playground, q, b):
    image = _need(b, "image")
    launches = max(1, _int(b, "launches", 2) or 2)
    warm = max(0, _int(b, "warm_calls", 10) or 0)
    method = (b.get("method") or "GET").upper()
    path = b.get("path") or "/healthz"
    health = b.get("health_path") or "/healthz"
    body = b.get("body") or None
    do_sr = bool(b.get("suspend_resume", True))
    do_auto = bool(b.get("auto_resume", True))
    term = bool(b.get("terminate_after", True))
    policy = IdlePolicy(
        max_idle=_int(b, "idle", 1800) or 1800, suspended_for=_int(b, "suspended_for", 3600) or 3600
    )
    params = {
        "image": image,
        "launches": launches,
        "warm_calls": warm,
        "method": method,
        "path": path,
        "suspend_resume": do_sr,
        "auto_resume": do_auto,
        "terminate_after": term,
    }

    def run(job: Job):
        fm = pg.fm()
        out: dict = {"image": image, "region": pg.cfg.region}
        if pg.dry_run:
            job.log(
                "dry run: plan",
                plan={
                    "1": f"launch {launches} VM(s) one at a time, timing RunMicrovm -> RUNNING -> "
                    f"first 200 on {health}; keep one probe",
                    "2": f"{warm} warm {method} {path} calls",
                    "3": "explicit suspend/resume with state check" if do_sr else "skipped",
                    "4": "auto-resume: suspend, then hit the endpoint" if do_auto else "skipped",
                    "5": "terminate the probe" if term else "leave the probe running",
                },
            )
            return pg._dry("probe", params, "nothing launched")
        samples = []
        probe = None
        for i in range(launches):
            deadline = time.time() + 180
            while time.time() < deadline:
                live = [v for v in fm.list(image) if v.state != "TERMINATED"]
                if len(live) <= (1 if probe else 0):
                    break
                job.log(f"waiting for {len(live)} live VM(s) of {image} to settle before launch {i + 1}")
                time.sleep(5)
            t0 = time.time()
            vm = fm.run(image, idle_policy=policy)
            t_api = time.time() - t0
            vm = fm.wait_until(vm.microvm_id, "RUNNING", timeout=180)
            t_run = time.time() - t0
            client = pg.ep(vm.microvm_id, endpoint=vm.endpoint)
            client.wait_ready(health, timeout=120)
            t_first = time.time() - t0
            samples.append(
                {
                    "id": vm.microvm_id,
                    "api_s": round(t_api, 2),
                    "running_s": round(t_run, 2),
                    "first_byte_s": round(t_first, 2),
                }
            )
            job.log(
                f"launch {i + 1}: api {t_api:.2f}s -> RUNNING {t_run:.2f}s -> serving {t_first:.2f}s",
                vm=vm.microvm_id,
            )
            if probe:
                fm.terminate(vm.microvm_id)
                job.log(f"terminated {vm.microvm_id} (keeping {probe.microvm_id} as the probe)")
            else:
                probe = vm
        fb = [s["first_byte_s"] for s in samples]
        out["launch"] = {
            "samples": samples,
            "first_byte_p50_s": round(statistics.median(fb), 2),
            "first_byte_p95_s": round(_pct(fb, 95), 2),
        }
        client = pg.ep(probe.microvm_id, endpoint=probe.endpoint)
        lat = []
        for _ in range(warm):
            t0 = time.time()
            r = client.request(
                method,
                path,
                json=body if isinstance(body, (dict, list)) else None,
                data=body.encode() if isinstance(body, str) else None,
                headers={"Content-Type": "application/json"} if body else {},
            )
            lat.append(round((time.time() - t0) * 1000, 1))
            if r.status_code != 200:
                job.log(f"warm call returned {r.status_code}", level="warn", body=r.text[:300])
        if lat:
            out["warm_ms"] = {
                "samples": lat,
                "p50": round(statistics.median(lat), 1),
                "p95": round(_pct(lat, 95), 1),
                "min": round(min(lat), 1),
            }
            job.log(
                f"warm {method} {path}: p50 {out['warm_ms']['p50']} ms, "
                f"p95 {out['warm_ms']['p95']} ms over {len(lat)} calls"
            )
        if do_sr:
            before = client.get("/healthz").json() if health == "/healthz" else None
            t0 = time.time()
            fm.suspend(probe.microvm_id)
            fm.wait_until(probe.microvm_id, "SUSPENDED", timeout=120)
            t_s = time.time() - t0
            t0 = time.time()
            fm.resume(probe.microvm_id)
            fm.wait_until(probe.microvm_id, "RUNNING", timeout=120)
            after = client.get("/healthz").json() if health == "/healthz" else None
            t_r = time.time() - t0
            out["suspend_resume"] = {
                "suspend_s": round(t_s, 2),
                "resume_to_serving_s": round(t_r, 2),
                "same_microvm_id": (before or {}).get("microvmId") == (after or {}).get("microvmId")
                if before and after
                else None,
            }
            job.log(f"suspend {t_s:.1f}s, resume to serving {t_r:.1f}s")
        if do_auto:
            fm.suspend(probe.microvm_id)
            fm.wait_until(probe.microvm_id, "SUSPENDED", timeout=120)
            t0 = time.time()
            r = client.get(health, resume_patience=90)
            t_a = time.time() - t0
            out["auto_resume"] = {"first_request_s": round(t_a, 2), "status": r.status_code}
            job.log(f"auto-resume: first request to the SUSPENDED VM answered {r.status_code} in {t_a:.1f}s")
        if term:
            fm.terminate(probe.microvm_id)
            job.log(f"terminated probe {probe.microvm_id}")
        else:
            job.log(f"probe {probe.microvm_id} left RUNNING", level="warn")
        out["probe"] = probe.microvm_id
        return out

    return pg.start_job("probe", params, run).to_dict()


# ---------------------------------------------------------------- jobs / trace
@Playground.route("GET", "/api/jobs")
def get_jobs(pg: Playground, q, b):
    jobs = sorted(pg.jobs.values(), key=lambda j: j.started, reverse=True)
    return {"items": [j.to_dict() for j in jobs[:50]]}


@Playground.route("GET", r"/api/jobs/(?P<jid>[^/]+)")
def get_job(pg: Playground, q, b, jid: str):
    job = pg.jobs.get(jid)
    if not job:
        raise ApiError("no such job", 404)
    return job.to_dict()


@Playground.route("GET", "/api/trace")
def get_trace(pg: Playground, q, b):
    since = int((q.get("since") or ["0"])[0])
    with pg._lock:
        items = [e for e in pg.trace if e["seq"] > since]
        return {"items": items, "seq": pg._trace_seq, "total": len(pg.trace)}


@Playground.route("DELETE", "/api/trace")
def clear_trace(pg: Playground, q, b):
    with pg._lock:
        pg.trace.clear()
    return {"ok": True}


# ---------------------------------------------------------------- http
def _make_handler(pg: Playground):
    index = (STATIC_DIR / "index.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _json(self, status: int, obj: Any) -> None:
            body = json.dumps(obj, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _dispatch(self, method: str) -> None:
            u = urlparse(self.path)
            if not u.path.startswith("/api/"):
                if u.path in ("/", "/index.html") and method == "GET":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(index)))
                    self.end_headers()
                    self.wfile.write(index)
                    return
                return self._json(404, {"error": "not found"})
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b""
            try:
                body = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                return self._json(400, {"error": "body must be JSON"})
            status, obj = pg.api(method, u.path, parse_qs(u.query), body)
            self._json(status, obj)

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def do_DELETE(self):
            self._dispatch("DELETE")

        def log_message(self, *a):
            pass

    return Handler


def serve(
    host: str = "127.0.0.1",
    port: int = 8765,
    dry_run: bool = False,
    open_browser: bool = True,
    cfg: PlaneConfig | None = None,
) -> None:
    pg = Playground(cfg, dry_run=dry_run)
    server = ThreadingHTTPServer((host, port), _make_handler(pg))
    url = f"http://{host}:{port}/"
    print(
        f"microvm-ctl playground {__version__} on {url}  "
        f"region={pg.cfg.region} profile={pg.cfg.profile or '-'}"
        f"{'  DRY RUN' if dry_run else ''}"
    )
    if open_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        pg.save_state()


_LAMBDA_PG: Playground | None = None


def lambda_handler(event: dict, context: Any = None) -> dict:
    """Adapter for a Lambda Function URL (payload v2). Synchronous endpoints only;
    jobs need a long-lived process, see docs/playground.md."""
    global _LAMBDA_PG
    if _LAMBDA_PG is None:
        _LAMBDA_PG = Playground(dry_run=os.environ.get("MVM_PLAYGROUND_DRY_RUN") == "1")
    http = event.get("requestContext", {}).get("http", {})
    method = http.get("method", "GET")
    path = event.get("rawPath", "/")
    query = {k: [v] for k, v in (event.get("queryStringParameters") or {}).items()}
    body = None
    if event.get("body"):
        import base64

        raw = base64.b64decode(event["body"]) if event.get("isBase64Encoded") else event["body"]
        body = json.loads(raw)
    if not path.startswith("/api/"):
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/html; charset=utf-8"},
            "body": (STATIC_DIR / "index.html").read_text(),
        }
    status, obj = _LAMBDA_PG.api(method, path, query, body)
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(obj, default=str),
    }
