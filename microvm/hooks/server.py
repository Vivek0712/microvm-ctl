"""Zero-dependency in-VM hook server implementing the Lambda MicroVMs contract.

Lambda drives the microVM lifecycle by calling HTTP hooks that *your app*
serves (stdlib only — nothing to install inside the image):

  build-time   GET/POST /aws/lambda-microvms/runtime/v1/ready     503 until warm, 200 => snapshot now
               GET/POST /aws/lambda-microvms/runtime/v1/validate  runs on a fresh VM from the image
  runtime      POST     /aws/lambda-microvms/runtime/v1/run       after clone; traffic starts on 200
               POST     /aws/lambda-microvms/runtime/v1/resume    VM stays SUSPENDED until 200
               POST     /aws/lambda-microvms/runtime/v1/suspend   flush before snapshot
               POST     /aws/lambda-microvms/runtime/v1/terminate cleanup before teardown

Because every VM is cloned from one snapshot, the /run hook is where you
restore uniqueness (reseed RNG, regenerate IDs, fetch per-tenant secrets from
the runHookPayload) — never at build time. Return fast; Lambda retries /ready
on 503, and a held-open hook request at timeout fails the build.

Usage:
    app = HookApp()

    @app.on_run
    def on_run(ctx):            # ctx = {"microvmId": ..., "runHookPayload": ...}
        seed_rng(); load_tenant(ctx.get("runHookPayload"))

    @app.route("POST", "/execute")
    def execute(body, headers):
        return 200, {"result": run(body["code"])}

    @app.on_lease                # runs in a thread when runHookPayload carries a lease
    def work(task, lease):
        app.job.phase("working")
        return {"ok": True}      # delivered to the orchestrator by the lease's completer

    app.serve(port=8080)

Built-in routes: GET /healthz, GET /status[?since=SEQ] (job telemetry snapshot),
GET /events (Server-Sent Events: one `data:` line per log line, a `snapshot` event every 5 s).
"""

from __future__ import annotations

import collections
import json
import os
import random
import select
import socket
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOOK_BASE = "/aws/lambda-microvms/runtime/v1"
LEASE_KINDS = ("sfn", "durable", "http", "sqs", "eventbridge", "none")
RESULT_LIMIT = 240 * 1024
SUMMARY_LIMIT = 4096


def _utc(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + "Z"


# -- job telemetry ---------------------------------------------------------------
class Job:
    """Phase, progress, counters, and a ring buffer of log lines for one VM's work.

    Thread-safe; every log line also goes to stdout as one JSON object so the
    service's CloudWatch log group carries the same story as /status."""

    RING = 200
    TAIL = 50

    def __init__(self, microvm_id: str | None = None):
        self._lock = threading.Lock()
        self.microvm_id = microvm_id
        self.lease: LeaseContext | None = None
        self.seq = 0
        self._ring: collections.deque = collections.deque(maxlen=self.RING)
        self.reset()

    def reset(self) -> None:
        """Restart the clock and clear phase/progress/counters (the log and seq stay monotonic)."""
        with self._lock:
            self.started = time.time()
            self.phase_name = "init"
            self.done = 0
            self.total: int | None = None
            self.counters: dict = {}

    def phase(self, name: str) -> None:
        with self._lock:
            self.phase_name = name
        self.log(f"phase: {name}")

    def progress(self, done: int, total: int | None = None) -> None:
        with self._lock:
            self.done = done
            if total is not None:
                self.total = total

    def counter(self, name: str, value) -> None:
        with self._lock:
            self.counters[name] = value

    def log(self, msg: str, level: str = "info", **data) -> None:
        with self._lock:
            self.seq += 1
            entry = {"seq": self.seq, "t": _utc(time.time()), "level": level, "msg": str(msg),
                     "phase": self.phase_name}
            for k, v in data.items():
                entry.setdefault(k, v)
            self._ring.append(entry)
            line = dict(entry)
            line.pop("seq")
            line["microvm_id"] = self.microvm_id
        try:
            sys.stdout.write(json.dumps(line, default=str) + "\n")
            sys.stdout.flush()
        except Exception:
            pass  # stdout closed or redirected away: telemetry must never break the job

    def since(self, seq: int) -> list[dict]:
        with self._lock:
            return [dict(e) for e in self._ring if e["seq"] > seq]

    def snapshot(self, since: int | None = None) -> dict:
        with self._lock:
            if since is None:
                tail = list(self._ring)[-self.TAIL:]
            else:
                tail = [e for e in self._ring if e["seq"] > since]
            lease = self.lease
            return {
                "phase": self.phase_name,
                "started": _utc(self.started),
                "elapsed_s": round(time.time() - self.started, 3),
                "progress": {"done": self.done, "total": self.total},
                "counters": dict(self.counters),
                "log_tail": [dict(e) for e in tail],
                "lease": lease.status() if lease else None,
                "microvm_id": self.microvm_id,
                "seq": self.seq,
            }


# -- lease runtime -----------------------------------------------------------------
class LeaseError(Exception):
    """Raise from an on_lease handler to fail the lease with a typed, structured error."""

    def __init__(self, error_type: str, message: str = "", retryable: bool = False, data: dict | None = None):
        super().__init__(message or error_type)
        self.error_type, self.message, self.retryable, self.data = error_type, message, retryable, data


class LeaseLost(LeaseError):
    """The orchestrator stopped waiting (token closed); the result has nowhere to go."""

    def __init__(self, message: str = "lease lost: the orchestrator stopped waiting"):
        super().__init__("LeaseLost", message, retryable=False)


class _TokenClosed(Exception):
    """Internal: a completer found the lease token closed on the orchestrator side."""


class LeaseContext:
    def __init__(self, task: dict, lease: dict, microvm_id: str | None, job: Job):
        self.task, self.lease, self.microvm_id, self.job = task, lease, microvm_id, job
        self.heartbeats = 0
        self.lost = False
        self.done = False
        self.error: dict | None = None
        self.started = time.time()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._completer = None

    def check(self) -> None:
        if self.lost:
            raise LeaseLost()

    def status(self) -> dict:
        return {"kind": self.lease.get("kind"), "id": self.lease.get("id"), "heartbeats": self.heartbeats,
                "lost": self.lost, "done": self.done, "error": dict(self.error) if self.error else None}

    def base(self) -> dict:
        """Fields every message carries (heartbeats send just these)."""
        return {"microvm_id": self.microvm_id, "lease_id": self.lease.get("id"),
                "elapsed_s": round(time.time() - self.started, 3)}

    def completion(self, result=None, error: dict | None = None) -> dict:
        """The payload every completer delivers (spec section 1), with the 240 KB result guard."""
        p = self.base()
        if error is not None:
            p["error"] = error
            return p
        p["result"] = result
        if len(json.dumps(p, default=str)) > RESULT_LIMIT:
            p["result"] = {"truncated": True, "summary": json.dumps(result, default=str)[:SUMMARY_LIMIT]}
        return p

    def _mark_lost(self) -> None:
        self.lost = True
        self._stop.set()
        self.job.log("lease lost: orchestrator token closed", level="warning")


def _error(error_type: str, message: str, retryable: bool, data: dict | None) -> dict:
    return {"error_type": error_type, "message": message, "retryable": bool(retryable), "data": data or {}}


def _decode_lease(raw) -> tuple[dict, dict] | None:
    """None when raw is not a lease payload; ValueError when it is one but malformed."""
    if not raw:
        return None
    try:
        obj = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return None
    if not isinstance(obj, dict) or "lease" not in obj:
        return None
    lease, task = obj["lease"], obj.get("task", {})
    if not isinstance(lease, dict) or not isinstance(task, dict):
        raise ValueError("lease and task must be JSON objects")
    kind = lease.get("kind")
    if kind not in LEASE_KINDS:
        raise ValueError(f"lease.kind must be one of {LEASE_KINDS}, got {kind!r}")
    if kind != "none" and not lease.get("token"):
        raise ValueError(f"lease.token is required for kind {kind!r}")
    if kind in ("http", "sqs", "eventbridge") and not lease.get("target"):
        raise ValueError(f"lease.target is required for kind {kind!r}")
    lease.setdefault("region", "us-east-1")
    hb = lease.get("heartbeat_s", 30)
    if isinstance(hb, bool) or not isinstance(hb, (int, float)) or hb <= 0:
        raise ValueError(f"lease.heartbeat_s must be a positive number of seconds, got {hb!r}")
    lease["heartbeat_s"] = hb
    return lease, task


def _default_client_factory(service: str, region: str):
    import boto3

    return boto3.client(service, region_name=region)


# -- completers: one per lease kind; each raises _TokenClosed when the orchestrator is gone ----
class _Completer:
    heartbeats = True  # False for kinds with nothing to heartbeat against

    def __init__(self, lease: dict, factory):
        self.lease, self.token, self.target = lease, lease.get("token", ""), lease.get("target")

    def send(self, status: str, payload: dict) -> None:  # status: success | failure | heartbeat
        raise NotImplementedError

    def success(self, payload: dict) -> None:
        self.send("success", payload)

    def failure(self, payload: dict) -> None:
        self.send("failure", payload)

    def heartbeat(self, payload: dict) -> None:
        self.send("heartbeat", payload)


class _NoneCompleter(_Completer):
    heartbeats = False

    def send(self, status: str, payload: dict) -> None:
        pass


def _closed_if(call, names: tuple, **kw):
    """Call a boto3 method; translate the named exception classes into _TokenClosed."""
    try:
        return call(**kw)
    except Exception as e:
        if type(e).__name__ in names:
            raise _TokenClosed(str(e)) from e
        raise


class _SfnCompleter(_Completer):
    CLOSED = ("TaskTimedOut", "InvalidToken")

    def __init__(self, lease: dict, factory):
        super().__init__(lease, factory)
        self.client = factory("stepfunctions", lease["region"])

    def send(self, status: str, payload: dict) -> None:
        if status == "heartbeat":
            _closed_if(self.client.send_task_heartbeat, self.CLOSED, taskToken=self.token)
        elif status == "success":
            _closed_if(self.client.send_task_success, self.CLOSED, taskToken=self.token,
                       output=json.dumps(payload, default=str))
        else:
            _closed_if(self.client.send_task_failure, self.CLOSED, taskToken=self.token,
                       error=payload["error"]["error_type"][:256],
                       cause=json.dumps(payload, default=str)[:32768])


class _DurableCompleter(_Completer):
    CLOSED = ("CallbackTimeoutException", "InvalidToken")

    def __init__(self, lease: dict, factory):
        super().__init__(lease, factory)
        self.client = factory("lambda", lease["region"])

    def send(self, status: str, payload: dict) -> None:
        c = self.client
        if status == "heartbeat":
            _closed_if(c.send_durable_execution_callback_heartbeat, self.CLOSED, CallbackId=self.token)
        elif status == "success":
            _closed_if(c.send_durable_execution_callback_success, self.CLOSED, CallbackId=self.token,
                       Result=json.dumps(payload, default=str).encode())
        else:
            err = payload["error"]
            _closed_if(c.send_durable_execution_callback_failure, self.CLOSED, CallbackId=self.token,
                       Error={"ErrorType": err["error_type"], "ErrorMessage": str(err["message"])[:1024],
                              "ErrorData": json.dumps(payload, default=str)[:4096]})


class _HttpCompleter(_Completer):
    TIMEOUT = 10
    RETRIES = 2

    def send(self, status: str, payload: dict) -> None:
        import urllib.error
        import urllib.request

        body = json.dumps(dict(payload, status=status), default=str).encode()
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        if self.lease.get("id"):
            headers["X-Microvm-Lease"] = str(self.lease["id"])
        for attempt in range(self.RETRIES + 1):
            req = urllib.request.Request(self.target, data=body, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=self.TIMEOUT) as r:
                    r.read()
                return
            except urllib.error.HTTPError as e:
                if e.code in (404, 410):
                    raise _TokenClosed(f"target answered {e.code}") from e
                raise
            except (urllib.error.URLError, OSError):
                if attempt == self.RETRIES:
                    raise
                time.sleep(1)


class _SqsCompleter(_Completer):
    def __init__(self, lease: dict, factory):
        super().__init__(lease, factory)
        self.client = factory("sqs", lease["region"])

    def send(self, status: str, payload: dict) -> None:
        body = json.dumps(dict(payload, token=self.token, status=status), default=str)
        self.client.send_message(QueueUrl=self.target, MessageBody=body)


class _EventBridgeCompleter(_Completer):
    def __init__(self, lease: dict, factory):
        super().__init__(lease, factory)
        self.client = factory("events", lease["region"])

    def send(self, status: str, payload: dict) -> None:
        detail = json.dumps(dict(payload, token=self.token, status=status), default=str)
        self.client.put_events(Entries=[{"Source": "microvm.lease", "DetailType": f"microvm.lease.{status}",
                                         "EventBusName": self.target, "Detail": detail}])


COMPLETERS = {"none": _NoneCompleter, "sfn": _SfnCompleter, "durable": _DurableCompleter,
              "http": _HttpCompleter, "sqs": _SqsCompleter, "eventbridge": _EventBridgeCompleter}


class HookApp:
    #: (service, region) -> boto3-like client; tests replace this with a fake factory.
    lease_client_factory = staticmethod(_default_client_factory)

    def __init__(self):
        self._hooks: dict[str, object] = {}
        self._routes: dict[tuple[str, str], object] = {}
        self.ready = False
        self.microvm_id: str | None = os.environ.get("AWS_MICROVM_ID")
        self.run_payload: str | None = None
        self.job = Job(self.microvm_id)
        self.lease: LeaseContext | None = None

        @self.route("GET", "/healthz")
        def _healthz(body, headers):
            return 200, {"ok": True, "microvmId": self.microvm_id}

        @self.route("GET", "/status")
        def _status(body, headers):
            since = (body or {}).get("since")
            return 200, self.job.snapshot(int(since) if since not in (None, "") else None)

    # -- decorators --------------------------------------------------------------
    def on_ready(self, fn):
        """Return truthy when warm enough to snapshot (else 503 -> retried)."""
        self._hooks["ready"] = fn
        return fn

    def on_validate(self, fn):
        """Exercise real code paths here: Lambda records the snapshot regions the
        validate run touches and prefetches them, cutting launch latency.
        Return False (or raise) to fail the build."""
        self._hooks["validate"] = fn
        return fn

    def on_run(self, fn):
        self._hooks["run"] = fn
        return fn

    def on_resume(self, fn):
        self._hooks["resume"] = fn
        return fn

    def on_suspend(self, fn):
        self._hooks["suspend"] = fn
        return fn

    def on_terminate(self, fn):
        self._hooks["terminate"] = fn
        return fn

    def on_lease(self, fn):
        """fn(task: dict, lease: LeaseContext) -> JSON-serializable result. Runs in a daemon
        thread after /run when runHookPayload carries a lease; the return value (or a raised
        LeaseError) is delivered to the orchestrator by the lease kind's completer."""
        self._hooks["lease"] = fn
        return fn

    def route(self, method: str, path: str):
        def deco(fn):
            self._routes[(method.upper(), path)] = fn
            return fn
        return deco

    # -- hook dispatch -----------------------------------------------------------
    def _dispatch_hook(self, name: str, ctx: dict) -> int:
        if name == "ready":
            fn = self._hooks.get("ready")
            ok = fn(ctx) if fn else True
            self.ready = bool(ok) if fn else True
            return 200 if self.ready else 503
        if name == "validate":
            fn = self._hooks.get("validate")
            if fn is None:
                return 200
            ok = fn(ctx)
            # None (no return statement) means "ran fine"; an explicit False
            # rejects the image version so a bad build never becomes ACTIVE.
            return 200 if ok is None or ok else 500
        if name == "run":
            self.microvm_id = ctx.get("microvmId") or os.environ.get("AWS_MICROVM_ID")
            self.run_payload = ctx.get("runHookPayload")
            self.job.microvm_id = self.microvm_id
            self.job.reset()
            # Restore entropy uniqueness for every clone before user code runs.
            random.seed()
        if name == "terminate":
            self._terminate_lease()
        fn = self._hooks.get(name)
        if fn:
            fn(ctx)
        if name == "run":
            self._maybe_start_lease(self.run_payload)
        return 200

    # -- lease worker ------------------------------------------------------------
    def _maybe_start_lease(self, raw) -> None:
        try:
            decoded = _decode_lease(raw)
        except ValueError as e:
            self.job.log(f"ignoring malformed lease payload: {e}", level="warning")
            return
        if decoded is None:
            return
        lease, task = decoded
        handler = self._hooks.get("lease")
        if handler is None:
            self.job.log("lease payload received but no on_lease handler is registered", level="warning",
                         kind=lease["kind"])
            return
        ctx = LeaseContext(task, lease, self.microvm_id, self.job)
        try:
            completer = COMPLETERS[lease["kind"]](lease, type(self).lease_client_factory)
        except Exception as e:
            ctx.done, ctx.error = True, _error("CompleterInit", f"{type(e).__name__}: {e}", False, None)
            self.job.lease = self.lease = ctx
            self.job.log(f"cannot build {lease['kind']} completer: {e}", level="error")
            return
        ctx._completer = completer
        self.job.lease = self.lease = ctx
        self.job.log("lease accepted", kind=lease["kind"], lease_id=lease.get("id"))
        threading.Thread(target=self._run_lease, args=(handler, ctx, completer), daemon=True).start()

    def _run_lease(self, handler, ctx: LeaseContext, completer) -> None:
        interval = float(ctx.lease.get("heartbeat_s") or 0)
        if completer.heartbeats and interval > 0:
            threading.Thread(target=self._heartbeat_loop, args=(ctx, completer, interval),
                             daemon=True).start()
        try:
            result = handler(ctx.task, ctx)
            self._deliver(ctx, completer, "success", ctx.completion(result=result))
        except LeaseError as e:
            err = _error(e.error_type, e.message, e.retryable, e.data)
            self._deliver(ctx, completer, "failure", ctx.completion(error=err))
        except Exception as e:
            err = _error("Unexpected", f"{type(e).__name__}: {e}", False,
                         {"trace": traceback.format_exc(limit=5)})
            self._deliver(ctx, completer, "failure", ctx.completion(error=err))

    def _heartbeat_loop(self, ctx: LeaseContext, completer, interval: float) -> None:
        while not ctx._stop.wait(interval):
            try:
                with ctx._lock:  # serialized with _deliver: no heartbeat lands after the completion
                    if ctx.done:
                        return
                    completer.heartbeat(ctx.base())
                    ctx.heartbeats += 1
            except _TokenClosed:
                ctx._mark_lost()
                return
            except Exception as e:
                self.job.log(f"heartbeat failed: {e}", level="warning")

    def _deliver(self, ctx: LeaseContext, completer, outcome: str, payload: dict) -> None:
        with ctx._lock:
            if ctx.done:
                return
            ctx.done = True
            ctx.error = payload.get("error")
        ctx._stop.set()
        if ctx.lost:
            self.job.log(f"lease {outcome} not delivered: orchestrator stopped waiting", level="warning")
            return
        try:
            completer.send(outcome, payload)
            self.job.log(f"lease {outcome} delivered", kind=ctx.lease.get("kind"))
        except _TokenClosed as e:
            ctx.lost = True
            self.job.log(f"lease {outcome} not delivered: {e}", level="warning")
        except Exception as e:
            self.job.log(f"lease {outcome} delivery failed: {type(e).__name__}: {e}", level="error")

    def _terminate_lease(self) -> None:
        ctx = self.lease
        if ctx is None or ctx.done:
            return
        try:
            err = _error("Terminated", "microVM terminated before the task finished", True, None)
            self._deliver(ctx, ctx._completer, "failure", ctx.completion(error=err))
        except Exception:
            pass

    # -- server ------------------------------------------------------------------
    def serve(self, port: int = 8080, background: bool = False):
        app = self

        class Server(ThreadingHTTPServer):
            daemon_threads = True

            def __init__(self, *a):
                super().__init__(*a)
                self.stopping = threading.Event()

            def shutdown(self):
                self.stopping.set()
                super().shutdown()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _read_json(self):
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b""
                try:
                    return json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    return {"_raw": raw.decode("utf-8", "replace")}

            def _query(self) -> dict:
                q = self.path.split("?", 1)[1] if "?" in self.path else ""
                return dict(kv.split("=", 1) for kv in q.split("&") if "=" in kv)

            def _send(self, status: int, payload):
                body = (payload if isinstance(payload, (bytes, bytearray))
                        else json.dumps(payload, default=str).encode())
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass  # the client left; there is nobody to answer and nothing to log

            def _client_gone(self) -> bool:
                try:
                    r, _, _ = select.select([self.connection], [], [], 0)
                    return bool(r) and self.connection.recv(1, socket.MSG_PEEK) == b""
                except OSError:
                    return True

            def _events(self):
                """SSE: `data:` per new log line, `event: snapshot` every 5 s, until the client leaves."""
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "close")
                    self.end_headers()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return
                seq = app.job.seq
                deadline, next_snapshot = time.time() + 3600, 0.0
                try:
                    while time.time() < deadline and not self.server.stopping.is_set():
                        for entry in app.job.since(seq):
                            seq = entry["seq"]
                            self.wfile.write(f"data: {json.dumps(entry, default=str)}\n\n".encode())
                        if time.time() >= next_snapshot:
                            snap = json.dumps(app.job.snapshot(), default=str)
                            self.wfile.write(f"event: snapshot\ndata: {snap}\n\n".encode())
                            next_snapshot = time.time() + 5
                        self.wfile.flush()
                        if self._client_gone():
                            return
                        time.sleep(0.2)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return

            def _handle(self, method: str):
                path = self.path.split("?", 1)[0]
                if method == "GET" and path == "/events":
                    return self._events()
                try:
                    if path.startswith(HOOK_BASE + "/"):
                        hook = path[len(HOOK_BASE) + 1:]
                        status = app._dispatch_hook(hook, self._read_json())
                        return self._send(status, {"hook": hook, "status": status})
                    fn = app._routes.get((method, path))
                    if fn is None:
                        return self._send(404, {"error": f"no route {method} {path}"})
                    body = self._read_json()
                    if method == "GET" and not body:
                        body = self._query()
                    status, payload = fn(body, dict(self.headers))
                    return self._send(status, payload)
                except Exception:
                    return self._send(500, {"error": traceback.format_exc(limit=5)})

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

            def log_message(self, *a):  # keep container logs for the app, not access noise
                pass

        server = Server(("0.0.0.0", port), Handler)
        if background:
            threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True).start()
            return server
        server.serve_forever()
