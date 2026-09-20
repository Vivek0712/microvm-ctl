"""Lease runtime and job telemetry inside the hook server (loopback only, no AWS)."""

import http.client
import io
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from microvm.hooks.server import HookApp, LeaseContext, LeaseError, LeaseLost

RUN = "/aws/lambda-microvms/runtime/v1/run"
TERMINATE = "/aws/lambda-microvms/runtime/v1/terminate"


# -- helpers -----------------------------------------------------------------------
class Collector:
    """Loopback HTTP target for kind `http`: records every POST, answers `status`."""

    def __init__(self):
        self.requests = []
        self.status = 200
        self._cv = threading.Condition()
        collector = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                with collector._cv:
                    collector.requests.append((self.path, dict(self.headers), json.loads(body)))
                    collector._cv.notify_all()
                self.send_response(collector.status)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05},
                         daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/callback"

    def wait_for(self, status, timeout=5.0):
        deadline = time.time() + timeout
        with self._cv:
            while True:
                hits = [r for r in self.requests if r[2].get("status") == status]
                if hits:
                    return hits[0]
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise AssertionError(f"no {status!r} message within {timeout}s: {self.requests}")
                self._cv.wait(remaining)

    def count(self, status):
        with self._cv:
            return sum(1 for r in self.requests if r[2].get("status") == status)

    def close(self):
        self.server.shutdown()


class FakeClient:
    """Records boto3-style calls; raises the exception class named in `raise_on`."""

    def __init__(self):
        self.calls = []
        self.raise_on = {}

    def __getattr__(self, name):
        def call(**kw):
            self.calls.append((name, kw))
            exc = self.raise_on.get(name)
            if exc:
                raise exc
            return {}
        return call


def _post(port, path, payload):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.status, json.loads(r.read())


def _get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
        return json.loads(r.read())


def _run(port, lease, task=None, microvm_id="mvm-test"):
    payload = json.dumps({"lease": lease, "task": task or {}})
    t0 = time.time()
    status, body = _post(port, RUN, {"microvmId": microvm_id, "runHookPayload": payload})
    return status, time.time() - t0


def _wait(pred, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return False


@pytest.fixture()
def collector():
    c = Collector()
    yield c
    c.close()


@pytest.fixture()
def app():
    a = HookApp()
    server = a.serve(port=0, background=True)
    a.port = server.server_address[1]
    yield a
    server.shutdown()


def _http_lease(collector, **kw):
    lease = {"kind": "http", "token": "secret-tok", "target": collector.url, "heartbeat_s": 0.05,
             "id": "job-1"}
    lease.update(kw)
    return lease


# -- http kind end to end ------------------------------------------------------------
def test_http_lease_runs_in_thread_heartbeats_and_succeeds(app, collector):
    seen = {}

    @app.on_lease
    def work(task, lease):
        seen["thread"] = threading.current_thread().name
        seen["ctx"] = lease
        app.job.phase("crunch")
        time.sleep(0.25)
        return {"answer": task["n"] * 2}

    status, took = _run(app.port, _http_lease(collector), {"n": 21})
    assert status == 200 and took < 0.1
    path, headers, body = collector.wait_for("success")
    assert seen["thread"] != "MainThread"
    assert path == "/callback"
    assert headers["Authorization"] == "Bearer secret-tok"
    assert headers["Content-Type"] == "application/json"
    assert headers["X-Microvm-Lease"] == "job-1"
    assert body["microvm_id"] == "mvm-test" and body["lease_id"] == "job-1"
    assert body["result"] == {"answer": 42} and body["elapsed_s"] >= 0.2
    assert "error" not in body
    assert collector.count("heartbeat") >= 1
    hb = collector.wait_for("heartbeat")[2]
    assert set(hb) == {"microvm_id", "lease_id", "elapsed_s", "status"}
    assert _wait(lambda: app.job.snapshot()["lease"]["done"])
    ctx = seen["ctx"]
    assert isinstance(ctx, LeaseContext) and ctx.task == {"n": 21} and ctx.microvm_id == "mvm-test"
    assert ctx.job is app.job and ctx.lease["kind"] == "http" and ctx.heartbeats >= 1
    snap = app.job.snapshot()
    assert snap["lease"] == {"kind": "http", "id": "job-1", "heartbeats": ctx.heartbeats, "lost": False,
                             "done": True, "error": None}
    assert collector.count("heartbeat") == ctx.heartbeats  # heartbeats stop once done


@pytest.mark.parametrize("exc,error_type,retryable,check", [
    (LeaseError("BadInput", "missing field", retryable=True, data={"field": "x"}), "BadInput", True,
     lambda err: err["message"] == "missing field" and err["data"] == {"field": "x"}),
    (ValueError("boom"), "Unexpected", False,
     lambda err: err["message"] == "ValueError: boom" and "ValueError: boom" in err["data"]["trace"]),
])
def test_handler_errors_become_failure_payloads(app, collector, exc, error_type, retryable, check):
    @app.on_lease
    def work(task, lease):
        raise exc

    _run(app.port, _http_lease(collector))
    body = collector.wait_for("failure")[2]
    assert body["error"]["error_type"] == error_type
    assert body["error"]["retryable"] is retryable
    assert check(body["error"])
    assert body["microvm_id"] == "mvm-test" and "result" not in body
    assert _wait(lambda: app.job.snapshot()["lease"]["done"])
    assert app.job.snapshot()["lease"]["error"]["error_type"] == error_type


def test_target_410_marks_lease_lost_and_check_raises(app, collector):
    collector.status = 410
    outcome = {}

    @app.on_lease
    def work(task, lease):
        deadline = time.time() + 3
        while time.time() < deadline:
            try:
                lease.check()
            except LeaseLost as e:
                outcome["raised"] = e
                raise
            time.sleep(0.01)
        return "should not get here"

    _run(app.port, _http_lease(collector))
    assert _wait(lambda: app.job.snapshot()["lease"]["done"])
    snap = app.job.snapshot()["lease"]
    assert snap["lost"] is True and snap["done"] is True
    assert snap["error"]["error_type"] == "LeaseLost"
    assert isinstance(outcome["raised"], LeaseError)
    assert collector.count("failure") == 0  # nothing to deliver to once the token is closed
    assert app.lease.heartbeats == 0


def test_terminate_hook_fails_an_active_lease(app, collector):
    release = threading.Event()

    @app.on_lease
    def work(task, lease):
        release.wait(5)
        return {"late": True}

    _run(app.port, _http_lease(collector, heartbeat_s=30))
    assert _wait(lambda: app.lease is not None)
    status, _ = _post(app.port, TERMINATE, {})
    assert status == 200
    body = collector.wait_for("failure")[2]
    assert body["error"]["error_type"] == "Terminated" and body["error"]["retryable"] is True
    assert body["error"]["message"] == "microVM terminated before the task finished"
    release.set()
    time.sleep(0.1)
    assert collector.count("success") == 0  # the late result is dropped: the lease is done
    assert _post(app.port, TERMINATE, {})[0] == 200  # idempotent once done


def test_kind_none_runs_and_status_shows_done(app):
    @app.on_lease
    def work(task, lease):
        app.job.phase("only")
        app.job.progress(1, 1)
        return {"ok": True}

    status, _ = _run(app.port, {"kind": "none"}, {"x": 1})
    assert status == 200
    assert _wait(lambda: _get(app.port, "/status")["lease"]["done"])
    snap = _get(app.port, "/status")
    assert snap["lease"] == {"kind": "none", "id": None, "heartbeats": 0, "lost": False, "done": True,
                             "error": None}
    assert snap["phase"] == "only" and snap["progress"] == {"done": 1, "total": 1}
    assert snap["microvm_id"] == "mvm-test"


def test_run_without_handler_or_with_bad_payload_still_acks(app):
    status, _ = _run(app.port, {"kind": "none"})
    assert status == 200
    msgs = [e["msg"] for e in app.job.snapshot()["log_tail"]]
    assert any("no on_lease handler" in m for m in msgs)
    assert app.lease is None

    @app.on_lease
    def work(task, lease):
        return 1

    status, _ = _run(app.port, {"kind": "sfn"})  # token missing
    assert status == 200 and app.lease is None
    msgs = [e["msg"] for e in app.job.snapshot()["log_tail"]]
    assert any("malformed lease payload" in m and "lease.token is required" in m for m in msgs)
    status, _ = _post(app.port, RUN, {"microvmId": "m", "runHookPayload": '{"tenant": "t1"}'})
    assert status == 200 and app.lease is None  # not a lease payload: existing behaviour


# -- job telemetry --------------------------------------------------------------------
def test_status_since_and_events_stream(app):
    app.job.phase("one")
    app.job.log("hello", level="debug", k=1)
    app.job.counter("files", 3)
    app.job.progress(2, 10)
    snap = _get(app.port, "/status")
    assert snap["phase"] == "one" and snap["counters"] == {"files": 3}
    assert snap["progress"] == {"done": 2, "total": 10} and snap["lease"] is None
    assert [e["msg"] for e in snap["log_tail"]] == ["phase: one", "hello"]
    assert snap["log_tail"][1]["k"] == 1 and snap["log_tail"][1]["level"] == "debug"
    assert snap["log_tail"][1]["phase"] == "one" and snap["seq"] == 2
    assert snap["started"].endswith("Z") and snap["elapsed_s"] >= 0
    since = _get(app.port, "/status?since=1")
    assert [e["msg"] for e in since["log_tail"]] == ["hello"] and since["seq"] == 2
    assert _get(app.port, "/status?since=2")["log_tail"] == []

    conn = http.client.HTTPConnection("127.0.0.1", app.port, timeout=5)
    conn.request("GET", "/events")
    resp = conn.getresponse()
    assert resp.status == 200 and resp.getheader("Content-Type") == "text/event-stream"
    app.job.log("streamed")
    lines = []
    while len([ln for ln in lines if ln.startswith(b"data:")]) < 2:
        line = resp.readline()
        assert line, "stream ended early"
        lines.append(line)
    assert lines[0] == b"event: snapshot\n"
    assert json.loads(lines[1][len(b"data: "):])["phase"] == "one"
    data_lines = [ln for ln in lines if ln.startswith(b"data:")]
    assert json.loads(data_lines[1][len(b"data: "):])["msg"] == "streamed"
    conn.close()
    time.sleep(0.5)  # the server thread notices the closed socket and returns
    assert sum(1 for t in threading.enumerate() if "events" in repr(t)) == 0


def test_log_prints_one_json_line_and_survives_closed_stdout(app, capsys, monkeypatch):
    app.job.microvm_id = "mvm-log"
    app.job.phase("p")
    app.job.log("multi\nline", n=2, phase="ignored")
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 2
    entry = json.loads(out[1])
    assert entry["msg"] == "multi\nline" and entry["level"] == "info" and entry["n"] == 2
    assert entry["phase"] == "p" and entry["microvm_id"] == "mvm-log" and entry["t"].endswith("Z")
    closed = io.StringIO()
    closed.close()
    monkeypatch.setattr(sys, "stdout", closed)
    app.job.log("still fine")  # must not raise
    assert app.job.snapshot()["log_tail"][-1]["msg"] == "still fine"


def test_ring_buffer_and_tail_limits(app):
    for i in range(250):
        app.job.log(f"m{i}")
    snap = app.job.snapshot()
    assert len(snap["log_tail"]) == 50 and snap["log_tail"][-1]["msg"] == "m249"
    assert len(app.job.since(0)) == 200 and app.job.since(0)[0]["msg"] == "m50"
    assert snap["seq"] == 250
    snap["counters"]["x"] = 1  # copies: mutating a snapshot never leaks back
    assert app.job.snapshot()["counters"] == {}


def test_completion_payload_shape_and_result_guard():
    ctx = LeaseContext({"a": 1}, {"kind": "none", "id": "L"}, "mvm-x", HookApp().job)
    ok = ctx.completion(result={"big": "x" * (250 * 1024)})
    assert set(ok) == {"microvm_id", "lease_id", "elapsed_s", "result"}
    assert ok["result"]["truncated"] is True and len(ok["result"]["summary"]) == 4096
    assert ok["result"]["summary"].startswith('{"big": "xxx')
    small = ctx.completion(result={"n": 1})
    assert small["result"] == {"n": 1} and small["lease_id"] == "L" and small["microvm_id"] == "mvm-x"
    err = ctx.completion(error={"error_type": "E", "message": "m", "retryable": False, "data": {}})
    assert set(err) == {"microvm_id", "lease_id", "elapsed_s", "error"}


# -- boto3-backed completers with a fake client factory ------------------------------------
@pytest.fixture()
def fake(monkeypatch):
    client = FakeClient()
    made = []

    def factory(service, region):
        made.append((service, region))
        return client

    monkeypatch.setattr(HookApp, "lease_client_factory", factory)
    client.made = made
    return client


def _lease_run(app, kind, outcome="ok", **extra):
    @app.on_lease
    def work(task, lease):
        # hold the lease until the heartbeat thread has fired at least once (0.05 s interval),
        # so the assertions on heartbeats do not depend on scheduler timing
        deadline = time.time() + 3
        while kind != "none" and lease.heartbeats < 1 and time.time() < deadline:
            time.sleep(0.01)
        if outcome == "ok":
            return {"v": 1}
        raise LeaseError("Bad", "no good", retryable=True, data={"k": 2})

    lease = {"kind": kind, "token": "tok-1", "region": "us-west-2", "heartbeat_s": 0.05, "id": "L1"}
    lease.update(extra)
    _run(app.port, lease)
    assert _wait(lambda: app.job.snapshot()["lease"]["done"])


def test_sfn_completer_calls(app, fake):
    _lease_run(app, "sfn")
    assert fake.made == [("stepfunctions", "us-west-2")]
    names = [n for n, _ in fake.calls]
    assert names[-1] == "send_task_success" and "send_task_heartbeat" in names
    name, kw = fake.calls[-1]
    assert set(kw) == {"taskToken", "output"} and kw["taskToken"] == "tok-1"
    out = json.loads(kw["output"])
    assert out["result"] == {"v": 1} and out["microvm_id"] == "mvm-test" and out["lease_id"] == "L1"
    assert [kw for n, kw in fake.calls if n == "send_task_heartbeat"][0] == {"taskToken": "tok-1"}


def test_sfn_failure_and_closed_token(app, fake):
    _lease_run(app, "sfn", outcome="fail")
    name, kw = fake.calls[-1]
    assert name == "send_task_failure" and set(kw) == {"taskToken", "error", "cause"}
    assert kw["error"] == "Bad"
    cause = json.loads(kw["cause"])
    assert cause["error"] == {"error_type": "Bad", "message": "no good", "retryable": True, "data": {"k": 2}}

    class TaskTimedOut(Exception):
        pass

    app2 = HookApp()
    server = app2.serve(port=0, background=True)
    try:
        app2.port = server.server_address[1]
        fake.calls.clear()
        fake.raise_on["send_task_heartbeat"] = TaskTimedOut("closed")

        @app2.on_lease
        def work(task, lease):
            assert _wait(lambda: lease.lost, timeout=3)
            lease.check()

        _run(app2.port, {"kind": "sfn", "token": "tok-2", "heartbeat_s": 0.02})
        assert _wait(lambda: app2.job.snapshot()["lease"]["done"])
        snap = app2.job.snapshot()["lease"]
        assert snap["lost"] and snap["error"]["error_type"] == "LeaseLost"
        assert [n for n, _ in fake.calls] == ["send_task_heartbeat"]  # nothing sent after the token closed
    finally:
        server.shutdown()


def test_durable_completer_calls(app, fake):
    _lease_run(app, "durable")
    assert fake.made == [("lambda", "us-west-2")]
    name, kw = fake.calls[-1]
    assert name == "send_durable_execution_callback_success"
    assert set(kw) == {"CallbackId", "Result"} and kw["CallbackId"] == "tok-1"
    assert isinstance(kw["Result"], bytes) and json.loads(kw["Result"])["result"] == {"v": 1}
    hb = [kw for n, kw in fake.calls if n == "send_durable_execution_callback_heartbeat"]
    assert hb and hb[0] == {"CallbackId": "tok-1"}


def test_durable_failure_and_closed_token(app, fake):
    class CallbackTimeoutException(Exception):
        pass

    fake.raise_on["send_durable_execution_callback_failure"] = CallbackTimeoutException("gone")
    _lease_run(app, "durable", outcome="fail", heartbeat_s=30)
    name, kw = fake.calls[-1]
    assert name == "send_durable_execution_callback_failure" and set(kw) == {"CallbackId", "Error"}
    assert set(kw["Error"]) == {"ErrorType", "ErrorMessage", "ErrorData"}
    assert kw["Error"]["ErrorType"] == "Bad" and kw["Error"]["ErrorMessage"] == "no good"
    assert json.loads(kw["Error"]["ErrorData"])["error"]["data"] == {"k": 2}
    assert app.job.snapshot()["lease"]["lost"] is True


def test_sqs_completer_calls(app, fake):
    _lease_run(app, "sqs", target="https://sqs.us-west-2.amazonaws.com/123/q")
    assert fake.made == [("sqs", "us-west-2")]
    assert {n for n, _ in fake.calls} == {"send_message"}
    bodies = [json.loads(kw["MessageBody"]) for _, kw in fake.calls]
    assert all(set(kw) == {"QueueUrl", "MessageBody"} for _, kw in fake.calls)
    assert all(kw["QueueUrl"] == "https://sqs.us-west-2.amazonaws.com/123/q" for _, kw in fake.calls)
    assert bodies[0]["status"] == "heartbeat" and bodies[0]["token"] == "tok-1"
    assert bodies[-1]["status"] == "success" and bodies[-1]["result"] == {"v": 1}
    assert bodies[-1]["token"] == "tok-1" and bodies[-1]["lease_id"] == "L1"


def test_eventbridge_completer_calls(app, fake):
    _lease_run(app, "eventbridge", target="my-bus", outcome="fail")
    assert fake.made == [("events", "us-west-2")]
    assert {n for n, _ in fake.calls} == {"put_events"}
    entries = [kw["Entries"] for _, kw in fake.calls]
    for e in entries:
        assert len(e) == 1 and set(e[0]) == {"Source", "DetailType", "EventBusName", "Detail"}
    assert entries[0][0]["Source"] == "microvm.lease" and entries[0][0]["EventBusName"] == "my-bus"
    assert entries[0][0]["DetailType"] == "microvm.lease.heartbeat"
    last = entries[-1][0]
    assert last["DetailType"] == "microvm.lease.failure"
    detail = json.loads(last["Detail"])
    assert detail["status"] == "failure" and detail["error"]["error_type"] == "Bad"
    assert detail["token"] == "tok-1"


def test_http_completer_needs_no_client_factory(app, collector, monkeypatch):
    def explode(service, region):
        raise AssertionError("http must not build a boto3 client")

    monkeypatch.setattr(HookApp, "lease_client_factory", explode)

    @app.on_lease
    def work(task, lease):
        return "done"

    _run(app.port, _http_lease(collector, heartbeat_s=30))
    assert collector.wait_for("success")[2]["result"] == "done"


def test_completer_init_failure_is_recorded_not_raised(app, monkeypatch):
    def broken(service, region):
        raise RuntimeError("no creds")

    monkeypatch.setattr(HookApp, "lease_client_factory", broken)

    @app.on_lease
    def work(task, lease):
        return 1

    status, _ = _run(app.port, {"kind": "sqs", "token": "t", "target": "https://q"})
    assert status == 200
    snap = app.job.snapshot()["lease"]
    assert snap["done"] and snap["error"]["error_type"] == "CompleterInit"


def test_existing_hook_api_unchanged(app):
    for name in ("on_ready", "on_validate", "on_run", "on_resume", "on_suspend", "on_terminate", "on_lease",
                 "route", "serve", "job", "lease", "lease_client_factory"):
        assert hasattr(app, name)
    with pytest.raises(urllib.error.HTTPError) as e:  # /events is GET-only
        _post(app.port, "/events", {})
    assert e.value.code == 404


def test_decode_rejects_malformed_heartbeat():
    import pytest as _pytest

    from microvm.hooks.server import _decode_lease
    with _pytest.raises(ValueError):
        _decode_lease('{"lease": {"kind": "none", "heartbeat_s": "soon"}, "task": {}}')
    lease, _ = _decode_lease('{"lease": {"kind": "none"}, "task": {}}')
    assert lease["heartbeat_s"] == 30
