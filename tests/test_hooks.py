"""Local tests for the in-VM hook server (no AWS needed): run with `pytest`."""

import json
import threading
import urllib.request

import pytest

from microvm.hooks.server import HookApp


@pytest.fixture()
def served():
    app = HookApp()
    calls = []

    @app.on_ready
    def ready(_ctx):
        return True

    @app.on_run
    def on_run(ctx):
        calls.append(("run", ctx))

    @app.route("POST", "/echo")
    def echo(body, _headers):
        return 200, {"you_sent": body}

    server = app.serve(port=0, background=True)
    port = server.server_address[1]
    yield app, calls, port
    server.shutdown()


def _post(port, path, payload=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(payload or {}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        return r.status, json.loads(r.read())


def test_ready_hook(served):
    _app, _calls, port = served
    status, body = _post(port, "/aws/lambda-microvms/runtime/v1/ready")
    assert status == 200 and body["hook"] == "ready"


def test_run_hook_sets_identity_and_payload(served):
    app, calls, port = served
    status, _ = _post(port, "/aws/lambda-microvms/runtime/v1/run",
                      {"microvmId": "microvm-abc", "runHookPayload": "{\"tenant\": \"t1\"}"})
    assert status == 200
    assert app.microvm_id == "microvm-abc"
    assert app.run_payload == '{"tenant": "t1"}'
    assert calls and calls[0][0] == "run"


def test_unregistered_hook_returns_200(served):
    # suspend/terminate with no handler must still ack — a 500 here would
    # block the service's lifecycle transitions.
    _app, _calls, port = served
    status, _ = _post(port, "/aws/lambda-microvms/runtime/v1/suspend")
    assert status == 200


def test_user_route_and_404(served):
    _app, _calls, port = served
    status, body = _post(port, "/echo", {"a": 1})
    assert status == 200 and body["you_sent"] == {"a": 1}
    with pytest.raises(urllib.error.HTTPError) as e:
        _post(port, "/nope")
    assert e.value.code == 404


def test_healthz(served):
    _app, _calls, port = served
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz") as r:
        assert r.status == 200
