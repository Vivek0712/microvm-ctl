import json
import urllib.request

import pytest

from microvm.hooks.server import HookApp

HOOK = "/aws/lambda-microvms/runtime/v1"


def _post(port, path):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=b"{}",
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


@pytest.mark.parametrize("ret,expected", [(None, 200), (True, 200), (False, 500)])
def test_validate_return_value_maps_to_status(ret, expected):
    app = HookApp()

    @app.on_validate
    def validate(_ctx):
        return ret

    server = app.serve(port=0, background=True)
    try:
        assert _post(server.server_address[1], f"{HOOK}/validate") == expected
    finally:
        server.shutdown()


def test_ready_false_means_503_until_warm():
    app = HookApp()
    warm = {"ok": False}

    @app.on_ready
    def ready(_ctx):
        return warm["ok"]

    server = app.serve(port=0, background=True)
    port = server.server_address[1]
    try:
        assert _post(port, f"{HOOK}/ready") == 503
        warm["ok"] = True
        assert _post(port, f"{HOOK}/ready") == 200
        assert json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz").read())["ok"]
    finally:
        server.shutdown()
