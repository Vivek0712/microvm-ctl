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

    app.serve(port=8080)
"""

from __future__ import annotations

import json
import os
import random
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOOK_BASE = "/aws/lambda-microvms/runtime/v1"


class HookApp:
    def __init__(self):
        self._hooks: dict[str, object] = {}
        self._routes: dict[tuple[str, str], object] = {}
        self.ready = False
        self.microvm_id: str | None = None
        self.run_payload: str | None = None

        @self.route("GET", "/healthz")
        def _healthz(body, headers):
            return 200, {"ok": True, "microvmId": self.microvm_id}

    # -- decorators --------------------------------------------------------------
    def on_ready(self, fn):
        """Return truthy when warm enough to snapshot (else 503 -> retried)."""
        self._hooks["ready"] = fn
        return fn

    def on_validate(self, fn):
        """Exercise real code paths here: Lambda records the snapshot regions the
        validate run touches and prefetches them, cutting launch latency."""
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
        if name == "run":
            self.microvm_id = ctx.get("microvmId") or os.environ.get("AWS_MICROVM_ID")
            self.run_payload = ctx.get("runHookPayload")
            # Restore entropy uniqueness for every clone before user code runs.
            random.seed()
        fn = self._hooks.get(name)
        if fn:
            fn(ctx)
        return 200

    # -- server ------------------------------------------------------------------
    def serve(self, port: int = 8080, background: bool = False):
        app = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _read_json(self):
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b""
                try:
                    return json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    return {"_raw": raw.decode("utf-8", "replace")}

            def _send(self, status: int, payload):
                body = (payload if isinstance(payload, (bytes, bytearray))
                        else json.dumps(payload).encode())
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _handle(self, method: str):
                path = self.path.split("?", 1)[0]
                try:
                    if path.startswith(HOOK_BASE + "/"):
                        hook = path[len(HOOK_BASE) + 1:]
                        status = app._dispatch_hook(hook, self._read_json())
                        return self._send(status, {"hook": hook, "status": status})
                    fn = app._routes.get((method, path))
                    if fn is None:
                        return self._send(404, {"error": f"no route {method} {path}"})
                    status, payload = fn(self._read_json(), dict(self.headers))
                    return self._send(status, payload)
                except Exception:
                    return self._send(500, {"error": traceback.format_exc(limit=5)})

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

            def log_message(self, *a):  # keep container logs for the app, not access noise
                pass

        server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        if background:
            threading.Thread(target=server.serve_forever, daemon=True).start()
            return server
        server.serve_forever()
