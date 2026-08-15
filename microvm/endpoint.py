"""Execution plane: authenticated HTTP into a microVM's dedicated endpoint.

Every microVM gets its own HTTPS endpoint
(`<microvm-id>.lambda-microvm.<region>.on.aws`). There is no unauthenticated
mode: each request carries a port-scoped, expiring JWE token in the
`X-aws-proxy-auth` header, minted via CreateMicrovmAuthToken. Non-default
ports are selected with `X-aws-proxy-port`.

This client mints tokens lazily, caches them until ~80% of their TTL, and
retries the two endpoint errors you must design for:
  429 — endpoint rate limit         -> jittered backoff
  502 — app down or VM auto-resuming -> patient retry (first request after
        suspend pays the resume; subsequent ones are warm)
"""

from __future__ import annotations

import random
import time

import requests

from microvm.client import microvm_client
from microvm.config import AUTH_HEADER, PORT_HEADER, PlaneConfig


class EndpointError(RuntimeError):
    pass


class EndpointClient:
    def __init__(
        self,
        config: PlaneConfig,
        microvm_id: str,
        endpoint: str | None = None,
        *,
        ports: list[int] | None = None,
        all_ports: bool = False,
        token_ttl_minutes: int = 15,
    ):
        self.cfg = config
        self.api = microvm_client(config.region, config.profile)
        self.microvm_id = microvm_id
        self.endpoint = endpoint or self._discover_endpoint()
        self.ports = ports or [8080]
        self.all_ports = all_ports
        self.ttl_minutes = token_ttl_minutes
        self._token: dict[str, str] | None = None
        self._token_expiry = 0.0
        self.http = requests.Session()

    def _discover_endpoint(self) -> str:
        vm = self.api.get_microvm(microvmIdentifier=self.microvm_id)
        return vm["endpoint"]

    # -- token lifecycle ---------------------------------------------------------
    def _auth_headers(self) -> dict[str, str]:
        if self._token is None or time.time() > self._token_expiry:
            spec = [{"allPorts": {}}] if self.all_ports else [{"port": p} for p in self.ports]
            resp = self.api.create_microvm_auth_token(
                microvmIdentifier=self.microvm_id,
                expirationInMinutes=self.ttl_minutes,
                allowedPorts=spec,
            )
            # authToken maps header names to values (e.g. "X-aws-proxy-auth").
            self._token = dict(resp["authToken"])
            self._token_expiry = time.time() + self.ttl_minutes * 60 * 0.8
        return self._token

    # -- requests ----------------------------------------------------------------
    def request(
        self,
        method: str,
        path: str,
        *,
        port: int | None = None,
        timeout: float = 60,
        max_attempts: int = 6,
        resume_patience: float = 30,
        **kwargs,
    ) -> requests.Response:
        """HTTP request to the microVM. 502s are retried for `resume_patience`
        seconds to ride out an auto-resume; 429s back off with jitter."""
        url = f"https://{self.endpoint}{path if path.startswith('/') else '/' + path}"
        headers = kwargs.pop("headers", {}) | self._auth_headers()
        if port and port != 8080:
            headers[PORT_HEADER] = str(port)
        started, delay = time.time(), 0.5
        last: requests.Response | None = None
        for attempt in range(1, max_attempts + 1):
            last = self.http.request(method, url, headers=headers, timeout=timeout, **kwargs)
            if last.status_code == 429:
                time.sleep(delay + random.uniform(0, delay))
                delay = min(delay * 2, 8)
                continue
            if last.status_code == 502 and time.time() - started < resume_patience:
                time.sleep(2)
                continue
            if last.status_code == 403:
                # token may have been revoked/expired server-side; re-mint once
                self._token = None
                if attempt < max_attempts:
                    headers = kwargs.get("headers", {}) | self._auth_headers()
                    continue
            return last
        return last  # type: ignore[return-value]

    def get(self, path: str, **kw) -> requests.Response:
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw) -> requests.Response:
        return self.request("POST", path, **kw)

    # -- conveniences ------------------------------------------------------------
    def wait_ready(self, path: str = "/healthz", timeout: float = 90) -> float:
        """Poll until the app answers 200; returns time-to-first-byte seconds."""
        started = time.time()
        deadline = started + timeout
        while time.time() < deadline:
            try:
                if self.get(path, timeout=10, max_attempts=1).status_code == 200:
                    return round(time.time() - started, 2)
            except requests.RequestException:
                pass
            time.sleep(1)
        raise EndpointError(f"{self.microvm_id} not serving {path} after {timeout}s")

    def shell_token(self, minutes: int = 15) -> dict[str, str]:
        """Token for interactive shell access (VM must run with SHELL_INGRESS)."""
        return dict(
            self.api.create_microvm_shell_auth_token(
                microvmIdentifier=self.microvm_id, expirationInMinutes=minutes
            )["authToken"]
        )
