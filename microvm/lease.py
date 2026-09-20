"""Lease contract: hand a microVM one task and let it report completion itself.

The orchestrator (Step Functions, a Lambda durable function, a queue poller,
anything) launches a VM with a `Lease` in the runHookPayload. Inside the VM the
hook runtime (`microvm.hooks.server`) decodes it, runs the `@app.on_lease`
handler, heartbeats while it works, and completes the lease through the
matching AWS API or HTTP target. This module is the control-plane half: it is
pure Python (no boto3) so it can be imported anywhere.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass

from microvm.config import MAX_DURATION_SECONDS

KINDS = ("sfn", "durable", "http", "sqs", "eventbridge", "none")
TARGET_KINDS = ("http", "sqs", "eventbridge")
PAYLOAD_LIMIT = 4096
TOKEN_LIMIT = 1024


@dataclass
class Lease:
    """What the VM needs to complete one task: where to call back and with what."""

    kind: str
    token: str = ""
    region: str = "us-east-1"
    target: str | None = None
    heartbeat_s: int = 30
    id: str | None = None

    def validate(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"lease.kind must be one of {KINDS}, got {self.kind!r}")
        if self.kind != "none" and not self.token:
            raise ValueError(f"lease.token is required for kind {self.kind!r}")
        if self.token and len(self.token) > TOKEN_LIMIT:
            raise ValueError(f"lease.token is {len(self.token)} chars, limit {TOKEN_LIMIT}")
        if self.kind in TARGET_KINDS and not self.target:
            raise ValueError(f"lease.target is required for kind {self.kind!r}")
        if self.kind == "http" and not str(self.target).startswith(("http://", "https://")):
            raise ValueError(f"lease.target must be an http(s) URL for kind 'http', got {self.target!r}")
        if not self.region or not isinstance(self.region, str):
            raise ValueError("lease.region must be a non-empty string")
        if isinstance(self.heartbeat_s, bool) or not isinstance(self.heartbeat_s, (int, float)):
            raise ValueError(f"lease.heartbeat_s must be a number of seconds, got {self.heartbeat_s!r}")
        if self.heartbeat_s <= 0:
            raise ValueError(f"lease.heartbeat_s must be positive, got {self.heartbeat_s!r}")

    def to_dict(self) -> dict:
        d = {
            "kind": self.kind, "token": self.token, "region": self.region, "target": self.target,
            "heartbeat_s": self.heartbeat_s, "id": self.id,
        }
        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def from_dict(cls, d) -> Lease:
        if not isinstance(d, dict):
            raise ValueError(f"lease must be a JSON object, got {type(d).__name__}")
        if "kind" not in d:
            raise ValueError("lease.kind is missing")
        unknown = set(d) - {"kind", "token", "region", "target", "heartbeat_s", "id"}
        if unknown:
            raise ValueError(f"lease has unknown fields: {sorted(unknown)}")
        lease = cls(
            kind=d["kind"], token=d.get("token") or "", region=d.get("region") or "us-east-1",
            target=d.get("target"), heartbeat_s=d.get("heartbeat_s", 30), id=d.get("id"),
        )
        lease.validate()
        return lease


@dataclass
class LeasePolicy:
    """How long the orchestrator waits, and the VM lifetime that goes with it."""

    budget_s: int = 900
    heartbeat_timeout_s: int = 120
    slack_s: int = 120

    def idle_policy(self):
        """No auto-resume: a leased VM that goes idle is finished, not dormant."""
        from microvm.fleet import IdlePolicy

        return IdlePolicy(max_idle=self.budget_s, suspended_for=60, auto_resume=False)

    def max_duration(self) -> int:
        return min(self.budget_s + self.slack_s, MAX_DURATION_SECONDS)


def encode_payload(lease: Lease, task: dict) -> str:
    """The runHookPayload string for RunMicrovm; refuses anything over the 4096-char limit."""
    lease.validate()
    raw = json.dumps({"lease": lease.to_dict(), "task": task}, separators=(",", ":"))
    if len(raw) > PAYLOAD_LIMIT:
        raise ValueError(
            f"runHookPayload is {len(raw)} chars, limit {PAYLOAD_LIMIT}: pass pointers, not bodies")
    return raw


def decode_payload(raw: str | None) -> tuple[Lease, dict]:
    """Inverse of encode_payload. Raises ValueError when raw is missing or not a lease payload."""
    if not raw:
        raise ValueError("runHookPayload is empty")
    try:
        obj = json.loads(raw)
    except (TypeError, ValueError) as e:
        raise ValueError(f"runHookPayload is not JSON: {e}") from None
    if not isinstance(obj, dict) or "lease" not in obj:
        raise ValueError("runHookPayload has no 'lease' object")
    task = obj.get("task", {})
    if not isinstance(task, dict):
        raise ValueError(f"task must be a JSON object, got {type(task).__name__}")
    return Lease.from_dict(obj["lease"]), task


def client_token(lease: Lease, salt: str = "") -> str:
    """RunMicrovm clientToken for a lease.

    A lease with a token is single-use, so the same lease always maps to the same token and a
    replayed launch returns the same VM. A lease without a token (kind `none`) has nothing
    single-use to key on: every call gets a fresh token, otherwise the service rejects the
    second launch with "clientToken was used with different request parameters"."""
    material = f"{lease.kind}:{lease.token}:{lease.id or ''}:{salt}"
    if not lease.token:
        material += ":" + uuid.uuid4().hex
    return hashlib.sha256(material.encode()).hexdigest()[:64]
