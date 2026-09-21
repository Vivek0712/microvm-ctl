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
import math
import os
import uuid
from dataclasses import asdict, dataclass

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


POLICY_ENV = {
    "budget_s": ("MVM_LEASE_BUDGET_S", int),
    "heartbeat_timeout_s": ("MVM_LEASE_HEARTBEAT_TIMEOUT_S", int),
    "slack_s": ("MVM_LEASE_SLACK_S", int),
    "max_concurrency": ("MVM_LEASE_MAX_CONCURRENCY", int),
    "max_vm_seconds": ("MVM_LEASE_MAX_VM_SECONDS", int),
    "approval_usd": ("MVM_LEASE_APPROVAL_USD", float),
}


@dataclass
class LeasePolicy:
    """How long the orchestrator waits, the VM lifetime that goes with it, and the
    platform-owned ceilings a fan-out must stay under (None means unlimited)."""

    budget_s: int = 900
    heartbeat_timeout_s: int = 120
    slack_s: int = 120
    max_concurrency: int | None = None
    max_vm_seconds: int | None = None
    approval_usd: float | None = None

    @classmethod
    def from_env(cls) -> LeasePolicy:
        """Defaults overridden by MVM_LEASE_BUDGET_S, MVM_LEASE_HEARTBEAT_TIMEOUT_S, MVM_LEASE_SLACK_S,
        MVM_LEASE_MAX_CONCURRENCY, MVM_LEASE_MAX_VM_SECONDS, and MVM_LEASE_APPROVAL_USD.
        An empty variable counts as unset; a malformed one raises ValueError naming it."""
        kw = {}
        for field_name, (var, cast) in POLICY_ENV.items():
            raw = os.environ.get(var, "").strip()
            if not raw:
                continue
            try:
                kw[field_name] = cast(raw)
            except ValueError:
                raise ValueError(f"{var}={raw!r} is not a valid {cast.__name__}") from None
        return cls(**kw)

    def to_dict(self) -> dict:
        return asdict(self)

    def heartbeat_every(self, requested: int | float | None = None) -> int:
        """The interval the VM should heartbeat at: the requested seconds (default 30), never
        more than a third of `heartbeat_timeout_s` and never under 5 s. A heartbeat interval at
        or above the timeout loses the lease before the first heartbeat lands, because the
        orchestrator's clock starts before RunMicrovm returns."""
        want = 30 if requested is None else float(requested)
        ceiling = max(5.0, self.heartbeat_timeout_s / 3.0)
        return int(max(5.0, min(want, ceiling)))

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


# ---------------------------------------------------------------------------- sizing
# Running a VM costs vCPU-seconds plus GB-seconds, and vCPU = memory / 2, so one GB-second of
# a VM is half a vCPU-second plus one GB-second. The two rates are the published us-east-1
# figures (monitor.RATE_VCPU_SECOND and monitor.RATE_GB_SECOND, repeated here because this
# module must stay importable without boto3; tests pin them to each other).
VM_USD_PER_GB_S = 0.0000276944 / 2 + 0.0000036667
#: seconds from RunMicrovm accepted to RUNNING for one snapshot restore (measured)
RESTORE_S = 3.5
#: fan-out ceiling when the memory quota is unknown and the policy sets no max_concurrency
DEFAULT_FANOUT_LIMIT = 8


class LeasePlanRejected(ValueError):
    """The plan must not launch; str(exc) is the reason."""


@dataclass
class FanoutLimit:
    """How many leases of one baseline can be in flight at once, and why."""

    baseline_mib: int
    memory_quota_gb: float | None
    launch_rate: float
    by_memory: int | None
    by_policy: int | None
    limit: int
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)


def fanout_limit(
    baseline_mib: int, policy: LeasePolicy, *, memory_quota_gb: float | None, launch_rate: float,
) -> FanoutLimit:
    """Pure arithmetic: floor(quota_gb * 1024 / baseline_mib) when the quota is known, the
    policy's max_concurrency when set, the smaller of the two when both are known, and
    DEFAULT_FANOUT_LIMIT (8) when neither is."""
    if baseline_mib < 1:
        raise ValueError(f"baseline_mib must be positive, got {baseline_mib}")
    by_memory = None if memory_quota_gb is None else int(memory_quota_gb * 1024 // baseline_mib)
    by_policy = policy.max_concurrency
    baseline_gb = baseline_mib / 1024
    if by_memory is not None and (by_policy is None or by_memory <= by_policy):
        limit, reason = by_memory, f"memory quota {memory_quota_gb:g} GB / {baseline_gb:g} GB baseline"
    elif by_policy is not None:
        limit, reason = by_policy, f"policy max_concurrency {by_policy}"
    else:
        limit = DEFAULT_FANOUT_LIMIT
        reason = f"default {DEFAULT_FANOUT_LIMIT}: memory quota unknown and no policy max_concurrency"
    return FanoutLimit(
        baseline_mib=baseline_mib, memory_quota_gb=memory_quota_gb, launch_rate=launch_rate,
        by_memory=by_memory, by_policy=by_policy, limit=limit, reason=reason,
    )


@dataclass
class LeasePlan:
    """The honest numbers for one fan-out of `shards` leases, before anything launches."""

    shards: int
    baseline_mib: int
    policy: LeasePolicy
    limit: FanoutLimit
    concurrency: int
    waves: int
    launch_to_all_running_s: float
    worst_case_vm_seconds: int
    worst_case_usd: float
    needs_approval: bool
    rejected: str | None

    def check(self) -> None:
        """Raise LeasePlanRejected(rejected) when the plan must not launch."""
        if self.rejected:
            raise LeasePlanRejected(self.rejected)

    def summary(self) -> str:
        """One sentence, for a log line or a console."""
        gb = self.baseline_mib / 1024
        if self.rejected:
            return f"{self.shards} shards on {gb:g} GB rejected: {self.rejected}"
        text = (
            f"{self.shards} shards on {gb:g} GB: {self.concurrency} at a time ({self.limit.reason}), "
            f"{self.waves} wave{'s' if self.waves != 1 else ''}, "
            f"all running in ~{self.launch_to_all_running_s:.0f} s, "
            f"worst case {self.worst_case_vm_seconds} VM-s = ${self.worst_case_usd:.2f}"
        )
        if self.needs_approval:
            text += f", needs approval (above ${self.policy.approval_usd:g})"
        return text

    def to_dict(self) -> dict:
        d = asdict(self)
        d["summary"] = self.summary()
        return d


def plan_fanout(
    shards: int, baseline_mib: int, policy: LeasePolicy, *, memory_quota_gb: float | None, launch_rate: float,
) -> LeasePlan:
    """Size a fan-out of `shards` leases of `baseline_mib` each under `policy`.

    Rejection rules, in order: shards < 1; the baseline alone exceeds the memory quota;
    policy.max_vm_seconds set and exceeded by the worst case; policy.max_concurrency == 0.
    More shards than the concurrency limit is not a rejection: that is waves."""
    limit = fanout_limit(baseline_mib, policy, memory_quota_gb=memory_quota_gb, launch_rate=launch_rate)
    concurrency = max(0, min(shards, limit.limit))
    waves = math.ceil(shards / concurrency) if concurrency > 0 else 0
    per_wave = ((concurrency - 1) / launch_rate if launch_rate > 0 else 0.0) + RESTORE_S
    launch_s = per_wave * waves
    worst_vm_s = max(shards, 0) * policy.max_duration()
    worst_usd = worst_vm_s * (baseline_mib / 1024) * VM_USD_PER_GB_S
    needs_approval = policy.approval_usd is not None and worst_usd > policy.approval_usd

    rejected = None
    if shards < 1:
        rejected = f"shards must be at least 1, got {shards}"
    elif limit.by_memory == 0:
        rejected = (f"baseline {baseline_mib} MiB exceeds the memory quota "
                    f"{memory_quota_gb:g} GB: nothing can launch")
    elif policy.max_vm_seconds is not None and worst_vm_s > policy.max_vm_seconds:
        rejected = (f"worst case {worst_vm_s} VM-seconds exceeds policy max_vm_seconds "
                    f"{policy.max_vm_seconds}")
    elif policy.max_concurrency == 0:
        rejected = "policy max_concurrency is 0: fan-out is switched off"

    return LeasePlan(
        shards=shards, baseline_mib=baseline_mib, policy=policy, limit=limit,
        concurrency=concurrency, waves=waves, launch_to_all_running_s=round(launch_s, 2),
        worst_case_vm_seconds=worst_vm_s, worst_case_usd=round(worst_usd, 6),
        needs_approval=needs_approval, rejected=rejected,
    )
