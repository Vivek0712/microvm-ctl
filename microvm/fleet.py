"""Fleet manager: run, suspend, resume, terminate, and scale microVM fleets.

A *fleet* is the set of microVMs launched from one image (optionally pinned
to a version). One endpoint == one microVM — there is no load balancer in
the service — so horizontal scale is literally more RunMicrovm calls, and
routing across the fleet is the control plane's job (see endpoint.py).

Every mutating call is throttled to the service's published TPS quotas and
retried with jittered backoff, so `scale_to(50)` is safe to call in one shot.
"""

from __future__ import annotations

import concurrent.futures as futures
import time
from dataclasses import dataclass, field
from typing import Callable

from microvm.client import image_arn, microvm_client
from microvm.config import TPS, PlaneConfig
from microvm.throttle import Throttled

ACTIVE_STATES = {"PENDING", "RUNNING", "SUSPENDING", "SUSPENDED"}


@dataclass
class Microvm:
    microvm_id: str
    state: str
    image_arn: str
    image_version: str
    started_at: object = None  # datetime from the API, or None
    endpoint: str | None = None

    @property
    def started_epoch(self) -> float:
        """startedAt as a POSIX timestamp (0.0 when unknown) - safe to sort on."""
        ts = getattr(self.started_at, "timestamp", None)
        return float(ts()) if callable(ts) else 0.0

    @classmethod
    def from_api(cls, d: dict) -> Microvm:
        return cls(
            microvm_id=d["microvmId"],
            state=d["state"],
            image_arn=d["imageArn"],
            image_version=d["imageVersion"],
            started_at=d.get("startedAt"),
            endpoint=d.get("endpoint"),
        )


@dataclass
class IdlePolicy:
    """Suspend after `max_idle` seconds without endpoint traffic; auto-resume on
    the next request; auto-TERMINATE after `suspended_for` seconds suspended."""

    max_idle: int = 300
    suspended_for: int = 3600
    auto_resume: bool = True

    def to_api(self) -> dict:
        return {
            "maxIdleDurationSeconds": self.max_idle,
            "suspendedDurationSeconds": self.suspended_for,
            "autoResumeEnabled": self.auto_resume,
        }


# Service Quotas codes for the applied (per-account) API rates — new accounts
# run reduced profiles (e.g. RunMicrovm 1/s instead of 5/s), so the published
# defaults are the wrong thing to throttle against.
QUOTA_CODES = {
    "RunMicrovm": "L-535CA9B6",
    "SuspendMicrovm": "L-90045317",
    "ResumeMicrovm": "L-118C44B3",
    "TerminateMicrovm": "L-74787B8A",
    "MaxMemoryGb": "L-CD1C0CC4",
}


def applied_quotas(config: PlaneConfig) -> dict[str, float]:
    """Best-effort lookup of this account's *applied* microVM quotas."""
    from microvm.client import lambda_client
    try:
        sq = lambda_client("service-quotas", config.region, config.profile)
        out = {}
        for name, code in QUOTA_CODES.items():
            try:
                out[name] = sq.get_service_quota(ServiceCode="lambda", QuotaCode=code)[
                    "Quota"]["Value"]
            except Exception:
                pass
        return out
    except Exception:
        return {}


class FleetManager:
    """Low-level lifecycle operations, one instance per (account, region)."""

    #: fraction of the applied TPS quota the token buckets are allowed to use
    QUOTA_HEADROOM = 0.8

    def __init__(self, config: PlaneConfig, quota_aware: bool = True):
        self.cfg = config
        self.api = microvm_client(config.region, config.profile)
        self.quotas = applied_quotas(config) if quota_aware else {}
        self._run = Throttled(self.api.run_microvm, self.tps("RunMicrovm"))
        self._suspend = Throttled(self.api.suspend_microvm, self.tps("SuspendMicrovm"))
        self._resume = Throttled(self.api.resume_microvm, self.tps("ResumeMicrovm"))
        self._terminate = Throttled(self.api.terminate_microvm, self.tps("TerminateMicrovm"))

    def tps(self, op: str) -> float:
        """Effective rate for one mutating API: applied quota (or the published
        default when Service Quotas is unavailable) times QUOTA_HEADROOM."""
        return (self.quotas.get(op) or TPS[op]) * self.QUOTA_HEADROOM

    @property
    def memory_quota_gb(self) -> float | None:
        """Applied 'max allocated microVM memory' quota, if Service Quotas answered."""
        return self.quotas.get("MaxMemoryGb")

    # -- single VM ---------------------------------------------------------------
    def run(
        self,
        image: str,
        *,
        version: str | None = None,
        idle_policy: IdlePolicy | None = None,
        run_payload: str | None = None,
        max_duration: int | None = None,
        ingress: list[str] | None = None,
        egress: list[str] | None = None,
        execution_role: str | None = None,
    ) -> Microvm:
        params: dict = {"imageIdentifier": image_arn(image, self.cfg.region, self.cfg.profile)}
        if version:
            params["imageVersion"] = version
        params["idlePolicy"] = (idle_policy or IdlePolicy()).to_api()
        if run_payload is not None:
            params["runHookPayload"] = run_payload
        if max_duration:
            params["maximumDurationInSeconds"] = max_duration
        if ingress:
            params["ingressNetworkConnectors"] = ingress
        if egress:
            params["egressNetworkConnectors"] = egress
        role = execution_role or self.cfg.execution_role_arn
        if role:
            params["executionRoleArn"] = role
        return Microvm.from_api(self._run(**params))

    def get(self, microvm_id: str) -> Microvm:
        return Microvm.from_api(self.api.get_microvm(microvmIdentifier=microvm_id))

    def suspend(self, microvm_id: str) -> None:
        self._suspend(microvmIdentifier=microvm_id)

    def resume(self, microvm_id: str) -> None:
        self._resume(microvmIdentifier=microvm_id)

    def terminate(self, microvm_id: str) -> None:
        self._terminate(microvmIdentifier=microvm_id)

    def wait_until(self, microvm_id: str, state: str, timeout: int = 120) -> Microvm:
        deadline = time.time() + timeout
        while time.time() < deadline:
            vm = self.get(microvm_id)
            if vm.state == state:
                return vm
            if vm.state == "TERMINATED" and state != "TERMINATED":
                raise RuntimeError(f"{microvm_id} terminated while waiting for {state}")
            time.sleep(2)
        raise TimeoutError(f"{microvm_id} did not reach {state} in {timeout}s")

    def list(self, image: str | None = None, version: str | None = None) -> list[Microvm]:
        kwargs: dict = {}
        if image:
            kwargs["imageIdentifier"] = image_arn(image, self.cfg.region, self.cfg.profile)
        if version:
            kwargs["imageVersion"] = version
        out = self.api.get_paginator("list_microvms").paginate(**kwargs).build_full_result()
        return [Microvm.from_api(d) for d in out.get("items", [])]


@dataclass
class Fleet:
    """Declarative fleet of microVMs from one image: scale up, down, drain."""

    manager: FleetManager
    image: str
    version: str | None = None
    idle_policy: IdlePolicy = field(default_factory=IdlePolicy)
    max_duration: int | None = None
    #: called with the launch index (0..n-1) to produce that VM's runHookPayload
    run_payload_factory: Callable[[int], str] | None = None
    ingress: list[str] | None = None
    egress: list[str] | None = None
    execution_role: str | None = None
    _pool: futures.ThreadPoolExecutor = field(
        default_factory=lambda: futures.ThreadPoolExecutor(max_workers=8), repr=False
    )

    # -- observation -------------------------------------------------------------
    def members(self) -> list[Microvm]:
        return [
            vm
            for vm in self.manager.list(self.image, self.version)
            if vm.state in ACTIVE_STATES
        ]

    def size(self) -> int:
        return len(self.members())

    # -- scaling -----------------------------------------------------------------
    def scale_to(self, desired: int, wait_running: bool = False) -> list[Microvm]:
        """Converge the fleet to `desired` active microVMs.

        Scale-up launches new VMs (throttled to the RunMicrovm quota).
        Scale-down terminates SUSPENDED VMs first (they cost only snapshot
        storage but count against the regional memory quota), then the
        youngest RUNNING VMs, sparing the oldest — they hold the warmest state.
        """
        current = self.members()
        delta = desired - len(current)
        if delta > 0:
            launched = list(
                self._pool.map(lambda i: self._launch_one(i), range(delta))
            )
            if wait_running:
                launched = [
                    self.manager.wait_until(vm.microvm_id, "RUNNING") for vm in launched
                ]
            return launched
        if delta < 0:
            for vm in self.scale_down_victims(current, -delta):
                self.manager.terminate(vm.microvm_id)
        return []

    @staticmethod
    def scale_down_victims(members: list[Microvm], count: int) -> list[Microvm]:
        """Pick `count` members to terminate: every SUSPENDED VM first (storage
        cost only, but they hold memory quota), then RUNNING VMs youngest first
        so the oldest, warmest members survive."""
        suspended = [v for v in members if v.state == "SUSPENDED"]
        running = [v for v in members if v.state != "SUSPENDED"]
        running.sort(key=lambda vm: vm.started_epoch, reverse=True)  # youngest first
        return (suspended + running)[:count]

    def _launch_one(self, index: int) -> Microvm:
        payload = self.run_payload_factory(index) if self.run_payload_factory else None
        return self.manager.run(
            self.image,
            version=self.version,
            idle_policy=self.idle_policy,
            run_payload=payload,
            max_duration=self.max_duration,
            ingress=self.ingress,
            egress=self.egress,
            execution_role=self.execution_role,
        )

    def suspend_all(self) -> int:
        vms = [v for v in self.members() if v.state == "RUNNING"]
        list(self._pool.map(lambda v: self.manager.suspend(v.microvm_id), vms))
        return len(vms)

    def resume_all(self) -> int:
        vms = [v for v in self.members() if v.state == "SUSPENDED"]
        list(self._pool.map(lambda v: self.manager.resume(v.microvm_id), vms))
        return len(vms)

    def drain(self) -> int:
        """Terminate every member of the fleet."""
        vms = self.members()
        list(self._pool.map(lambda v: self.manager.terminate(v.microvm_id), vms))
        return len(vms)

    # -- reaper ------------------------------------------------------------------
    def reap(self, max_age_seconds: int) -> list[str]:
        """Terminate members older than `max_age_seconds` (belt-and-braces on top
        of maximumDurationInSeconds and the idle policy's auto-terminate)."""
        now = time.time()
        reaped = []
        for vm in self.members():
            started = vm.started_epoch
            if started and now - started > max_age_seconds:
                self.manager.terminate(vm.microvm_id)
                reaped.append(vm.microvm_id)
        return reaped
