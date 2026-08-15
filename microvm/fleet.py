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

from microvm.client import microvm_client, image_arn
from microvm.config import PlaneConfig, TPS
from microvm.throttle import Throttled

ACTIVE_STATES = {"PENDING", "RUNNING", "SUSPENDING", "SUSPENDED"}


@dataclass
class Microvm:
    microvm_id: str
    state: str
    image_arn: str
    image_version: str
    started_at: object = None
    endpoint: str | None = None

    @classmethod
    def from_api(cls, d: dict) -> "Microvm":
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

    def __init__(self, config: PlaneConfig, quota_aware: bool = True):
        self.cfg = config
        self.api = microvm_client(config.region, config.profile)
        self.quotas = applied_quotas(config) if quota_aware else {}
        tps = lambda op: (self.quotas.get(op) or TPS[op]) * 0.8
        self._run = Throttled(self.api.run_microvm, tps("RunMicrovm"))
        self._suspend = Throttled(self.api.suspend_microvm, tps("SuspendMicrovm"))
        self._resume = Throttled(self.api.resume_microvm, tps("ResumeMicrovm"))
        self._terminate = Throttled(self.api.terminate_microvm, tps("TerminateMicrovm"))

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
    run_payload_factory: object = None  # callable (index:int) -> str, for per-VM payloads
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
            victims = sorted(
                current,
                key=lambda vm: (vm.state != "SUSPENDED", vm.started_at or 0),
                reverse=False,
            )
            # SUSPENDED first, then youngest RUNNING (sort puts suspended first,
            # oldest first — so take suspended, then from the tail of running).
            suspended = [v for v in victims if v.state == "SUSPENDED"]
            running = [v for v in victims if v.state != "SUSPENDED"]
            running.sort(key=lambda vm: vm.started_at or 0, reverse=True)  # youngest first
            for vm in (suspended + running)[: -delta]:
                self.manager.terminate(vm.microvm_id)
        return []

    def _launch_one(self, index: int) -> Microvm:
        payload = self.run_payload_factory(index) if callable(self.run_payload_factory) else None
        return self.manager.run(
            self.image,
            version=self.version,
            idle_policy=self.idle_policy,
            run_payload=payload,
            max_duration=self.max_duration,
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
            started = vm.started_at.timestamp() if hasattr(vm.started_at, "timestamp") else None
            if started and now - started > max_age_seconds:
                self.manager.terminate(vm.microvm_id)
                reaped.append(vm.microvm_id)
        return reaped
