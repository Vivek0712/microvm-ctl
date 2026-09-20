"""Observability: fleet status, CloudWatch logs, and per-session cost modeling.

MicroVM billing has four metered dimensions; the cost model here mirrors the
published us-east-1 rates so `mvm cost` and the monitor can attribute spend
per VM/session — the levers, in order of impact: suspend ratio, right-sized
baseline (4x vertical burst absorbs peaks), terminate-don't-suspend for
one-shot jobs, and snapshot size discipline.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass

from microvm.client import image_arn, lambda_client, microvm_client
from microvm.config import PlaneConfig
from microvm.endpoint import EndpointClient
from microvm.fleet import FleetManager

# us-east-1 launch rates. Re-verify against the live pricing page.
RATE_VCPU_SECOND = 0.0000276944
RATE_GB_SECOND = 0.0000036667
RATE_SNAPSHOT_WRITE_GB = 0.0038
RATE_SNAPSHOT_READ_GB = 0.00155
RATE_STORAGE_GB_MONTH = 0.08


@dataclass
class CostModel:
    """Estimate one microVM session. Memory in GiB; vCPU is memory/2 (fixed ratio)."""

    memory_gb: float = 2.0
    snapshot_gb: float | None = None  # suspended-state size; defaults to memory size

    @property
    def vcpu(self) -> float:
        return self.memory_gb / 2

    def running_cost(self, seconds: float) -> float:
        return seconds * (self.vcpu * RATE_VCPU_SECOND + self.memory_gb * RATE_GB_SECOND)

    def suspend_cycle_cost(self) -> float:
        gb = self.snapshot_gb or self.memory_gb
        return gb * (RATE_SNAPSHOT_WRITE_GB + RATE_SNAPSHOT_READ_GB)

    def suspended_cost(self, seconds: float) -> float:
        gb = self.snapshot_gb or self.memory_gb
        return gb * RATE_STORAGE_GB_MONTH * seconds / (30 * 86400)

    def session(self, active_s: float, suspended_s: float, cycles: int = 1) -> dict:
        run = self.running_cost(active_s)
        cyc = self.suspend_cycle_cost() * cycles
        sus = self.suspended_cost(suspended_s)
        always_on = self.running_cost(active_s + suspended_s)
        return {
            "running_usd": round(run, 6),
            "suspend_cycles_usd": round(cyc, 6),
            "suspended_storage_usd": round(sus, 6),
            "total_usd": round(run + cyc + sus, 6),
            "vs_always_on_usd": round(always_on, 6),
            "savings_pct": round(100 * (1 - (run + cyc + sus) / always_on), 1) if always_on else 0.0,
        }


def job_row(microvm_id: str, snap: dict) -> dict:
    """Reduce one hook-runtime `/status` snapshot to the fields a fleet table needs."""
    lease = snap.get("lease") or {}
    err = lease.get("error")
    return {
        "microvm_id": microvm_id,
        "lease_id": lease.get("id"),
        "phase": snap.get("phase"),
        "progress": snap.get("progress") or {"done": 0, "total": None},
        "elapsed_s": snap.get("elapsed_s"),
        "done": bool(lease.get("done")) if lease else snap.get("phase") == "done",
        "lost": bool(lease.get("lost")),
        "error": f"{err.get('error_type')}: {err.get('message')}" if isinstance(err, dict) else None,
    }


def job_summary(rows: list[dict]) -> dict:
    """The footer under a fleet job table: done D/N, running R, lost L, slowest member."""
    answered = [r for r in rows if "phase" in r]
    done = [r for r in answered if r["done"]]
    lost = [r for r in answered if r["lost"] and not r["done"]]
    running = [r for r in answered if not r["done"] and not r["lost"]]
    slowest = max(running or answered, key=lambda r: r.get("elapsed_s") or 0, default=None)
    return {
        "total": len(rows),
        "done": len(done),
        "running": len(running),
        "lost": len(lost),
        "failed": sum(1 for r in done if r["error"]),
        "unanswered": len(rows) - len(answered),
        "slowest": (
            {k: slowest[k] for k in ("microvm_id", "phase", "elapsed_s")} if slowest else None
        ),
    }


class FleetMonitor:
    #: Seconds one member gets to answer `/status` before its row becomes an error.
    MEMBER_TIMEOUT_S = 5.0

    def __init__(self, config: PlaneConfig):
        self.cfg = config
        self.api = microvm_client(config.region, config.profile)
        self.logs = lambda_client("logs", config.region, config.profile)
        self._ep_clients: dict = {}

    # -- fleet job: one /status per RUNNING member ---------------------------------
    def endpoint_client(self, vm, port: int = 8080, ttl: int = 15) -> EndpointClient:
        """One cached EndpointClient per (member, port, ttl): the token is minted once per VM
        and reused across polls until ~80% of its TTL."""
        key = (vm.microvm_id, port, ttl)
        client = self._ep_clients.get(key)
        if client is None:
            client = EndpointClient(self.cfg, vm.microvm_id, endpoint=vm.endpoint, ports=[port],
                                    token_ttl_minutes=ttl)
            self._ep_clients[key] = client
        return client

    def _pool(self) -> ThreadPoolExecutor:
        """One executor per monitor, reused across polls so a slow member never leaks a thread per call."""
        if getattr(self, "_status_pool", None) is None:
            self._status_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="job-status")
        return self._status_pool

    def job_status(self, image: str, *, port: int = 8080, ttl: int = 15) -> list[dict]:
        """One row per RUNNING member of `image`, from the hook runtime's `GET /status`:
        `{"microvm_id", "lease_id", "phase", "progress", "elapsed_s", "done", "lost", "error"}`,
        or `{"microvm_id", "error"}` for a member that does not answer within MEMBER_TIMEOUT_S.
        Members are polled in parallel (8 threads) and in list order; one bad member never
        fails the sweep."""
        members = [vm for vm in FleetManager(self.cfg).list(image) if vm.state == "RUNNING"]
        if not members:
            return []

        def one(vm) -> dict:
            client = self.endpoint_client(vm, port, ttl)
            # one attempt, bounded by the member timeout: a dead member must not pin a worker
            return job_row(vm.microvm_id, client.status(timeout=self.MEMBER_TIMEOUT_S, max_attempts=1))

        pool = self._pool()
        futs = [pool.submit(one, vm) for vm in members]
        deadline = time.time() + self.MEMBER_TIMEOUT_S
        rows = []
        for vm, fut in zip(members, futs):
            try:
                rows.append(fut.result(timeout=max(0.0, deadline - time.time())))
            except FutureTimeout:
                rows.append({"microvm_id": vm.microvm_id,
                             "error": f"no answer on /status within {self.MEMBER_TIMEOUT_S:g} s"})
            except Exception as e:
                rows.append({"microvm_id": vm.microvm_id, "error": f"{type(e).__name__}: {str(e)[:200]}"})
        for fut in futs:
            fut.cancel()  # not-yet-started polls of members that already timed out
        return rows

    def snapshot(self, image: str | None = None) -> dict:
        """Fleet-wide state counts + members, one paginated ListMicrovms sweep."""
        kwargs = {"imageIdentifier": image_arn(image, self.cfg.region, self.cfg.profile)} if image else {}
        items = (
            self.api.get_paginator("list_microvms").paginate(**kwargs).build_full_result()
        ).get("items", [])
        by_state: dict[str, int] = {}
        for vm in items:
            by_state[vm["state"]] = by_state.get(vm["state"], 0) + 1
        return {"total": len(items), "by_state": by_state, "members": items}

    @staticmethod
    def log_groups(image_name: str) -> list[str]:
        """Candidate log groups, service name first: build + runtime logs land in
        /aws/lambda-microvms/<image-name>, one stream per microVM; the old
        /aws/lambda/microvms/<image-name> name is tried second."""
        return [f"/aws/lambda-microvms/{image_name}", f"/aws/lambda/microvms/{image_name}"]

    def tail_logs(self, image_name: str, minutes: int = 15, limit: int = 200) -> list[dict]:
        """Recent events from the image's log group (first group that exists)."""
        for group in self.log_groups(image_name):
            try:
                resp = self.logs.filter_log_events(
                    logGroupName=group,
                    startTime=int((time.time() - minutes * 60) * 1000),
                    limit=limit,
                )
            except self.logs.exceptions.ResourceNotFoundException:
                continue
            return [
                {"stream": e["logStreamName"], "ts": e["timestamp"], "message": e["message"].rstrip()}
                for e in resp.get("events", [])
            ]
        return []

    def estimate_fleet_cost_per_hour(self, image: str | None, memory_gb: float) -> dict:
        snap = self.snapshot(image)
        model = CostModel(memory_gb=memory_gb)
        running = snap["by_state"].get("RUNNING", 0) + snap["by_state"].get("PENDING", 0)
        suspended = snap["by_state"].get("SUSPENDED", 0)
        return {
            "running_vms": running,
            "suspended_vms": suspended,
            "running_usd_per_hour": round(running * model.running_cost(3600), 4),
            "suspended_usd_per_hour": round(suspended * model.suspended_cost(3600), 6),
        }
