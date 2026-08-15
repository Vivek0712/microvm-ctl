"""Observability: fleet status, CloudWatch logs, and per-session cost modeling.

MicroVM billing has four metered dimensions; the cost model here mirrors the
published us-east-1 rates so `mvm cost` and the monitor can attribute spend
per VM/session — the levers, in order of impact: suspend ratio, right-sized
baseline (4x vertical burst absorbs peaks), terminate-don't-suspend for
one-shot jobs, and snapshot size discipline.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from microvm.client import lambda_client, microvm_client, image_arn
from microvm.config import PlaneConfig

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


class FleetMonitor:
    def __init__(self, config: PlaneConfig):
        self.cfg = config
        self.api = microvm_client(config.region, config.profile)
        self.logs = lambda_client("logs", config.region, config.profile)

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

    def tail_logs(self, image_name: str, minutes: int = 15, limit: int = 200) -> list[dict]:
        """Build + runtime logs land in /aws/lambda/microvms/<image-name>,
        one stream per microVM."""
        group = f"/aws/lambda/microvms/{image_name}"
        try:
            resp = self.logs.filter_log_events(
                logGroupName=group,
                startTime=int((time.time() - minutes * 60) * 1000),
                limit=limit,
            )
        except self.logs.exceptions.ResourceNotFoundException:
            return []
        return [
            {"stream": e["logStreamName"], "ts": e["timestamp"], "message": e["message"].rstrip()}
            for e in resp.get("events", [])
        ]

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
