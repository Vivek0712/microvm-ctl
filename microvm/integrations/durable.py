"""Lambda durable functions integration: lease a microVM inside a durable execution.

    create_callback  ->  step: FleetManager.lease(...)  ->  callback.result()  ->  step: terminate

The launch step runs at most once per retry with no SDK retries and a clientToken
derived from the callback id, so a replay after a crash gets the same VM back.
`callback.result()` suspends the execution by raising a BaseException, so it is never
wrapped in try/finally: each `except` branch terminates the VM itself.

`lease_map` fans out: a plan step (the plane's sizing and pre-flight refusal), an
optional approval callback, then `context.map` over the tasks with one
`lease_with_relaunch` per item under the plan's concurrency.

Requires `pip install microvm-ctl[durable]` (Python 3.11+). Importing this module
without the SDK works; calling `lease_microvm` raises ImportError with the hint.
"""

from __future__ import annotations

import dataclasses
import json
import os
from typing import Any, Callable

from microvm.lease import Lease, LeasePolicy

try:  # the durable SDK is optional; keep the module importable without it
    from aws_durable_execution_sdk_python import (
        CallbackError,
        CallbackExternalError,
        CallbackTimeoutError,
        durable_step,
    )
    from aws_durable_execution_sdk_python.config import (
        CallbackConfig,
        CompletionConfig,
        Duration,
        MapConfig,
        StepConfig,
        StepSemantics,
        WaitForCallbackConfig,
    )
    from aws_durable_execution_sdk_python.retries import RetryPresets

    _SDK_ERROR: ImportError | None = None
except ImportError as exc:  # pragma: no cover - exercised on the 3.9 CI leg only
    _SDK_ERROR = exc

    def durable_step(func):  # type: ignore[no-redef]
        return func


def _require_sdk() -> None:
    if _SDK_ERROR is not None:
        raise ImportError(
            "microvm.integrations.durable needs the AWS durable execution SDK: "
            "pip install 'microvm-ctl[durable]' (Python 3.11+)"
        ) from _SDK_ERROR


# ------------------------------------------------------------------ steps (side effects)
@durable_step
def _launch(step, fm, image, lease_dict: dict, task: dict, policy_dict: dict, version, execution_role):
    lease = Lease.from_dict(lease_dict)
    policy = LeasePolicy(**policy_dict)
    vm = fm.lease(image, lease, task, policy, version=version, execution_role=execution_role)
    step.logger.info("leased %s (%s) for %s", vm.microvm_id, vm.endpoint, lease.id)
    return {"microvm_id": vm.microvm_id, "endpoint": vm.endpoint}


@durable_step
def _terminate(step, fm, microvm_id: str) -> dict:
    try:
        fm.terminate(microvm_id)
        return {"terminated": microvm_id}
    except fm.api.exceptions.ResourceNotFoundException:
        return {"terminated": microvm_id, "already_gone": True}


@durable_step
def _plan(step, fm, shards: int, baseline_mib: int, policy_dict: dict | None) -> dict:
    """Size the fan-out (reads the memory quota once); the dict is what replays see."""
    policy = LeasePolicy(**policy_dict) if policy_dict else None
    plan = fm.plan(shards, baseline_mib, policy)
    d = plan_dict(plan)
    step.logger.info("plan: %s", d.get("summary"))
    return d


# ------------------------------------------------------------------ pure helpers
def function_region(context) -> str:
    """The region the VM must call back to: the execution ARN's region, else the
    environment (the local test runner uses a non-ARN identifier)."""
    arn = getattr(getattr(context, "execution_context", None), "durable_execution_arn", None) or ""
    parts = arn.split(":")
    if arn.startswith("arn:") and len(parts) > 3 and parts[3]:
        return parts[3]
    return os.environ.get("MVM_REGION") or os.environ.get("AWS_REGION", "us-east-1")


def execution_name(context) -> str | None:
    """A short label for the lease: the last path segment of the execution ARN."""
    arn = getattr(getattr(context, "execution_context", None), "durable_execution_arn", None) or ""
    if not arn:
        return None
    return arn.replace(":", "/").rstrip("/").rsplit("/", 1)[-1][:128] or None


def _policy_dict(policy: LeasePolicy | None) -> dict | None:
    return dataclasses.asdict(policy) if policy is not None else None


def plan_dict(plan: Any) -> dict:
    """A JSON-safe view of a `LeasePlan` (or anything shaped like one) that keeps the
    fields `lease_map` reads back after a replay: rejected, needs_approval, concurrency,
    and the one-sentence summary."""
    if hasattr(plan, "to_dict"):
        d = dict(plan.to_dict())
    elif dataclasses.is_dataclass(plan):
        d = dataclasses.asdict(plan)
    else:
        d = {k: v for k, v in vars(plan).items() if not k.startswith("_")}
    for key in ("rejected", "needs_approval", "concurrency"):
        d.setdefault(key, getattr(plan, key, None))
    if "summary" not in d:
        summary = getattr(plan, "summary", None)
        d["summary"] = summary() if callable(summary) else summary
    return json.loads(json.dumps(d, default=str))


def _decode(raw: Any) -> Any:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return raw
    return raw


def _error_from(exc) -> dict:
    """Normalise a CallbackExternalError into the contract's error object. The VM's
    completer puts the whole completion payload in ErrorData, so prefer that."""
    data = _decode(getattr(exc, "data", None))
    err = data.get("error") if isinstance(data, dict) else None
    if not isinstance(err, dict):
        err = data if isinstance(data, dict) and "error_type" in data else {}
    sdk_type = getattr(exc, "error_type", None) or ""
    if sdk_type.startswith("aws_durable_execution_sdk_python"):
        sdk_type = ""  # the SDK's own class path, not the VM's error type
    return {
        "error_type": err.get("error_type") or sdk_type or "Unknown",
        "message": err.get("message") or getattr(exc, "message", None) or str(exc),
        "retryable": bool(err.get("retryable", False)),
        "data": err.get("data") if isinstance(err.get("data"), dict) else {},
    }


# ------------------------------------------------------------------ the lease
def lease_microvm(
    context,
    fm,
    image: str,
    task: dict,
    *,
    policy: LeasePolicy | None = None,
    label: str = "lease",
    version: str | None = None,
    execution_role: str | None = None,
    heartbeat_s: int = 30,
) -> dict:
    """One lease: callback -> launch -> wait -> terminate.

    Returns `{"status": "done", "result": ..., "retryable": False, "vm": {...}}`,
    `{"status": "timed_out", "error": {...}, "retryable": True, "vm": {...}}`, or
    `{"status": "failed", "error": {...}, "retryable": bool, "vm": {...}}`, and never
    raises for those expected outcomes so the caller can decide to relaunch."""
    _require_sdk()
    policy = policy or LeasePolicy()
    region = function_region(context)
    callback = context.create_callback(
        name=f"{label}-callback",
        config=CallbackConfig(
            timeout=Duration.from_seconds(policy.budget_s),
            heartbeat_timeout=Duration.from_seconds(policy.heartbeat_timeout_s),
        ),
    )
    lease = Lease(kind="durable", token=callback.callback_id, region=region,
                  heartbeat_s=heartbeat_s, id=execution_name(context))
    policy_dict = {"budget_s": policy.budget_s, "heartbeat_timeout_s": policy.heartbeat_timeout_s,
                   "slack_s": policy.slack_s}
    launch_once = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
                             retry_strategy=RetryPresets.none())
    vm = context.step(
        _launch(fm, image, lease.to_dict(), task, policy_dict, version, execution_role),
        name=f"{label}-launch", config=launch_once,
    )

    try:
        raw = callback.result()  # suspends here; no compute billed while the VM works
    except CallbackTimeoutError as e:
        context.step(_terminate(fm, vm["microvm_id"]), name=f"{label}-terminate")
        return {"status": "timed_out", "retryable": True, "vm": vm,
                "error": {"error_type": "CallbackTimeout", "message": str(e), "retryable": True, "data": {}}}
    except CallbackExternalError as e:
        context.step(_terminate(fm, vm["microvm_id"]), name=f"{label}-terminate")
        err = _error_from(e)
        return {"status": "failed", "error": err, "retryable": err["retryable"], "vm": vm}

    context.step(_terminate(fm, vm["microvm_id"]), name=f"{label}-terminate")
    payload = _decode(raw)
    result = payload.get("result", payload) if isinstance(payload, dict) and "result" in payload else payload
    return {"status": "done", "result": result, "retryable": False, "vm": vm}


def lease_with_relaunch(context, fm, image: str, task: dict, *, max_relaunches: int = 1,
                        label: str = "lease", **kw) -> dict:
    """Run `lease_microvm` up to `max_relaunches + 1` times while the outcome is
    retryable (timeout, or a failure the VM marked retryable). Adds `attempt`."""
    outcome: dict = {}
    for attempt in range(max_relaunches + 1):
        outcome = lease_microvm(context, fm, image, task, label=f"{label}-{attempt}", **kw)
        outcome["attempt"] = attempt
        if outcome["status"] == "done" or not outcome.get("retryable"):
            return outcome
    return outcome


# ------------------------------------------------------------------ the fan-out
def lease_map(
    context,
    fm,
    image: str,
    tasks: list,
    *,
    policy: LeasePolicy | None = None,
    label: str = "lease",
    baseline_mib: int,
    max_concurrency: int | None = None,
    max_relaunches: int = 1,
    approve: Callable[[str, dict], None] | None = None,
    approval_timeout_s: int = 3600,
    **kw,
) -> dict:
    """One lease per task, in parallel, under the plane's plan.

    (1) step `{label}-plan`: `fm.plan(len(tasks), baseline_mib, policy)`; a rejected plan
    returns `{"status": "rejected", "reason", "plan"}` and launches nothing. (2) When the
    plan needs approval: with `approve`, `wait_for_callback` named `{label}-approval` whose
    submitter calls `approve(callback_id, plan_dict)` (publish it somewhere a human or a
    policy engine answers with SendDurableExecutionCallbackSuccess); a timeout or a failure
    returns `{"status": "denied", ...}`; without `approve`, `{"status": "approval_required",
    "plan"}`. (3) `context.map` named `{label}-map` over the tasks, `max_concurrency` the
    plan's concurrency (or lower when given), each item `lease_with_relaunch(...,
    label=f"{label}-{i}")`; extra keyword arguments (version, execution_role, heartbeat_s)
    reach every lease. (4) `{"status": "done", "plan", "outcomes", "succeeded", "failed",
    "errors"}` where `outcomes` are the per-shard outcome dicts in completion order,
    `succeeded` counts status "done", and `errors` lists shards whose item raised."""
    _require_sdk()
    tasks = list(tasks)
    plan = context.step(_plan(fm, len(tasks), int(baseline_mib), _policy_dict(policy)), name=f"{label}-plan")
    if plan.get("rejected"):
        return {"status": "rejected", "reason": plan["rejected"], "plan": plan}
    if plan.get("needs_approval"):
        if approve is None:
            return {"status": "approval_required", "plan": plan}

        def submitter(callback_id: str, _ctx) -> None:
            approve(callback_id, plan)

        try:
            context.wait_for_callback(
                submitter, name=f"{label}-approval",
                config=WaitForCallbackConfig(timeout=Duration.from_seconds(int(approval_timeout_s))),
            )
        except CallbackTimeoutError as e:
            reason = f"no approval within {approval_timeout_s}s: {e}"
            return {"status": "denied", "reason": reason, "plan": plan}
        except CallbackError as e:
            return {"status": "denied", "reason": getattr(e, "message", None) or str(e), "plan": plan}

    concurrency = int(plan.get("concurrency") or 1)
    if max_concurrency is not None:
        concurrency = max(1, min(concurrency, int(max_concurrency)))

    def item(ctx, task, i, _all):
        return lease_with_relaunch(ctx, fm, image, task, policy=policy, label=f"{label}-{i}",
                                   max_relaunches=max_relaunches, **kw)

    results = context.map(
        tasks, item, name=f"{label}-map",
        config=MapConfig(max_concurrency=concurrency, completion_config=CompletionConfig.all_completed()),
    )
    outcomes = list(results.get_results())
    succeeded = sum(1 for o in outcomes if isinstance(o, dict) and o.get("status") == "done")
    errors = [{"error_type": getattr(e, "error_type", None) or "Unknown",
               "message": getattr(e, "message", None) or str(e)} for e in results.get_errors()]
    return {"status": "done", "plan": plan, "outcomes": outcomes, "succeeded": succeeded,
            "failed": len(tasks) - succeeded, "errors": errors}
