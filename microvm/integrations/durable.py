"""Lambda durable functions integration: lease a microVM inside a durable execution.

    create_callback  ->  step: FleetManager.lease(...)  ->  callback.result()  ->  step: terminate

The launch step runs at most once per retry with no SDK retries and a clientToken
derived from the callback id, so a replay after a crash gets the same VM back.
`callback.result()` suspends the execution by raising a BaseException, so it is never
wrapped in try/finally: each `except` branch terminates the VM itself.

Requires `pip install microvm-ctl[durable]` (Python 3.11+). Importing this module
without the SDK works; calling `lease_microvm` raises ImportError with the hint.
"""

from __future__ import annotations

import json
import os
from typing import Any

from microvm.lease import Lease, LeasePolicy

try:  # the durable SDK is optional; keep the module importable without it
    from aws_durable_execution_sdk_python import (
        CallbackExternalError,
        CallbackTimeoutError,
        durable_step,
    )
    from aws_durable_execution_sdk_python.config import CallbackConfig, Duration, StepConfig, StepSemantics
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
