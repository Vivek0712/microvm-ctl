"""Step Functions integration: generate the lease state machine and its IAM.

One Standard-workflow task state leases a microVM with `runMicrovm.waitForTaskToken`,
the VM completes the task token itself (see `microvm.hooks.server` `on_lease` with
kind "sfn"), a `Terminate` state tidies up, and the `Catch` path reaps by image and
age because a timed-out lease carries no microVM id. Everything here is pure: no
boto3, no network, so the ASL can be generated in CI and committed.
"""

from __future__ import annotations

from microvm.lease import LeasePolicy

SDK = "arn:aws:states:::aws-sdk:lambdamicrovms"
RUN_WAIT = f"{SDK}:runMicrovm.waitForTaskToken"
TERMINATE = f"{SDK}:terminateMicrovm"
LIST = f"{SDK}:listMicrovms"

THROTTLE_ERRORS = ["LambdaMicrovms.ThrottlingException", "LambdaMicrovms.ServiceQuotaExceededException"]
NOT_FOUND = "LambdaMicrovms.ResourceNotFoundException"
STALE_STATES = ["RUNNING", "SUSPENDED", "PENDING"]


def _jsonata(expr: str) -> str:
    return f"{{% {expr} %}}"


def _retry_throttling() -> list[dict]:
    return [{
        "ErrorEquals": list(THROTTLE_ERRORS),
        "IntervalSeconds": 10,
        "MaxAttempts": 6,
        "BackoffRate": 2,
        "JitterStrategy": "FULL",
    }]


def _idle_policy_args(policy: LeasePolicy) -> dict:
    api = policy.idle_policy().to_api()
    return {k[0].upper() + k[1:]: v for k, v in api.items()}


def run_hook_payload_expr(region: str, heartbeat_s: int, task_expr: str = "$states.input") -> str:
    """The JSONata expression that builds the lease payload the VM decodes."""
    lease = (
        "{'kind': 'sfn', 'token': $states.context.Task.Token, "
        f"'region': '{region}', 'heartbeat_s': {int(heartbeat_s)}, "
        "'id': $states.context.Execution.Name}"
    )
    return _jsonata(f"$string({{'lease': {lease}, 'task': {task_expr}}})")


def lease_state_machine(
    *,
    image_arn: str,
    execution_role_arn: str,
    policy: LeasePolicy | None = None,
    region: str | None = None,
    name: str = "Lease",
    task_expr: str = "$states.input",
    heartbeat_s: int = 30,
) -> dict:
    """The JSONata ASL for one lease: Lease -> Terminate -> Done, with the failure
    path Lease -> Reap -> TerminateStale -> Failed.

    `region` defaults to the image ARN's region. `task_expr` is the JSONata expression
    that becomes the payload's `task` (pointers, not bodies: the payload is capped at
    4096 chars). The output of `Done` is the VM's success payload."""
    policy = policy or LeasePolicy()
    if region is None:
        parts = image_arn.split(":")
        region = parts[3] if image_arn.startswith("arn:") and len(parts) > 3 and parts[3] else "us-east-1"
    max_duration = policy.max_duration()
    stale_ms = max_duration * 1000
    items_expr = (
        f"[$states.input.Items[State in {STALE_STATES!r} and "
        f"$toMillis(StartedAt) < $toMillis($now()) - {stale_ms}]]"
    )
    return {
        "Comment": f"microvm-ctl lease of {image_arn.rsplit(':', 1)[-1]}: budget {policy.budget_s}s, "
                   f"VM cap {max_duration}s",
        "QueryLanguage": "JSONata",
        "StartAt": name,
        "States": {
            name: {
                "Type": "Task",
                "Resource": RUN_WAIT,
                "Arguments": {
                    "ImageIdentifier": image_arn,
                    "ExecutionRoleArn": execution_role_arn,
                    "RunHookPayload": run_hook_payload_expr(region, heartbeat_s, task_expr),
                    "IdlePolicy": _idle_policy_args(policy),
                    "MaximumDurationInSeconds": max_duration,
                    "ClientToken": _jsonata(
                        "$substring($states.context.Execution.Name & '-' & "
                        "$states.context.State.Name, 0, 128)"
                    ),
                },
                "TimeoutSeconds": policy.budget_s,
                "HeartbeatSeconds": policy.heartbeat_timeout_s,
                "Retry": _retry_throttling(),
                "Catch": [{
                    "ErrorEquals": ["States.Timeout", "States.HeartbeatTimeout", "States.TaskFailed"],
                    "Assign": {"lease_error": _jsonata("$states.errorOutput")},
                    "Next": "Reap",
                }],
                "Assign": {"vm": _jsonata("$states.result.microvm_id")},
                "Next": "Terminate",
            },
            "Terminate": {
                "Type": "Task",
                "Resource": TERMINATE,
                "Arguments": {"MicrovmIdentifier": _jsonata("$vm")},
                "Retry": _retry_throttling(),
                "Catch": [{"ErrorEquals": [NOT_FOUND], "Output": _jsonata("$states.input"), "Next": "Done"}],
                "Output": _jsonata("$states.input"),
                "Next": "Done",
            },
            "Reap": {
                "Type": "Task",
                "Resource": LIST,
                "Arguments": {"ImageIdentifier": image_arn},
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}],
                "Next": "TerminateStale",
            },
            "TerminateStale": {
                "Type": "Map",
                "Items": _jsonata(items_expr),
                "MaxConcurrency": 4,
                "ItemProcessor": {
                    "ProcessorConfig": {"Mode": "INLINE"},
                    "StartAt": "TerminateOne",
                    "States": {
                        "TerminateOne": {
                            "Type": "Task",
                            "Resource": TERMINATE,
                            "Arguments": {"MicrovmIdentifier": _jsonata("$states.input.MicrovmId")},
                            "Retry": _retry_throttling(),
                            "Catch": [{"ErrorEquals": [NOT_FOUND], "Next": "AlreadyGone"}],
                            "End": True,
                        },
                        "AlreadyGone": {"Type": "Pass", "End": True},
                    },
                },
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}],
                "Next": "Failed",
            },
            "Failed": {
                "Type": "Fail",
                "Error": "LeaseFailed",
                "Cause": _jsonata("$string($lease_error)"),
            },
            "Done": {"Type": "Succeed"},
        },
    }


# ---------------------------------------------------------------------------- IAM
VM_ACTIONS = {
    "sfn": ["states:SendTaskSuccess", "states:SendTaskFailure", "states:SendTaskHeartbeat"],
    "durable": [
        "lambda:SendDurableExecutionCallbackSuccess",
        "lambda:SendDurableExecutionCallbackFailure",
        "lambda:SendDurableExecutionCallbackHeartbeat",
    ],
    "sqs": ["sqs:SendMessage"],
    "eventbridge": ["events:PutEvents"],
    "http": [],
    "none": [],
}


def iam_statements(kind: str, *, orchestrator_arn: str | None = None) -> list[dict]:
    """Statements the VM's execution role needs to complete a lease of `kind`.

    `orchestrator_arn` is the state machine ARN (sfn), the durable function ARN
    (durable; ":*" is appended so version-qualified execution ARNs match), the queue
    ARN (sqs), or the event bus ARN (eventbridge). Without it the resource is "*"."""
    if kind not in VM_ACTIONS:
        raise ValueError(f"unknown lease kind {kind!r}; expected one of {sorted(VM_ACTIONS)}")
    actions = VM_ACTIONS[kind]
    if not actions:
        return []
    resource = orchestrator_arn or "*"
    if kind == "durable" and orchestrator_arn and not orchestrator_arn.endswith(":*"):
        resource = f"{orchestrator_arn}:*"
    return [{"Effect": "Allow", "Action": list(actions), "Resource": resource}]


NETWORK_CONNECTORS = "arn:aws:lambda:*:aws:network-connector:aws-network-connector:*"


def orchestrator_statements(execution_role_arn: str | None = None) -> list[dict]:
    """Statements the orchestrator (state machine role or durable function role) needs
    to launch, list, inspect, and terminate microVMs and to pass the VM's execution role."""
    return [
        {
            "Effect": "Allow",
            "Action": ["lambda:RunMicrovm", "lambda:TerminateMicrovm", "lambda:ListMicrovms",
                       "lambda:GetMicrovm"],
            "Resource": "*",
        },
        {"Effect": "Allow", "Action": "iam:PassRole", "Resource": execution_role_arn or "*"},
        # RunMicrovm passes the ingress and egress connectors, the AWS-managed defaults included
        {"Effect": "Allow", "Action": "lambda:PassNetworkConnector", "Resource": NETWORK_CONNECTORS},
    ]


def policy_document(statements: list[dict]) -> dict:
    return {"Version": "2012-10-17", "Statement": statements}
