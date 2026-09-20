"""Step Functions integration: generate the lease state machine and its IAM.

One Standard-workflow task state leases a microVM with `runMicrovm.waitForTaskToken`,
the VM completes the task token itself (see `microvm.hooks.server` `on_lease` with
kind "sfn"), a `Terminate` state tidies up, and the `Catch` path reaps by image and
age because a timed-out lease carries no microVM id. Everything here is pure: no
boto3, no network, so the ASL can be generated in CI and committed.
"""

from __future__ import annotations

from dataclasses import dataclass

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
        # the launch quota is per second, so a short first wait recovers a throttled launch in
        # seconds; a 10 s interval dominated every fan-out we measured
        "IntervalSeconds": 2,
        "MaxAttempts": 8,
        "BackoffRate": 2,
        "JitterStrategy": "FULL",
    }]


def _idle_policy_args(policy: LeasePolicy) -> dict:
    api = policy.idle_policy().to_api()
    return {k[0].upper() + k[1:]: v for k, v in api.items()}


LEASE_ID_EXPR = "$states.context.Execution.Name"
# Inside a JSONata Map item processor the service exposes no item index, so the Map's `Items`
# expression wraps every shard as {'index': i, 'task': shard} and the states read those fields.
MAP_TASK_EXPR = "$states.input.task"
MAP_LEASE_ID_EXPR = "$states.context.Execution.Name & '-' & $string($states.input.index)"
CLIENT_TOKEN_EXPR = "$states.context.Execution.Name & '-' & $states.context.State.Name"
MAP_CLIENT_TOKEN_EXPR = CLIENT_TOKEN_EXPR + " & '-' & $string($states.input.index)"
MAP_DONE_NAME = "ShardDone"  # state names must be unique across the whole definition


def indexed_items_expr(items_expr: str) -> str:
    """JSONata that turns the shard array into [{'index': i, 'task': shard}, ...]."""
    return f"$map({items_expr}, function($v, $i) {{ {{'index': $i, 'task': $v}} }})"
SNS_WAIT = "arn:aws:states:::sns:publish.waitForTaskToken"
APPROVAL_TIMEOUT_S = 3600


@dataclass
class FanoutSpec:
    """Run one lease per element of a JSONata array with a Step Functions Map.

    `items_expr` yields the array of task objects (each becomes one payload `task`).
    `max_concurrency` is the Map's MaxConcurrency: the plane's honest number, from
    `FleetManager.fanout_limit`. With `approval_topic_arn` set, an execution whose
    array is longer than `approve_above_shards` (0 when omitted) first publishes to
    the topic with a task token and waits for `SendTaskSuccess` from an approver."""

    items_expr: str = "$states.input.shards"
    max_concurrency: int = 4
    approval_topic_arn: str | None = None
    approve_above_shards: int | None = None

    @property
    def approval(self) -> bool:
        return self.approval_topic_arn is not None


def run_hook_payload_expr(
    region: str, heartbeat_s: int, task_expr: str = "$states.input", lease_id_expr: str = LEASE_ID_EXPR,
) -> str:
    """The JSONata expression that builds the lease payload the VM decodes."""
    lease = (
        "{'kind': 'sfn', 'token': $states.context.Task.Token, "
        f"'region': '{region}', 'heartbeat_s': {int(heartbeat_s)}, "
        f"'id': {lease_id_expr}}}"
    )
    return _jsonata(f"$string({{'lease': {lease}, 'task': {task_expr}}})")


def _image_region(image_arn: str) -> str:
    parts = image_arn.split(":")
    return parts[3] if image_arn.startswith("arn:") and len(parts) > 3 and parts[3] else "us-east-1"


def _lease_states(
    *, image_arn: str, execution_role_arn: str, policy: LeasePolicy, region: str, name: str,
    task_expr: str, heartbeat_s: int, lease_id_expr: str, client_token_expr: str,
    done_name: str = "Done",
) -> dict:
    """The states of one lease: `name` -> Terminate -> Done, failures -> Reap ->
    TerminateStale -> Failed. Used as the whole machine and as a Map item processor."""
    max_duration = policy.max_duration()
    stale_ms = max_duration * 1000
    items_expr = (
        f"[$states.input.Items[State in {STALE_STATES!r} and "
        f"$toMillis(StartedAt) < $toMillis($now()) - {stale_ms}]]"
    )
    return {
        name: {
            "Type": "Task",
            "Resource": RUN_WAIT,
            "Arguments": {
                "ImageIdentifier": image_arn,
                "ExecutionRoleArn": execution_role_arn,
                "RunHookPayload": run_hook_payload_expr(region, heartbeat_s, task_expr, lease_id_expr),
                "IdlePolicy": _idle_policy_args(policy),
                "MaximumDurationInSeconds": max_duration,
                "ClientToken": _jsonata(f"$substring({client_token_expr}, 0, 128)"),
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
            "Catch": [{"ErrorEquals": [NOT_FOUND], "Output": _jsonata("$states.input"), "Next": done_name}],
            "Output": _jsonata("$states.input"),
            "Next": done_name,
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
        done_name: {"Type": "Succeed"},
    }


def approval_message_expr(items_expr: str, image_arn: str) -> str:
    """The SNS message: a JSON object with the shard count, the image, the execution,
    and the task token the approver passes to `SendTaskSuccess` (or `SendTaskFailure`)."""
    return _jsonata(
        "$string({'action': 'microvm-ctl lease fan-out approval', "
        f"'shards': $count({items_expr}), 'image': '{image_arn}', "
        "'execution': $states.context.Execution.Name, 'task_token': $states.context.Task.Token})"
    )


def _fanout_states(fanout: FanoutSpec, image_arn: str, processor_states: dict, name: str) -> dict:
    states = {
        "Fanout": {
            "Type": "Map",
            "Items": _jsonata(indexed_items_expr(fanout.items_expr)),
            "MaxConcurrency": int(fanout.max_concurrency),
            "ItemProcessor": {
                "ProcessorConfig": {"Mode": "INLINE"},
                "StartAt": name,
                "States": processor_states,
            },
            "Next": "Done",
        },
        "Done": {"Type": "Succeed"},
    }
    if not fanout.approval:
        return states
    threshold = int(fanout.approve_above_shards or 0)
    gate = {
        "Gate": {
            "Type": "Choice",
            "Choices": [{
                "Condition": _jsonata(f"$count({fanout.items_expr}) > {threshold}"),
                "Next": "RequestApproval",
            }],
            "Default": "Fanout",
        },
        "RequestApproval": {
            "Type": "Task",
            "Resource": SNS_WAIT,
            "Arguments": {
                "TopicArn": fanout.approval_topic_arn,
                "Message": approval_message_expr(fanout.items_expr, image_arn),
            },
            "TimeoutSeconds": APPROVAL_TIMEOUT_S,
            "Catch": [{"ErrorEquals": ["States.Timeout", "States.TaskFailed"], "Next": "Denied"}],
            "Output": _jsonata("$states.input"),
            "Next": "Fanout",
        },
        "Denied": {
            "Type": "Fail",
            "Error": "ApprovalDenied",
            "Cause": f"no approval within {APPROVAL_TIMEOUT_S}s, or the approver sent a task failure",
        },
    }
    gate.update(states)
    return gate


def lease_state_machine(
    *,
    image_arn: str,
    execution_role_arn: str,
    policy: LeasePolicy | None = None,
    region: str | None = None,
    name: str = "Lease",
    task_expr: str = "$states.input",
    heartbeat_s: int = 30,
    fanout: FanoutSpec | None = None,
) -> dict:
    """The JSONata ASL for one lease: Lease -> Terminate -> Done, with the failure
    path Lease -> Reap -> TerminateStale -> Failed.

    `region` defaults to the image ARN's region. `task_expr` is the JSONata expression
    that becomes the payload's `task` (pointers, not bodies: the payload is capped at
    4096 chars). The output of `Done` is the VM's success payload.

    With `fanout`, the same lease flow becomes the item processor of a `Fanout` Map
    over `fanout.items_expr` (each item is the payload `task`; `task_expr` is ignored),
    the lease id and clientToken carry the item index, and `Done` outputs the array of
    VM payloads. When the spec has an approval topic, a `Gate` Choice sends executions
    above `approve_above_shards` through `RequestApproval` (SNS publish with a task
    token) first; a timeout or a task failure ends in `Denied`."""
    policy = policy or LeasePolicy()
    if region is None:
        region = _image_region(image_arn)
    max_duration = policy.max_duration()
    image_name = image_arn.rsplit(':', 1)[-1]
    if fanout is None:
        return {
            "Comment": f"microvm-ctl lease of {image_name}: budget {policy.budget_s}s, "
                       f"VM cap {max_duration}s",
            "QueryLanguage": "JSONata",
            "StartAt": name,
            "States": _lease_states(
                image_arn=image_arn, execution_role_arn=execution_role_arn, policy=policy, region=region,
                name=name, task_expr=task_expr, heartbeat_s=heartbeat_s,
                lease_id_expr=LEASE_ID_EXPR, client_token_expr=CLIENT_TOKEN_EXPR,
            ),
        }
    processor = _lease_states(
        image_arn=image_arn, execution_role_arn=execution_role_arn, policy=policy, region=region,
        name=name, task_expr=MAP_TASK_EXPR, heartbeat_s=heartbeat_s,
        lease_id_expr=MAP_LEASE_ID_EXPR, client_token_expr=MAP_CLIENT_TOKEN_EXPR, done_name=MAP_DONE_NAME,
    )
    comment = (f"microvm-ctl lease fan-out of {image_name}: {fanout.max_concurrency} at a time, "
               f"budget {policy.budget_s}s, VM cap {max_duration}s")
    if fanout.approval:
        comment += f", approval above {int(fanout.approve_above_shards or 0)} shards"
    return {
        "Comment": comment,
        "QueryLanguage": "JSONata",
        "StartAt": "Gate" if fanout.approval else "Fanout",
        "States": _fanout_states(fanout, image_arn, processor, name),
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
