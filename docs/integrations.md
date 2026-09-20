# Integrations: the lease contract

A lease hands one microVM one task and lets the VM report completion itself. The orchestrator (Step Functions, a Lambda durable function, or any process you write) launches the VM with a `Lease` in `runHookPayload`; inside the VM the hook runtime runs your `@app.on_lease` handler, heartbeats while it works, and completes the lease through the matching API. No endpoint call, no auth token, no polling for RUNNING.

| Concern | Step Functions (`sfn`) | Durable functions (`durable`) | Generic (`http`, `sqs`, `eventbridge`) |
|---|---|---|---|
| token | task token | callback id | anything you mint |
| complete | `SendTaskSuccess` / `SendTaskFailure` | `SendDurableExecutionCallbackSuccess` / `Failure` | POST to a URL, SQS message, EventBridge event |
| keep alive | `SendTaskHeartbeat` against `HeartbeatSeconds` | `SendDurableExecutionCallbackHeartbeat` against `heartbeat_timeout` | optional status messages |
| hard cap | `TimeoutSeconds` = budget | `CallbackConfig.timeout` = budget | your timer |
| VM-side cap | `maximumDurationInSeconds` = budget + slack | same | same |
| idempotent launch | `ClientToken` from execution and state name | at-most-once step plus `clientToken` from the callback id | `clientToken` from the token |
| closed token | `TaskTimedOut` sets `lease.lost` | `CallbackTimeoutException` sets `lease.lost` | http 404 or 410; no signal for sqs and eventbridge |

## The payload

`runHookPayload` is capped at 4,096 characters, so `task` carries pointers (S3 keys, PR numbers), never bodies.

```json
{"lease": {"kind": "sfn", "token": "<opaque, up to 1024 chars>", "region": "us-east-1",
           "target": "<URL, queue URL, or bus name; http, sqs, eventbridge only>",
           "heartbeat_s": 30, "id": "<optional label, for example the execution name>"},
 "task": {"repo": "o/r", "pr": 7}}
```

Every completion the VM sends is a JSON object with `microvm_id`, `lease_id`, `elapsed_s`, and either `result` (success) or `error` with `error_type`, `message`, `retryable`, and `data` (failure). A success payload over 240 KB is replaced by `{"truncated": true, "summary": "<first 4 KB>"}`.

## Control plane

```python
from microvm import FleetManager, Lease, LeasePolicy, PlaneConfig

fm = FleetManager(PlaneConfig())
lease = Lease(kind="sqs", token="job-42", target="https://sqs.us-east-1.amazonaws.com/1/results", id="job-42")
policy = LeasePolicy(budget_s=900, heartbeat_timeout_s=120, slack_s=120)
vm = fm.lease("handoff-agent", lease, {"steps": ["make test"]}, policy)
```

`Lease.validate()` checks the kind, the token (required unless kind is `none`), and the target (required for `http`, `sqs`, `eventbridge`). `LeasePolicy.idle_policy()` is `IdlePolicy(max_idle=budget_s, suspended_for=60, auto_resume=False)`: a leased VM that goes idle is finished, not dormant. `LeasePolicy.max_duration()` is `budget_s + slack_s`, capped at 28,800. `FleetManager.lease` encodes the payload, applies both, and sets `clientToken = client_token(lease)`, so a replayed launch returns the same VM. A lease without a token (kind `none`) gets a fresh token on every call, since there is nothing single-use to key on. `microvm.lease` also exposes `encode_payload`, `decode_payload`, and `client_token`.

## Inside the VM

```python
from microvm_hooks import HookApp, LeaseError

app = HookApp()

@app.on_lease
def work(task, lease):
    lease.job.phase("clone")
    ...
    lease.check()                        # raises LeaseLost once the orchestrator stopped waiting
    if failed:
        raise LeaseError("StepFailed", "exit 1", retryable=False, data={"step": 2})
    return {"passed": True}              # becomes the success payload's "result"

app.serve(port=8080)
```

`/run` answers 200 immediately and the handler runs in a daemon thread. `lease` is a `LeaseContext` with `.task`, `.lease`, `.microvm_id`, `.job`, `.lost`, `.heartbeats`, and `.check()`. A `LeaseError` becomes a typed failure; any other exception becomes `Unexpected` with a short trace; `/terminate` mid-flight sends `Terminated` with `retryable: true`. Job telemetry (`app.job.phase`, `progress`, `log`, `counter`) is always on and served at `GET /status` and `GET /events`, lease or not. See [the hook contract](hooks.md).

## Completers and IAM

The completer is picked by `lease.kind`. boto3 is imported inside the completer, so it must be present in the image for `sfn`, `durable`, `sqs`, and `eventbridge`; `http` uses urllib only; `none` completes nothing and is observable through `/status`.

| kind | the VM's execution role needs | on |
|---|---|---|
| `sfn` | `states:SendTaskSuccess`, `states:SendTaskFailure`, `states:SendTaskHeartbeat` | the state machine ARN |
| `durable` | `lambda:SendDurableExecutionCallbackSuccess`, `Failure`, `Heartbeat` | `arn:...:function:<name>:*` |
| `sqs` | `sqs:SendMessage` | the queue ARN |
| `eventbridge` | `events:PutEvents` | the bus ARN |
| `http` | nothing | the token travels as `Authorization: Bearer` |

The orchestrator itself needs `lambda:RunMicrovm`, `lambda:TerminateMicrovm`, `lambda:ListMicrovms`, `lambda:GetMicrovm`, `iam:PassRole` on the execution role, and `lambda:PassNetworkConnector` on the connector ARNs (the AWS-managed defaults included). `mvm lease policy` prints both documents; `microvm.integrations.iam_statements(kind, orchestrator_arn=...)` and `orchestrator_statements(execution_role_arn)` return the statements.

## CLI

```console
mvm lease asl --image handoff-agent --execution-role arn:aws:iam::123456789012:role/agent --budget 900 > lease.asl.json
mvm lease policy --kind sfn --orchestrator arn:aws:states:us-east-1:123456789012:stateMachine:lease
mvm lease run handoff-agent --kind none --task '{"steps": ["echo hi"]}' --wait
mvm status <microvm-id>
mvm watch <microvm-id>
```

`mvm lease asl` prints the JSONata state machine described below; `--budget`, `--heartbeat`, `--slack`, and `--heartbeat-every` map to `LeasePolicy` and `Lease.heartbeat_s`. `mvm lease run` launches one lease by hand with `--kind`, `--token`, `--target`, and `--task`. `mvm status` prints the job snapshot and log tail; `mvm watch` streams `/events` into a live table.

## Step Functions

`microvm.integrations.lease_state_machine(image_arn=..., execution_role_arn=..., policy=LeasePolicy(), region=..., name="Lease", task_expr="$states.input")` returns the ASL dict. `Lease` is a `runMicrovm.waitForTaskToken` task whose `RunHookPayload` carries the task token, `TimeoutSeconds` is the budget and `HeartbeatSeconds` the heartbeat timeout, `ClientToken` comes from the execution and state names so a `Retry` never launches a second VM, and `Assign` keeps `$states.result.microvm_id` for `Terminate`. Timeouts and task failures are caught into `Reap` (`listMicrovms` by image) and `TerminateStale` (a `Map` that terminates members older than the VM cap), then `Failed` with error `LeaseFailed` and the caught error as the cause. `Done` outputs the VM's success payload. Standard workflows only: Express cannot wait for a token. Deployed example: [stepfunctions-handoff](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/stepfunctions-handoff).

## Lambda durable functions

`pip install microvm-ctl[durable]` (Python 3.11+). `microvm.integrations.durable.lease_microvm(context, fm, image, task, policy=LeasePolicy(), label="lease", version=None, execution_role=None)` creates the callback, launches through `FleetManager.lease` in an at-most-once step, waits on `callback.result()`, terminates in every branch, and returns `{"status": "done" | "timed_out" | "failed", "result" | "error", "retryable", "vm"}` without raising for those outcomes. `lease_with_relaunch(context, fm, image, task, max_relaunches=1, **kw)` retries while the outcome is retryable and adds `attempt`. The region comes from the execution ARN, or `MVM_REGION` under the local test runner. Deployed example: [durable-handoff](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/durable-handoff).

## Any other orchestrator

A laptop script, a CI job, or your own control plane uses `http`, `sqs`, or `eventbridge`: mint a token, call `FleetManager.lease`, and consume the heartbeat, success, and failure messages the VM sends to your target. There is no closed-token signal for `sqs` and `eventbridge`, so the VM's `maximumDurationInSeconds` is the guarantee. Example with all three kinds and a Function URL collector: [generic-handoff](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/generic-handoff). The shared agent image every example runs is [handoff-agent](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/handoff-agent).
