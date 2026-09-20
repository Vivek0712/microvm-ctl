# CLI reference

`mvm` is a thin layer over the SDK. Every command accepts `--region` and `--profile` before the subcommand; otherwise they come from `MVM_REGION` and `MVM_PROFILE` (or `AWS_PROFILE`).

```console
mvm --version
mvm --help
mvm <command> --help
```

## Account

### `mvm bootstrap [--prefix microvm-ctl]`

Creates the artifact bucket and the build and execution roles if they do not exist, refreshes their inline policies, and prints the three environment variables to export. Safe to re-run.

### `mvm quotas`

Prints the published default, the value applied to this account, and the rate the plane will throttle at, for `RunMicrovm`, `SuspendMicrovm`, `ResumeMicrovm`, `TerminateMicrovm`, and the total microVM memory quota. Needs `servicequotas:GetServiceQuota`; without it the applied column reads "unknown" and the plane falls back to published defaults.

## Images

### `mvm image build NAME DIR [--memory 2048] [--env K=V ...] [--caps-all] [--description TEXT]`

Zips `DIR` (which must contain a `Dockerfile` at its root), injects `microvm_hooks.py`, uploads to S3, creates the image or a new version of it, waits for the build, and marks the version ACTIVE. Prints build time and snapshot sizes.

| Flag | Meaning |
|---|---|
| `--memory` | minimum memory in MiB; vCPU count is fixed at memory / 2 GiB |
| `--env` | image-level environment variables, shared by every clone; never put per-VM values or secrets here |
| `--caps-all` | `additionalOsCapabilities: ["ALL"]`, needed for containerd, FUSE, and eBPF inside the VM |
| `--description` | free text stored on the image |

### `mvm image ls`

Lists images with state, active version, and creation time.

### `mvm image versions NAME`

Lists versions of one image with build state and status.

## Lifecycle

### `mvm run IMAGE [--version V] [-n COUNT] [--idle 300] [--suspended-ttl 3600] [--payload STRING] [--max-duration SECONDS] [--wait]`

Launches one or more microVMs from the image's active version.

| Flag | Meaning |
|---|---|
| `-n` | how many to launch; each goes through the token bucket |
| `--idle` | seconds without endpoint traffic before the service suspends the VM |
| `--suspended-ttl` | seconds a VM may stay suspended before the service terminates it |
| `--payload` | `runHookPayload`, delivered to the VM's `/run` hook; the only per-VM input at launch |
| `--max-duration` | hard lifetime cap in seconds; the service ceiling is 28,800 |
| `--wait` | poll until the VM reports RUNNING |

The execution role comes from `MVM_EXECUTION_ROLE_ARN`. Without one the VM has no AWS credentials, which fails loudly inside the VM rather than silently sharing anything.

### `mvm ls [--image NAME] [--all]`

Lists microVMs, hiding TERMINATED ones unless `--all`.

### `mvm get ID`

Prints the raw `GetMicrovm` response as JSON, including the endpoint hostname.

### `mvm suspend ID...`, `mvm resume ID...`, `mvm terminate ID...`

Lifecycle transitions, throttled to the applied quota.

### `mvm scale IMAGE N [--idle 300] [--wait]`

Converges the set of active (PENDING, RUNNING, SUSPENDING, SUSPENDED) microVMs of an image to `N`. Scale-up launches in parallel through the token bucket. Scale-down terminates SUSPENDED members first, then RUNNING members youngest first.

### `mvm drain IMAGE`

Terminates every active microVM of an image.

## Execution plane

### `mvm call ID PATH [-X METHOD] [-d DATA] [--port PORT]`

Mints a port-scoped token, sends one authenticated request to the VM's endpoint, and prints the status, elapsed time, and body. Non-default ports go in `X-aws-proxy-port`. A 502 during an auto-resume is retried for up to 30 seconds; a 429 backs off with jitter.

```console
mvm call microvm-abc /execute -X POST -d '{"code":"print(2+2)"}'
mvm call microvm-abc /metrics --port 9100
```

### `mvm status ID [--port PORT]`

Prints the hook runtime's job snapshot (`GET /status`): phase, elapsed time, progress, counters, the lease state, and the log tail. Works for any VM running `HookApp`, leased or not.

### `mvm watch ID [--port PORT] [--timeout 600]`

Streams `GET /events` into a live table of phase, progress, and heartbeats, printing each log line as it arrives. Stops when the lease is done or after `--timeout` seconds.

## Leases

### `mvm lease asl --image NAME [--execution-role ARN] [--budget 900] [--heartbeat 120] [--slack 120] [--heartbeat-every 30] [--name Lease] [--task-expr '$states.input']`

Prints the JSONata Step Functions state machine for one lease as JSON. A bare image name is resolved with the account from the execution role ARN, so this needs no credentials. See [Integrations](integrations.md).

### `mvm lease policy --kind sfn|durable|sqs|eventbridge|http [--orchestrator ARN] [--execution-role ARN]`

Prints two IAM policy documents: `execution_role` (what the VM needs to complete a lease of that kind against `--orchestrator`) and `orchestrator_role` (run, terminate, list, get, `iam:PassRole` on the execution role, and `lambda:PassNetworkConnector` on the connectors).

### `mvm lease run IMAGE --kind K [--token T] [--target X] [--task JSON] [--id LABEL] [--budget 900] [--heartbeat 120] [--slack 120] [--heartbeat-every 30] [--version V] [--execution-role ARN] [--wait]`

Launches one lease through `FleetManager.lease` for manual tests. `--token` is required for every kind except `none`; `--target` is the URL, queue URL, or bus name for `http`, `sqs`, and `eventbridge`. Prints the VM id and endpoint; follow it with `mvm watch`.

```console
mvm lease run handoff-agent --kind none --task '{"steps": ["echo hi"]}' --wait
```

### `mvm lease plan --image NAME --shards N [--baseline-mib M] [--max-concurrency N] [--max-vm-seconds S] [--approval-usd USD] [--json]`

Prints the fan-out plan for N leases of that image: concurrency and why (memory quota, policy, or default), waves, launch time to all running, worst-case VM-seconds and USD, whether approval is needed, and the rejection reason if any. Exit code 2 when rejected, 3 when approval is needed. Policy values come from `MVM_LEASE_*` and the flags.

### `mvm lease run IMAGE --shards N [--task-template JSON]`

Launches N leases at once after the plan check; `{i}` in the template's string values is replaced by the shard index. Refuses more shards than the concurrency limit.

### `mvm lease asl --map [--items-expr E] [--max-concurrency N] [--approval-topic ARN --approve-above-shards K]`

Emits the state machine with a `Map` over the shards, `MaxConcurrency` computed from the image and the applied quota when omitted, and an optional approval gate through SNS and a task token.

### `mvm watch --image NAME [--interval 2] [--timeout 600]`

One row per running member of the image with phase, progress, and elapsed time, and a footer with done over total and the slowest member. Exits when every member is done or gone.

## Observability

### `mvm top [--image NAME] [--watch] [--interval 3]`

A state-colored table of every microVM with counts per state. `--watch` refreshes in place.

### `mvm logs IMAGE [--minutes 15]`

Tails the CloudWatch log group `/aws/lambda-microvms/<image>`, falling back to the older `/aws/lambda/microvms/<image>` name. Build logs land here too, one stream per VM, which is where a failed Dockerfile step shows up.

### `mvm cost [--memory-gb 2] [--snapshot-gb SIZE] [--active 30] [--suspended 480] [--cycles 1]`

Prices a session shape with the published `us-east-1` rates: running compute, suspend cycles (snapshot write plus read), suspended storage, the total, the always-on equivalent, and the saving. Minutes for `--active` and `--suspended`.

```console
mvm cost --memory-gb 2 --snapshot-gb 0.61 --active 30 --suspended 480
```

## Playground

### `mvm playground [--host 127.0.0.1] [--port 8765] [--dry-run] [--no-open]`

Starts the local web app and opens a browser tab. Everything in this reference is reachable from it, parameterised, with live job logs and a trace of every AWS API call. See [The playground](playground.md).
