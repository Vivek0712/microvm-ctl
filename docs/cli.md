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

## Observability

### `mvm top [--image NAME] [--watch] [--interval 3]`

A state-colored table of every microVM with counts per state. `--watch` refreshes in place.

### `mvm logs IMAGE [--minutes 15]`

Tails the CloudWatch log group `/aws/lambda/microvms/<image>`. Build logs land here too, one stream per VM, which is where a failed Dockerfile step shows up.

### `mvm cost [--memory-gb 2] [--snapshot-gb SIZE] [--active 30] [--suspended 480] [--cycles 1]`

Prices a session shape with the published `us-east-1` rates: running compute, suspend cycles (snapshot write plus read), suspended storage, the total, the always-on equivalent, and the saving. Minutes for `--active` and `--suspended`.

```console
mvm cost --memory-gb 2 --snapshot-gb 0.61 --active 30 --suspended 480
```
