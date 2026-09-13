# Architecture

![architecture](img/architecture.png)

## The two planes

The **control plane** talks to the `lambda-microvms` API over SigV4. It owns images, lifecycle, fleets, tokens, quotas, and cost attribution. It runs wherever your orchestration runs: a laptop, a CI job, or a Lambda function of your own.

The **execution plane** talks HTTPS to each microVM's dedicated endpoint, `<id>.lambda-microvm.<region>.on.aws`. It owns request authentication (a JWE token in `X-aws-proxy-auth`), port selection (`X-aws-proxy-port`, default 8080), retries, and the hook server inside the VM.

Nothing in the execution plane holds AWS credentials beyond what token minting needs, and nothing in the control plane touches workload data. The split is what lets a compromised sandbox stay a compromised sandbox.

## What one request looks like

1. Your code or the CLI uploads a zip and calls `CreateMicrovmImage`. Lambda builds a VM from your Dockerfile, starts your ENTRYPOINT, polls `GET /ready` until it returns 200, snapshots memory and disk, boots a fresh clone, and calls `POST /validate`.
2. `RunMicrovm(image)` restores the snapshot and calls `POST /run` with `{"microvmId": ..., "runHookPayload": ...}`. When your app returns 200 the VM is RUNNING and its endpoint is live.
3. `CreateMicrovmAuthToken(ports, ttl)` returns the header map. The client caches it until 80% of the TTL.
4. Your request goes to the endpoint with `X-aws-proxy-auth` and reaches your route.
5. After `maxIdleDurationSeconds` without traffic the service calls `POST /suspend`, snapshots the VM, and stops compute billing.
6. The next request (with `autoResumeEnabled`) is held while the service restores the VM and calls `POST /resume`. The client sees a 502 only if the restore fails; otherwise it sees the reply.

## Lifecycle state machine

![lifecycle](img/lifecycle.png)

Suspension is triggered by the idle policy or by an explicit `SuspendMicrovm`. Wake is triggered by `ResumeMicrovm` or by any endpoint request when auto-resume is on. Termination is triggered by an explicit call, by `suspendedDurationSeconds` elapsing while suspended, by `maximumDurationInSeconds`, or by the 8 hour ceiling that applies across RUNNING and SUSPENDED time combined.

Billing by state: RUNNING is vCPU plus memory per second. SUSPENDED is snapshot storage only. TERMINATED is zero, although the memory quota is released with a lag of up to a few minutes.

## Fleet design decisions

**There is no service-side load balancer.** One endpoint per VM means horizontal scale is more `RunMicrovm` calls, and routing across the fleet is the control plane's job. `Fleet` and `EndpointClient` are the primitives; the eval harness in the companion repo shows round-robin over N clients.

**Throttle to the applied quota, not the published one.** New accounts run reduced profiles. Our fresh account had `RunMicrovm` at 1 per second and 8 GB of total memory against published defaults of 5 per second and 1,024 GB. `FleetManager` reads the applied values from Service Quotas at startup and runs every mutating call through a token bucket at 80% of them, with jittered exponential backoff behind the bucket.

**Memory quota is the real ceiling, and it counts more than you think.** RUNNING, SUSPENDED, TERMINATING, and image-build VMs all count. A fleet that suspends instead of terminating still holds quota, so `scale_to` terminates suspended members first on the way down. The benchmark harness waits for terminations to settle before the scale section for the same reason.

**Every launch carries a duration cap.** Runaway spend is a launch-time configuration problem. `maximumDurationInSeconds` and `suspendedDurationSeconds` are set on every `run`, and `Fleet.reap()` backstops both.

**The image is a photocopy.** Every VM is a byte-for-byte restore of one snapshot, including RNG state, open sockets, and anything in memory at `/ready` time. Per-VM uniqueness and credentials are re-established in `/run`, never at build time. The hook runtime reseeds Python's RNG on every `/run` so at least that one is handled for you.

## Security model

- There is no unauthenticated path to a VM. Every request needs a port-scoped, expiring token minted through the IAM-authenticated `CreateMicrovmAuthToken`. `EndpointClient` scopes tokens to `[8080]` by default rather than all ports.
- Build role and execution role are separate. The sandbox's role never reads your artifact bucket; the build role never gets workload permissions.
- Secrets never go in the image. Environment variables and snapshot memory are both cloned into every VM. Per-VM context travels in `runHookPayload`; secrets are fetched in `/run` with the execution role.
- Ingress connectors: `ALL_INGRESS` (default), `NO_INGRESS` for push-only workloads, `SHELL_INGRESS` for interactive shells, which should be gated. Egress is `INTERNET_EGRESS` or a VPC network connector when the code inside should not be able to call home.

## Module map

| Module | Responsibility |
|---|---|
| `microvm/config.py` | `PlaneConfig`, region list, connector and base image ARNs, published TPS defaults, the 8 h ceiling |
| `microvm/client.py` | boto3 session factory with the vendored service model, account id and image ARN helpers |
| `microvm/throttle.py` | `TokenBucket` and `Throttled`, the retry policy for `ThrottlingException`, `TooManyRequestsException`, `ServiceQuotaExceededException` |
| `microvm/bootstrap.py` | artifact bucket and the two IAM roles |
| `microvm/images.py` | packaging, upload, create or update, build polling, ACTIVE promotion |
| `microvm/fleet.py` | `Microvm`, `IdlePolicy`, `FleetManager`, `Fleet`, applied quota lookup |
| `microvm/endpoint.py` | `EndpointClient`: token cache, retries, `wait_ready`, shell tokens |
| `microvm/hooks/server.py` | the zero-dependency `HookApp` injected into every image as `microvm_hooks.py` |
| `microvm/monitor.py` | `FleetMonitor`, `CostModel`, published rates |
| `microvm/cli.py` | the `mvm` command |
| `benchmarks/benchmark.py` | the measurement protocol behind every number in the docs |
