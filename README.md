# microvm-ctl

**The control and execution plane for [AWS Lambda MicroVMs](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html).** Build a snapshot image from a Dockerfile, launch Firecracker microVMs in seconds, scale a fleet inside the quotas your account actually has, call into every VM over its authenticated endpoint, watch the whole thing live, and hand a VM a task from Step Functions, Lambda durable functions, or any orchestrator through a lease the VM completes itself. One Python SDK, one `mvm` command.

[![ci](https://github.com/Vivek0712/microvm-ctl/actions/workflows/ci.yml/badge.svg)](https://github.com/Vivek0712/microvm-ctl/actions/workflows/ci.yml)
[![pypi](https://img.shields.io/pypi/v/microvm-ctl)](https://pypi.org/project/microvm-ctl/)
![python](https://img.shields.io/badge/python-3.9%20to%203.13-blue)
![license](https://img.shields.io/badge/license-Apache--2.0-green)

```console
pip install microvm-ctl          # https://pypi.org/project/microvm-ctl/

mvm bootstrap                              # one time: S3 artifact bucket + build/execution IAM roles
mvm image build my-sandbox ./my-app        # Dockerfile at ./my-app root -> runnable snapshot
mvm run my-sandbox --wait                  # RunMicrovm, then poll to RUNNING
mvm call <microvm-id> /execute -X POST -d '{"code":"print(2+2)"}'
mvm scale my-sandbox 10                    # converge the fleet, throttled to your applied quota
mvm top --watch                            # live state table
```

To see it used for real first, jump to [the eight examples](#see-it-working-eight-examples-and-the-article-series).

## Why this exists

Lambda MicroVMs exposes the primitive under Lambda itself: a Firecracker VM with a full AL2023 userland, a dedicated HTTPS endpoint, and a lifecycle you control (run, suspend, resume, terminate). The service deliberately stops there. There is no load balancer (one endpoint per VM), no fleet abstraction, no token management, no dashboard, and a fresh account runs quotas far below the published defaults. microvm-ctl fills that gap.

| Concern | What the plane does | Module |
|---|---|---|
| Image factory | app directory to zip, to S3, to `CreateMicrovmImage`, to an ACTIVE version; injects the hook runtime into every image | `microvm/images.py` |
| Lifecycle and fleets | run, suspend, resume, terminate, `Fleet.scale_to(n)`, drain, reap | `microvm/fleet.py` |
| Quota-aware throttling | reads the quotas *applied* to your account, not the published defaults, and token-buckets every mutating call at 80% of them | `microvm/fleet.py`, `microvm/throttle.py` |
| Execution plane | mints and caches port-scoped JWE tokens, sets `X-aws-proxy-auth`, backs off on 429, waits out a 502 while a suspended VM auto-resumes | `microvm/endpoint.py` |
| In-VM hook runtime | zero-dependency server for `/ready`, `/validate`, `/run`, `/resume`, `/suspend`, `/terminate` plus your own routes | `microvm/hooks/server.py` |
| Observability and cost | live fleet table, CloudWatch tail, a cost model that prices a session shape before you commit to it | `microvm/monitor.py` |
| Account bootstrap | artifact bucket plus separate build and execution roles | `microvm/bootstrap.py` |
| Leases at scale | `LeasePolicy` ceilings, `plan` with the honest concurrency and worst-case cost, `lease_many`, `mvm lease asl --map`, `lease_map`, `mvm watch --image` | `microvm/lease.py`, `microvm/integrations/` |
| Leases | `Lease`, `LeasePolicy`, `FleetManager.lease`: one task per VM, completed by the VM through a task token, callback id, HTTP, SQS, or EventBridge | `microvm/lease.py` |
| Orchestrator integrations | generated Step Functions state machine and IAM (`mvm lease asl`, `mvm lease policy`), `lease_microvm` for Lambda durable functions | `microvm/integrations/` |

The `lambda-microvms` botocore service model ships inside the package, so the plane works on whatever boto3 you already have.

![architecture](docs/img/architecture.png)

## Measured, not quoted

Every number in this repo comes from the live service in `us-east-1`. The harness that produced them is [`benchmarks/benchmark.py`](benchmarks/benchmark.py); run it against your own account.

| What | Measured |
|---|---|
| Image build, Dockerfile to runnable snapshot | 123 to 145 s |
| `RunMicrovm` to serving authenticated traffic | **p50 3.54 s**, p95 4.49 s |
| Warm authenticated request (real Python execution inside the VM) | **p50 111 ms** |
| Explicit suspend / resume | 2.5 s / 2.6 s, same PID, state intact |
| First request to a suspended VM (auto-resume) | **200 OK in 0.7 s** |
| Fleet scale-out, 0 to 6 running VMs, on a 1 launch/s quota | **9.7 s** wall |
| 30 min active + 8 h suspended session vs always-on | **93.8% cheaper** |

![benchmark transcript](docs/img/benchmark.png)

## The SDK in 20 lines

```python
from microvm import PlaneConfig, FleetManager, EndpointClient, Fleet
from microvm.fleet import IdlePolicy

cfg = PlaneConfig()                      # region, profile, bucket, roles from MVM_* env vars
fm = FleetManager(cfg)                   # throttled to your account's applied TPS

vm = fm.run("my-sandbox",
            idle_policy=IdlePolicy(max_idle=300, suspended_for=3600, auto_resume=True),
            run_payload='{"tenant_id": "acme"}')

client = EndpointClient(cfg, vm.microvm_id)
print(client.post("/execute", json={"code": "print(41+1)"}).json())

fleet = Fleet(fm, "my-sandbox")
fleet.scale_to(20, wait_running=True)    # one call, token-bucketed, jittered retries
fleet.suspend_all()                      # park the fleet: snapshot storage billing only
fleet.drain()                            # terminate every member
```

Inside the VM, declare the lifecycle with the injected hook runtime. There is nothing to install in the image.

```python
from microvm_hooks import HookApp
app = HookApp()

@app.on_ready
def ready(ctx):
    warm_caches()
    return True                          # 200 means "snapshot me now"

@app.on_run
def run(ctx):                            # every clone, before traffic; RNG is reseeded for you
    load_tenant(ctx.get("runHookPayload"))

@app.route("POST", "/execute")
def execute(body, headers):
    return 200, {"out": sandbox_exec(body["code"])}

app.serve(port=8080)
```

## What the CLI looks like

`mvm quotas` shows the difference between the published defaults and what your account can actually do. This is a fresh account: one launch per second and 8 GB of total microVM memory.

![mvm quotas](docs/img/mvm-quotas.png)

`mvm image ls` after building the eight example images from the companion repo.

![mvm image ls](docs/img/mvm-image-ls.png)

`mvm cost` prices a session shape from the published rates. This is the 30 minutes active plus 8 hours suspended shape on a 2 GB VM with the measured 0.61 GB snapshot.

![mvm cost](docs/img/mvm-cost.png)

## The playground

`mvm playground` opens a local web app that drives everything above against the live service: build images, run and scale fleets, call VMs, tail logs, price sessions, run a parameterised benchmark, and watch every AWS API call the process makes in a trace. A dry-run switch turns each mutating action into a printout of the exact request it would send. See [docs/playground.md](docs/playground.md).

```console
mvm playground            # http://127.0.0.1:8765
```

## Documentation

- [Quickstart](docs/quickstart.md): from an empty account to a serving VM, with the environment variables explained.
- [CLI reference](docs/cli.md): every `mvm` command and flag.
- [SDK reference](docs/sdk.md): `PlaneConfig`, `ImageBuilder`, `FleetManager`, `Fleet`, `EndpointClient`, `FleetMonitor`, `CostModel`.
- [Architecture](docs/architecture.md): the two planes, the lifecycle state machine, the fleet design decisions, and the security model.
- [The hook contract](docs/hooks.md): what each hook is for and what breaks when you ignore it.
- [Quotas and cost](docs/quotas-and-cost.md): the quota walls, what counts against them, and the cost model with worked examples.
- [Troubleshooting](docs/troubleshooting.md): the errors I hit on the live service and what each one meant.
- [The playground](docs/playground.md): the local web app over the whole SDK, and how to host it.
- [Integrations](docs/integrations.md): the lease contract for handing a VM to Step Functions, Lambda durable functions, or your own orchestrator, and what the plane should own.

## See it working: thirteen examples and the article series

The fastest way to understand the plane is to read the apps built on it. The companion repo [awesome-microvm](https://github.com/Vivek0712/awesome-microvm) holds thirteen production-shaped examples, each a Dockerfile plus a single-file app, deployed and recorded on the live service:

| Example | What it shows |
|---|---|
| [code-sandbox](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/code-sandbox) | untrusted or AI-written Python per session; state persists across calls and suspend |
| [ai-code-runner](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/ai-code-runner) | Bedrock writes code, the VM runs it, tracebacks drive a self-repair loop |
| [agent-eval](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/agent-eval) | `Fleet.scale_to` fan-out over byte-identical clones, scoreboard, drain |
| [notebook](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/notebook) | a kernel whose namespace survives suspend and resume with the same PID |
| [data-analytics](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/data-analytics) | DuckDB over S3 through the execution role; bulk data off the endpoint |
| [ci-runner](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/ci-runner) | clone, test, report, terminate; `--max-duration` as the runaway cap |
| [pdf-service](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/pdf-service) | untrusted HTML rendered in the VM; idle policy sleeps it between bursts |
| [multi-tenant-agents](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/multi-tenant-agents) | one VM per tenant, identity via `runHookPayload`, `run_payload_factory` on a `Fleet` |
| [handoff-agent](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/handoff-agent) | the one image every orchestrator leases: `on_lease`, phases, progress, heartbeats, typed failures, in-VM parallel steps |
| [stepfunctions-handoff](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/stepfunctions-handoff) | `runMicrovm.waitForTaskToken` machines from `mvm lease asl`, single lease and a governed Map fan-out |
| [durable-handoff](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/durable-handoff) | a Lambda durable function leases a VM with `lease_with_relaunch` and fans out with `lease_map` |
| [generic-handoff](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/generic-handoff) | any controller: the same lease over SQS, EventBridge, or an HTTP collector |
| [circuit-breaker](https://github.com/Vivek0712/awesome-microvm/tree/main/examples/circuit-breaker) | running-memory metric, alarm, and `Fleet.drain` for every image: the account-wide stop behind `LeasePolicy` |

The four-part article series **Building on AWS Lambda MicroVMs** on the AWS Builder Center walks through them:

1. [Control and scale AWS Lambda MicroVMs with microvm-ctl](https://builder.aws.com/content/3JIDTpz0ZgatSBv24drra3gEod9/control-and-scale-aws-lambda-microvms-with-microvm-ctl): this plane and its measurements.
2. [Seven workloads Lambda could never run, until MicroVMs](https://builder.aws.com/content/3JJ2oNWY9EsZzivMMx044cSlrFQ/seven-workloads-lambda-could-never-run-until-microvms): the first seven examples through build, run, cost, and gotchas.
3. [A kernel for every customer: scaling AI agents to 1,000 tenants on AWS Lambda MicroVMs with microvm-ctl](https://builder.aws.com/content/3JJ7tASPSSUUTrcpWnWtxOuu8g3/a-kernel-for-every-customer-scaling-ai-agents-to-1000-tenants-on-aws-lambda-microvms-with-microvm-ctl): the multi-tenant finale with the decision guide.
4. Hand a task to a MicroVM from anywhere (part 4, [source in awesome-microvm](https://github.com/Vivek0712/awesome-microvm/blob/main/blog/03-handoff.md), Builder Center link to follow): the lease contract, Step Functions, durable functions, your own controller, and leases at scale.

The article sources and a long-form deep dive per example live under [awesome-microvm/blog](https://github.com/Vivek0712/awesome-microvm/tree/main/blog).

Every lease scenario also runs live, on Step Functions and on a Lambda durable function, in [microvm-handoff-demo](https://github.com/Vivek0712/microvm-handoff-demo): nine scenarios on both orchestrators plus one lease from the CLI, with the execution histories, VM and orchestrator logs, benchmarks, and console screenshots checked in. Its first pass ran on 0.3.0 and found the two things that became 0.3.1: a typed failure whose VM Step Functions never terminated, and a 30 s heartbeat interval against a 30 s heartbeat timeout that lost a durable callback before the first heartbeat landed.

## Requirements and regions

Python 3.9 or newer on the machine running the plane. The VMs themselves are ARM64 (Graviton) only, so audit binary wheels before you build an image. The service is available in `us-east-1`, `us-east-2`, `us-west-2`, `eu-west-1`, and `ap-northeast-1`.

## Credits and inspiration

This project grew out of [lambda-microvm-starter](https://github.com/vidanov/lambda-microvm-starter) by [Alexey Vidanov](https://github.com/vidanov), the one-command on-ramp that deploys any Dockerfile to a Lambda MicroVM behind a public CloudFront URL. His starter kit and its troubleshooting notes were the first working map of the service I had, and several of the gotchas documented here were first written down there. microvm-ctl takes the next step from one deployed app to fleets, tokens, quotas, and cost, and I am grateful for the ground he covered first.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the local test loop (no AWS account needed for the unit tests) and the conventions used in this repo.

## License

Apache-2.0
