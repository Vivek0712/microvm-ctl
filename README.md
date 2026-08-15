# microvm-ctl

**The control & execution plane for [AWS Lambda MicroVMs](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html).** Build snapshot images from a Dockerfile, spin up Firecracker microVMs in seconds, scale fleets up and down inside your real quotas, call into VMs over their authenticated endpoints, and watch the whole fleet live — from a Python SDK and a single `mvm` CLI.

```
pip install microvm-ctl
mvm bootstrap                              # S3 artifact bucket + build/execution IAM roles
mvm image build my-sandbox ./my-app        # Dockerfile at ./my-app root → runnable snapshot
mvm run my-sandbox --wait
mvm call <microvm-id> /execute -X POST -d '{"code":"print(2+2)"}'
mvm scale my-sandbox 10
mvm top --watch
```

Every number we publish is measured on the live service (us-east-1): **p50 3.5 s** from `RunMicrovm` to serving authenticated traffic, **111 ms** warm requests, suspend/resume with byte-for-byte state preservation (same PID), auto-resume in **0.7 s**, a fleet scaled 0→6 running VMs in **9.7 s**. Reproduce them with [`benchmarks/benchmark.py`](benchmarks/benchmark.py).

## What the service gives you — and what it doesn't

Lambda MicroVMs exposes the primitive under Lambda itself: a Firecracker VM with a full AL2023 userland, a dedicated HTTPS endpoint, and a controllable lifecycle (run → suspend → resume → terminate). It deliberately does **not** give you a load balancer (one endpoint per VM), a fleet abstraction, token management, monitoring, or generous default quotas. That's the gap this package fills:

| Concern | Module |
|---|---|
| Image factory: app dir → zip → S3 → build → ACTIVE version | `microvm/images.py` |
| Lifecycle + fleets: run, suspend, resume, terminate, `scale_to(n)`, drain, reap | `microvm/fleet.py` |
| **Quota-aware throttling** — reads your *applied* Service Quotas, not published defaults | `microvm/fleet.py`, `microvm/throttle.py` |
| Execution plane: JWE token mint/cache, `X-aws-proxy-auth`, 429 backoff, 502 auto-resume patience | `microvm/endpoint.py` |
| In-VM lifecycle hook server (zero dependencies, auto-injected into every image) | `microvm/hooks/server.py` |
| Live dashboard, CloudWatch tail, per-session cost model | `microvm/monitor.py` |
| One-time account setup: bucket + build/execution roles | `microvm/bootstrap.py` |

The `lambda-microvms` botocore service model ships inside the package (`microvm/data/`), so everything works on whatever boto3 you already have.

## The SDK in 20 lines

```python
from microvm import PlaneConfig, FleetManager, EndpointClient, Fleet
from microvm.fleet import IdlePolicy

cfg = PlaneConfig()                      # region/profile/roles from env
fm  = FleetManager(cfg)                  # throttled to your account's real TPS

vm = fm.run("my-sandbox",
            idle_policy=IdlePolicy(max_idle=300, suspended_for=3600, auto_resume=True),
            run_payload='{"tenant_id": "acme"}')

client = EndpointClient(cfg, vm.microvm_id)
print(client.post("/execute", json={"code": "print(41+1)"}).json())

fleet = Fleet(fm, "my-sandbox")
fleet.scale_to(20, wait_running=True)    # safe in one call — token-bucketed + jittered retry
fleet.suspend_all()                      # park the fleet: snapshot-storage billing only
fleet.drain()
```

Inside the VM, declare your lifecycle with the injected hook runtime — nothing to install:

```python
from microvm_hooks import HookApp
app = HookApp()

@app.on_ready
def ready(ctx): warm_caches(); return True      # 200 == "snapshot me now"

@app.on_run
def run(ctx): load_tenant(ctx.get("runHookPayload"))

@app.route("POST", "/execute")
def execute(body, headers): return 200, {"out": sandbox_exec(body["code"])}

app.serve(port=8080)
```

## Docs

- [Architecture](docs/architecture.md) — the two planes, lifecycle state machine, fleet design decisions, security model
- [The hook contract, in depth](docs/hooks.md) — `/ready`, `/validate`, `/run`, `/resume`, `/suspend`, `/terminate`, and the snapshot rules that bite everyone

## Examples & the blog series

Eight production-shaped apps built on this plane — code execution sandbox, AI code runner, agent-eval fleet, stateful notebook, sandboxed analytics, CI runners, HTML→PDF, multi-tenant agents — live in the companion repo **[awesome-microvm](https://github.com/vivekrajaps/awesome-microvm)**, each with a deep-dive blog post and a live terminal transcript.

## Requirements & regions

Python ≥ 3.9 on your machine; the service is ARM64-only inside the VM. Regions: `us-east-1`, `us-east-2`, `us-west-2`, `eu-west-1`, `ap-northeast-1`.

## License

Apache-2.0
