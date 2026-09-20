# SDK reference

Everything the CLI does is available from Python. Import from the top-level package:

```python
from microvm import (
    PlaneConfig, ImageBuilder, ImageBuildError,
    FleetManager, Fleet, EndpointClient, EndpointError,
    FleetMonitor, CostModel, microvm_client, lambda_client,
)
from microvm.fleet import IdlePolicy, Microvm, applied_quotas
```

## PlaneConfig

A dataclass holding everything one deployment needs. Every field reads an environment variable by default.

| Field | Env var | Default |
|---|---|---|
| `region` | `MVM_REGION` | `us-east-1` |
| `profile` | `MVM_PROFILE`, then `AWS_PROFILE` | none |
| `artifact_bucket` | `MVM_ARTIFACT_BUCKET` | none (required to build) |
| `build_role_arn` | `MVM_BUILD_ROLE_ARN` | none (required to build) |
| `execution_role_arn` | `MVM_EXECUTION_ROLE_ARN` | none (VMs get no AWS credentials) |

Unsupported regions raise `ValueError` at construction. Properties `base_image_arn`, `ingress_all`, `ingress_none`, `ingress_shell`, and `egress_internet` give the per-region ARNs of the managed base image and network connectors.

## ImageBuilder

```python
builder = ImageBuilder(cfg)
built = builder.build("my-sandbox", "./my-app", memory_mib=2048,
                      environment={"MODEL_ID": "amazon.nova-lite-v1:0"},
                      egress_connectors=None,          # default: INTERNET_EGRESS
                      os_capabilities_all=False, wait=True)
built.version, built.build_seconds, built.memory_snapshot_bytes, built.disk_snapshot_bytes
```

`build` packages the directory (skipping `.git`, `__pycache__`, `.venv`), injects `microvm_hooks.py` unless the app ships its own, uploads to `s3://<bucket>/microvm-images/<name>/<timestamp>.zip`, then calls `CreateMicrovmImage` for a new name or `UpdateMicrovmImage` for an existing one. With `wait=True` it polls the build every 10 seconds, raises `ImageBuildError` with a CloudWatch pointer on failure, and marks the version ACTIVE.

`hooks` defaults to `default_hooks(port=8080)`: all six hooks enabled, `/ready` timeout 300 s, `/validate` 120 s, `/run` and `/resume` 60 s, `/suspend` and `/terminate` 30 s. Pass your own dict to change ports or timeouts.

Helpers: `package(app_dir) -> bytes`, `upload(name, payload) -> s3 uri`, `list_images()`, `list_versions(name)`, `arn(name)`.

## FleetManager

One instance per account and region. On construction it reads the applied quotas from Service Quotas (best effort) and builds a token bucket per mutating API at `QUOTA_HEADROOM` (0.8) of the applied rate, falling back to the published defaults when Service Quotas does not answer.

```python
fm = FleetManager(cfg)                    # quota_aware=False skips the Service Quotas lookup
fm.tps("RunMicrovm")                      # effective launches per second
fm.memory_quota_gb                        # applied memory ceiling, or None

vm = fm.run("my-sandbox", version=None, idle_policy=IdlePolicy(), run_payload=None,
            max_duration=None, ingress=None, egress=None, execution_role=None,
            client_token=None)                # 1 to 128 chars; a replayed launch with the same token returns the same VM
fm.run_params("my-sandbox", ...)            # the exact RunMicrovm request, for logging or dry runs
fm.get(vm.microvm_id)                     # Microvm(microvm_id, state, image_arn, image_version, started_at, endpoint)
fm.wait_until(vm.microvm_id, "RUNNING", timeout=120)
fm.suspend(vm.microvm_id); fm.resume(vm.microvm_id); fm.terminate(vm.microvm_id)
fm.list(image="my-sandbox", version=None)
```

`wait_until` raises `RuntimeError` if the VM terminates while you wait for another state, and `TimeoutError` on timeout.

### IdlePolicy

```python
IdlePolicy(max_idle=300, suspended_for=3600, auto_resume=True)
```

Maps to `maxIdleDurationSeconds`, `suspendedDurationSeconds`, and `autoResumeEnabled`. Idle detection keys off endpoint traffic only. `suspended_for` is an auto-terminate timer, so size it to the longest absence you want to survive.

### Leases at scale

```python
from microvm import Lease, LeasePolicy
policy = LeasePolicy.from_env()                       # MVM_LEASE_* ceilings, platform owned
limit = fm.fanout_limit(2048, policy)                 # how many 2 GB VMs at once, and why
plan = fm.plan(8, 2048, policy)                       # concurrency, waves, launch time, worst-case VM-s and USD
plan.check()                                          # LeasePlanRejected before anything launches
vms = fm.lease_many("handoff-agent", leases, tasks, policy, baseline_mib=2048)
```

`plan_fanout`, `LeasePlan`, `FanoutLimit`, and `LeasePlanRejected` live in `microvm.lease` and are pure; `ImageBuilder.baseline_mib(name)` reads an image's baseline. `FleetMonitor.job_status(image)` returns every running member's `/status` snapshot for `mvm watch --image` and the playground.

## Fleet

A declarative set of microVMs from one image.

```python
fleet = Fleet(fm, "my-sandbox", version=None,
              idle_policy=IdlePolicy(max_idle=600, suspended_for=7200),
              max_duration=3600,
              run_payload_factory=lambda i: f'{{"shard": {i}}}',   # per-VM payload by launch index
              ingress=None, egress=None, execution_role=None)

fleet.members()                # active members (PENDING, RUNNING, SUSPENDING, SUSPENDED)
fleet.size()
fleet.scale_to(20, wait_running=True)   # returns the launched Microvm list on scale-up
fleet.suspend_all(); fleet.resume_all(); fleet.drain()
fleet.reap(max_age_seconds=7200)        # terminate members older than the cap
Fleet.scale_down_victims(members, count)   # the selection rule, exposed for tests and tooling
```

Scale-up fans out on an 8-thread pool; each launch still passes through the shared token bucket. Scale-down picks SUSPENDED members first (they cost only storage but still hold memory quota), then RUNNING members youngest first, so the oldest and warmest survive.

## EndpointClient

```python
client = EndpointClient(cfg, vm.microvm_id, endpoint=None,   # endpoint discovered via GetMicrovm if omitted
                        ports=[8080], all_ports=False, token_ttl_minutes=15)
resp = client.post("/execute", json={"code": "print(1)"}, timeout=60,
                   max_attempts=6, resume_patience=30)
client.get("/state", port=9100)
client.wait_ready("/healthz", timeout=90)   # seconds to first 200
client.shell_token(minutes=15)              # for SHELL_INGRESS VMs
client.status(since=None)                   # the hook runtime's job snapshot (GET /status)
for event in client.watch(timeout=600):     # parsed JSON from GET /events (SSE): log lines and snapshots
    ...
```

Tokens are minted lazily with `CreateMicrovmAuthToken`, scoped to `ports` (or all ports), and cached until 80% of their TTL. Per request: 429 backs off with jitter up to 8 s; 502 is retried every 2 s for `resume_patience` seconds because the first request to a suspended VM pays the resume; 403 re-mints the token once and retries with the caller's headers intact. Anything else is returned to you as a normal `requests.Response`.

`status()` returns the `HookApp` job snapshot: `phase`, `elapsed_s`, `progress`, `counters`, the last 50 log lines, `lease` (or `None`), `microvm_id`, and `seq`; `since=<seq>` returns only newer log lines. `watch()` streams `/events` and yields each log line and each periodic snapshot as a dict, stopping when a snapshot reports the lease done, after `timeout` seconds, or when the VM closes the stream.

### Leases

```python
from microvm import Lease, LeasePolicy

lease = Lease(kind="sfn", token=task_token, region="us-east-1", target=None, heartbeat_s=30, id="exec-1")
policy = LeasePolicy(budget_s=900, heartbeat_timeout_s=120, slack_s=120)
vm = fm.lease("handoff-agent", lease, {"pr": 7}, policy, version=None, execution_role=None,
              ingress=None, egress=None)
```

`fm.lease` is `fm.run` with the lease encoded into `run_payload`, `policy.idle_policy()` (no auto-resume, `max_idle` = budget), `max_duration = policy.max_duration()` (budget plus slack, capped at 28,800), and `client_token = client_token(lease)`, so a replayed launch returns the same VM. `kind` is one of `sfn`, `durable`, `http`, `sqs`, `eventbridge`, `none`; `target` is required for the three generic kinds. `microvm.lease` also exposes `encode_payload`, `decode_payload`, and `client_token`. See [Integrations](integrations.md).

## FleetMonitor and CostModel

```python
mon = FleetMonitor(cfg)
mon.snapshot(image=None)              # {"total", "by_state", "members"}
mon.tail_logs("my-sandbox", minutes=15, limit=200)   # /aws/lambda-microvms/<image>, then the old /aws/lambda/microvms/<image>
mon.estimate_fleet_cost_per_hour("my-sandbox", memory_gb=2)

model = CostModel(memory_gb=2, snapshot_gb=0.61)
model.session(active_s=1800, suspended_s=8 * 3600, cycles=1)
# {"running_usd", "suspend_cycles_usd", "suspended_storage_usd", "total_usd", "vs_always_on_usd", "savings_pct"}
```

Rates are the published `us-east-1` launch rates and live as module constants in `microvm/monitor.py`. Re-verify them against the pricing page before quoting anyone.

## Low-level clients

```python
api = microvm_client(region, profile)          # boto3 "lambda-microvms" client with the vendored model
s3 = lambda_client("s3", region, profile)      # any other service on the same session
```

Use these when you need an API the plane does not wrap. Everything the plane does is a thin layer over them.
