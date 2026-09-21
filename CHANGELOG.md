# Changelog

## 0.3.1 (2026-09-21)

Found by running every lease scenario live from Step Functions and a durable function ([microvm-handoff-demo](https://github.com/Vivek0712/microvm-handoff-demo)).

- Step Functions: a typed lease failure now terminates the VM it came from. The caught cause is the VM's own payload and names `microvm_id`; the generated machine gains `OnLeaseError` (a Choice) and `TerminateFailed` before `Reap`. A timeout still carries no id: that VM is bounded by `MaximumDurationInSeconds` (budget plus slack) and `TerminateStale` reaps anything older.
- `LeasePolicy.heartbeat_every(requested)`: the VM's heartbeat interval is clamped to at most a third of `heartbeat_timeout_s` (never under 5 s) by `lease_state_machine`, `durable.lease_microvm`, and `mvm lease run`. With `heartbeat_timeout_s=30` and the default 30 s interval, a durable callback timed out at 30.1 s before the first heartbeat landed.
- Hook runtime: the first heartbeat goes out as soon as the lease is accepted, then every `heartbeat_s`.

## 0.3.0 (2026-09-20)

- Leases at scale: `LeasePolicy` gains `max_concurrency`, `max_vm_seconds`, `approval_usd`, and `from_env()`; `FleetManager.fanout_limit`, `plan` (`LeasePlan`, `LeasePlanRejected`), and `lease_many`; `ImageBuilder.baseline_mib`.
- `mvm lease plan`, `mvm lease run --shards`, `mvm lease asl --map` with an optional SNS approval gate, `mvm watch --image`, and per-tier counts in `mvm quotas`.
- `microvm.integrations.durable.lease_map`: plan step, optional approval callback, `context.map` over `lease_with_relaunch`.
- `FleetMonitor.job_status` and a fleet job panel plus shards in the playground's lease form.
- Companion examples: Step Functions map machine, durable fan-out mode, `parallel: true` steps in the handoff agent, a circuit breaker stack, fan-out and in-VM parallel benchmarks.

## 0.2.1 (2026-09-20)

- Hook runtime: `GET /events` captures its cursor before the headers go out, so a log line written the moment a client connects is streamed instead of waiting for the next snapshot. Rebuild images to pick it up.

## 0.2.0 (2026-09-20)

- Lease contract: `Lease`, `LeasePolicy`, `FleetManager.lease`, and `@app.on_lease` in the injected hook runtime, with completers for Step Functions task tokens, durable function callbacks, HTTP, SQS, and EventBridge; heartbeats, typed failures, closed-token detection, failure from `/terminate` mid-flight.
- Job telemetry inside the VM: `app.job`, `GET /status`, `GET /events` (SSE); `EndpointClient.status()` and `watch()`; `mvm status`, `mvm watch`.
- `microvm.integrations.stepfunctions`: `lease_state_machine` generates the JSONata state machine for one lease (`runMicrovm.waitForTaskToken`, terminate, reap by age on timeout); `iam_statements` and `orchestrator_statements`; `mvm lease asl`, `mvm lease policy`, `mvm lease run`.
- `microvm.integrations.durable.lease_microvm` and `lease_with_relaunch` for Lambda durable functions, behind `pip install microvm-ctl[durable]`.
- `mvm playground`: a local web app over the whole SDK with images, fleet, calls, logs, cost, a parameterised probe, an API trace of every AWS call, a job panel and lease form, credentials and dry-run switches; standard library only.
- `ImageBuilder.build(log=...)` reports build milestones; `FleetManager.run_params` exposes the exact RunMicrovm request; `FleetManager.run(client_token=...)` makes a launch idempotent.
- Fixes from the live runs: orchestrator policies include `lambda:PassNetworkConnector`; the service log group is `/aws/lambda-microvms/<image>` (bootstrap grants both names, `tail_logs` tries the service name first); a token-less lease gets a fresh `clientToken` per launch.
- Generated state machines carry the shard index in the Map items (the service exposes none inside a JSONata item processor), use unique state names, and retry throttled launches after 2 s instead of 10 s.
- `FleetMonitor.job_status` bounds each member poll and reuses one executor; `/events` opens with the snapshot before queued lines.
- Python 3.13 in CI.

## 0.1.0 (2026-09-13)

First public release.

- Image factory: app directory to ACTIVE image version, with the hook runtime injected into every zip.
- Fleet manager and declarative `Fleet` with quota-aware throttling read from Service Quotas.
- `EndpointClient` with token caching, 429 backoff, patient 502 retry across auto-resume, and 403 re-mint that preserves caller headers and port selection.
- Zero-dependency `HookApp` implementing all six lifecycle hooks; `/validate` handlers can now reject a build by returning False.
- `mvm` CLI including `quotas` (applied versus published) and `cost` (session pricing).
- Benchmark harness and the measured numbers behind the docs.
- Credits to Alexey Vidanov's lambda-microvm-starter, the project that inspired this one.
- Unit tests for throttling, endpoint retries, fleet scale-down selection, config, and hooks; GitHub Actions CI on Python 3.9 to 3.12.
