# Changelog

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
