# The playground

`mvm playground` starts a local web app that drives every feature of the SDK against the live service, with every parameter exposed and every result shown.

```console
pip install microvm-ctl
export MVM_ARTIFACT_BUCKET=... MVM_BUILD_ROLE_ARN=... MVM_EXECUTION_ROLE_ARN=...   # from mvm bootstrap
mvm playground                  # http://127.0.0.1:8765, opens a browser tab
mvm playground --dry-run        # start with dry run on
mvm playground --port 9000 --no-open
```

It is one Python process: a JSON API over `microvm` (standard library HTTP server, no extra dependencies) and a single HTML page. Nothing is stored; close the tab and the process and it is gone.

## What it does

| View | What you can do | SDK underneath |
|---|---|---|
| Overview | applied vs published quotas, fleet counts by state, configuration, recent API calls and jobs | `applied_quotas`, `FleetManager.list` |
| Images | list images and versions; build from a directory with memory tier, hooks port, env vars, OS capabilities; live build log with snapshot sizes | `ImageBuilder.build(log=...)` |
| Fleet | live table with suspend, resume, terminate, call, job, raw JSON per VM; run N VMs with idle policy, payload, duration cap, wait and health poll; `scale_to`, suspend all, resume all, reap, drain | `FleetManager`, `Fleet` |
| Lease (in Fleet) | hand one VM one task: image, kind (`none`, `sfn`, `durable`, `http`, `sqs`, `eventbridge`), token, target, task JSON, budget, heartbeat timeout, slack, heartbeat interval; the job log shows the encoded `runHookPayload` and `clientToken`, the exact RunMicrovm request, then every phase and log line from inside the VM until the lease completes | `Lease`, `LeasePolicy`, `encode_payload`, `FleetManager.lease`, `EndpointClient.status` |
| Call a VM | authenticated request with method, path, port, body, token TTL, resume patience, attempts; status, timing, headers, body; mint a token on its own and inspect it | `EndpointClient` |
| Logs | CloudWatch tail per image with window and limit, auto-refresh | `FleetMonitor.tail_logs` |
| Probe | a parameterised benchmark: launch latency to first byte, warm latency with a sparkline, suspend/resume timing, auto-resume timing, cleanup | the same protocol as `benchmarks/benchmark.py` |
| Cost | session pricing and the current fleet's cost per hour | `CostModel`, `FleetMonitor.estimate_fleet_cost_per_hour` |
| API trace | every AWS API call the process makes: operation, status, duration, redacted request and response | botocore `before-call` / `after-call` hooks |
| Settings | region, profile, bucket, roles for this session; bootstrap the account | `PlaneConfig`, `bootstrap` |

## Job inside the VM

The `job` button on a RUNNING VM in the Fleet table opens a panel under the row that polls the hook runtime's `GET /status` every two seconds (`GET /api/vms/<id>/job?since=<seq>&port=<port>`, which is `EndpointClient.status(since)`): the current phase, elapsed time, a progress bar, the counters the handler set, a lease chip (kind, heartbeats, active / done / lost / failed) and a live log tail that only fetches lines above the last sequence number seen. A VM whose image has no hook runtime, or that is not RUNNING, shows the reason in the panel instead of an error toast. "Stop following" ends the polling. In dry run without AWS the panel plays a sample job that loops through a few phases so the UI can be tried without a VM.

## Transparency

Long operations run as jobs. Each job has an event log that streams to the page as it runs, the parameters it was started with, and a structured result. Every AWS call made by the process, from any job or view, lands in the API trace with its request and response (tokens masked). Nothing happens off screen.

## Dry run

The switch in the top bar turns every mutating action into a printout of the exact request that would have been sent, recorded in the trace under the service name `dry-run`. Reads still hit AWS. Use it to see what `scale_to(3)` would launch or terminate, or what a build request looks like, before spending anything. `--dry-run` starts the process with the switch on.

## Cost and safety

The playground spends real money when dry run is off: builds, running VMs, snapshot cycles. Terminate and drain ask for confirmation. Every launch carries an idle policy and can carry a duration cap, so nothing runs forever by accident, but check the Fleet view before you close the tab.

## Hosting it behind CloudFront

The API is transport-agnostic: `Playground.api(method, path, query, body)` returns `(status, json)`, and `microvm.playground.server.lambda_handler` adapts a Lambda Function URL event to it. The UI is a single static file, `microvm/playground/static/index.html`, that calls `/api/...` on the same origin.

A deployment therefore looks like: the HTML in an S3 bucket behind CloudFront, the Function URL as a second CloudFront origin on the `/api/*` path, and the Lambda running with the plane's environment variables and an execution role that holds the control-plane permissions. Two things to plan for that the local process gives you for free: jobs run in background threads, which a Lambda invocation does not keep alive, so builds, scale operations and probes need a long-lived host (App Runner, ECS, or an EC2 instance running `mvm playground --host 0.0.0.0` behind CloudFront works for all of it); and the page must sit behind authentication, since it can launch VMs in your account.
