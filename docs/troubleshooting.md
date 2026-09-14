# Troubleshooting

Errors I hit on the live service, what each one meant, and what fixed it. Alexey Vidanov's [lambda-microvm-starter troubleshooting guide](https://github.com/vidanov/lambda-microvm-starter/blob/main/TROUBLESHOOTING.md) covers twenty more from the deploy-a-web-app side and is worth reading alongside this page.

## `ServiceQuotaExceededException` on `RunMicrovm`

The memory quota is full. Check `mvm quotas` for the applied ceiling and `mvm ls --all` for what is holding it. Remember that SUSPENDED, TERMINATING, and image-build VMs all count. Wait a minute or two for terminations to settle, drain what you do not need, or file a raise. `Throttled` already retries this error with long waits, so a fleet operation that hits it will usually recover on its own if capacity is freeing up.

## `ThrottlingException` or `TooManyRequestsException`

You exceeded the applied TPS for that API. The plane throttles to 80% of the applied rate, so this normally means another process is sharing the quota, or Service Quotas could not be read and the plane fell back to the published defaults. Run `mvm quotas`; if the applied column says "unknown", grant `servicequotas:GetServiceQuota` to the caller.

## Build fails with a CloudWatch pointer

`mvm image build` raises `ImageBuildError` with the build's `stateReason`. Run `mvm logs <image>` to see the Dockerfile output. Common causes: a package without an ARM64 wheel, a missing `Dockerfile` at the zip root, or an app that never answered 200 on `/ready` inside the 300 second timeout.

## `/validate` fails the build

Your validate handler raised or returned False. The hook runs on a clone restored from the new snapshot, so anything that only works on the build VM (a file written after `/ready`, a socket opened at build time) is missing here. Move that work into `/run`.

## First request returns 502

Either the app is not listening on the declared port yet, or the VM is mid-resume. `EndpointClient` retries 502s every 2 seconds for `resume_patience` seconds (30 by default, raise it for slow resumes). If the 502 persists, `mvm logs` shows whether the process crashed.

## `403` on the endpoint

The token expired or was minted for a different port. The client re-mints once on 403. If it keeps happening, check that the port you call matches the token scope (`ports=[...]` on `EndpointClient`).

## `NoCredentialsError` inside the VM

The VM was launched without an execution role. Set `MVM_EXECUTION_ROLE_ARN` or pass `execution_role=` to `run`. This failure is loud on purpose: there is no key baked into the image to fall back on.

## The VM was terminated while suspended

`suspendedDurationSeconds` elapsed. The default is 3,600 seconds. Raise `--suspended-ttl` for interactive workloads, inside the 8 hour total lifetime.

## The VM never suspends

Something is sending endpoint traffic. Health checks and polling frontends count. Move them outside the idle window or lengthen `--idle`.

## Every clone has the same "random" ID

You generated it at import time or in `/ready`, so it was baked into the snapshot. Generate per-VM values in `/run`. `HookApp` reseeds the RNG there, but it cannot un-cache a UUID you computed earlier.

## `pip install` inside the VM fails on a wheel

The VM is ARM64 only. Packages without aarch64 wheels fall back to source builds, which need a compiler in the image. Bake the heavy dependencies at build time and treat runtime installs as an escape hatch.

## Uploads or downloads through the endpoint are slow

Endpoint bandwidth is capped by VM size, roughly 1 MB/s at 0.5 GB up to 16 MB/s at 8 GB. Move bulk data through S3 or EFS using the execution role and send only pointers through the endpoint.
