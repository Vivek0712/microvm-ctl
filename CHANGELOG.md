# Changelog

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
