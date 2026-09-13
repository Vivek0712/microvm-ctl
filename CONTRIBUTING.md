# Contributing

## Local loop

```console
git clone https://github.com/Vivek0712/microvm-ctl
cd microvm-ctl
python -m pip install -e ".[dev]"
python -m pytest -q
python -m ruff check microvm tests benchmarks
```

The unit tests do not touch AWS. They drive the hook server on a loopback port and fake the API and HTTP layers for the throttle, endpoint, and fleet tests.

## Testing against the live service

Set the `MVM_*` environment variables from `mvm bootstrap`, then run the benchmark harness against a small image. It launches and terminates a handful of VMs and costs a few cents.

```console
python benchmarks/benchmark.py --image code-sandbox --launches 3 --scale-to 3
```

Check `mvm quotas` first. Fresh accounts have 8 GB of total microVM memory, which the harness respects by running one probe VM at a time.

## Conventions

- Every mutating API call goes through `Throttled`. Do not call `run_microvm` and friends directly from new code.
- Nothing that differs per VM may be computed at build time. If you add a hook example, put uniqueness in `/run`.
- Numbers in the docs are measured. If you change something that affects latency or cost, re-run the harness and update the tables with the new output.
- Docs are written as one paragraph per line with plain ASCII punctuation, so they paste cleanly into other publishing tools.

## Releasing

Releases go to PyPI through trusted publishing, so no token lives in the repo. Bump `version` in `pyproject.toml` and the top entry in `CHANGELOG.md`, commit, then tag and push:

```console
git tag v0.1.0
git push origin main --tags
```

The `publish` workflow builds the sdist and wheel and uploads them. The first release needs the pending publisher registered once on pypi.org (owner `Vivek0712`, repository `microvm-ctl`, workflow `publish.yml`, environment `pypi`).

## Reporting a problem

Open an issue with the `mvm` command or SDK call, the region, the applied quotas from `mvm quotas`, and the relevant lines from `mvm logs <image>`. Strip account ids and endpoint hostnames if you prefer.
