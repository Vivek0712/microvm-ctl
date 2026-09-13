# Quickstart

This page takes an account with nothing in it to a microVM answering authenticated requests. It takes about five minutes of your time and about two minutes of build time on the service.

## 1. Install

```console
pip install microvm-ctl
mvm --version
```

The package vendors the `lambda-microvms` service model, so any boto3 from the last two years works. Your AWS credentials come from the usual places: `AWS_PROFILE`, environment variables, or an instance role.

## 2. Pick a region and profile

```console
export MVM_REGION=us-east-1          # default; also us-east-2, us-west-2, eu-west-1, ap-northeast-1
export MVM_PROFILE=my-profile        # optional; falls back to AWS_PROFILE
```

## 3. Bootstrap the account once

```console
mvm bootstrap
```

This creates three things and prints them back as export lines:

| Resource | Purpose |
|---|---|
| `microvm-ctl-artifacts-<account>-<region>` S3 bucket | holds the zipped app directories the build reads |
| `microvm-ctl-build-role` | assumed by the image build; reads the artifact bucket and writes build logs |
| `microvm-ctl-execution-role` | assumed by the running VM; writes runtime logs and whatever your workload needs |

Put the printed exports in your shell profile:

```console
export MVM_ARTIFACT_BUCKET=microvm-ctl-artifacts-123456789012-us-east-1
export MVM_BUILD_ROLE_ARN=arn:aws:iam::123456789012:role/microvm-ctl-build-role
export MVM_EXECUTION_ROLE_ARN=arn:aws:iam::123456789012:role/microvm-ctl-execution-role
```

The two roles are separate on purpose. The sandbox role never gets to read your artifact bucket, and the build role never gets workload permissions. Add policies to the execution role for anything your app calls (Bedrock, S3 data buckets, and so on).

## 4. Check your quotas before you plan a fleet

```console
mvm quotas
```

A fresh account typically shows `RunMicrovm` at 1 request per second and 8 GB of total microVM memory, against published defaults of 5 per second and 1,024 GB. The plane reads these applied values at startup and throttles to 80% of them, so scale operations degrade gracefully instead of failing. File a quota increase on day one if you plan to run more than three or four 2 GB VMs at once. See [Quotas and cost](quotas-and-cost.md).

## 5. Write an app

An app directory needs a `Dockerfile` at its root and a process that serves the hook contract on port 8080. The builder injects `microvm_hooks.py` into the zip, so the app can import it without installing anything.

`my-app/Dockerfile`:

```dockerfile
FROM public.ecr.aws/lambda/microvms:al2023-minimal
RUN dnf install -y python3.12 python3.12-pip && dnf clean all
RUN python3.12 -m pip install --no-cache-dir numpy
WORKDIR /app
COPY microvm_hooks.py app.py /app/
EXPOSE 8080
ENTRYPOINT ["python3.12", "/app/app.py"]
```

`my-app/app.py`:

```python
import subprocess
from microvm_hooks import HookApp

app = HookApp()

@app.on_ready
def ready(_ctx):
    return True                                   # warm enough to snapshot

@app.on_validate
def validate(_ctx):
    subprocess.run(["python3.12", "-c", "import numpy"], check=True)   # prefetch the hot path

@app.route("POST", "/execute")
def execute(body, _headers):
    proc = subprocess.run(["python3.12", "-c", body["code"]], capture_output=True, text=True, timeout=30)
    return 200, {"stdout": proc.stdout, "stderr": proc.stderr, "exit_code": proc.returncode}

if __name__ == "__main__":
    app.serve(port=8080)
```

## 6. Build the image

```console
mvm image build my-sandbox ./my-app --memory 2048
```

Lambda boots a fresh build VM from the managed AL2023 base image, runs your Dockerfile, starts your ENTRYPOINT, polls `/ready` until it answers 200, snapshots memory and disk, then boots a clone and calls `/validate`. The command waits for the build, marks the version ACTIVE, and prints the snapshot sizes. Expect about two minutes.

## 7. Run, call, watch

```console
mvm run my-sandbox --wait
mvm call <microvm-id> /execute -X POST -d '{"code":"import numpy; print(numpy.zeros(3))"}'
mvm top
mvm logs my-sandbox
```

The first request to a fresh VM includes the token mint and costs about 700 ms. Warm requests measured at a p50 of 111 ms end to end.

## 8. Suspend, resume, terminate

```console
mvm suspend <microvm-id>        # compute billing stops; memory goes to snapshot storage
mvm call <microvm-id> /execute -X POST -d '{"code":"print(1)"}'   # auto-resumes the VM
mvm terminate <microvm-id>
```

Every `mvm run` sets an idle policy. The defaults suspend after 300 s without endpoint traffic and terminate after 3,600 s suspended. Change them with `--idle` and `--suspended-ttl`, and cap total lifetime with `--max-duration`.

## 9. Scale a fleet

```console
mvm scale my-sandbox 6 --wait
mvm top --watch
mvm drain my-sandbox
```

Scale-out launches through the token bucket. Scale-down terminates suspended members first, because they still hold memory quota, then the youngest running members.

## Where next

- The eight example apps in [awesome-microvm](https://github.com/Vivek0712/awesome-microvm) show the patterns that matter in practice: per-session sandboxes, fan-out fleets, stateful kernels, and multi-tenant identity through `runHookPayload`.
- [The hook contract](hooks.md) explains what to do in each hook and the snapshot rules behind it.
- [Troubleshooting](troubleshooting.md) lists the errors we hit and what they meant.
