# The hook contract

Lambda MicroVMs drives your app's lifecycle through plain HTTP endpoints that **your app serves** on the port declared in the image's `hooks.port`. The `HookApp` server injected into every image implements all of them with the standard library only. This page explains what each hook is for and what goes wrong when you ignore it.

## Build-time hooks

| Hook | When it is called | Contract |
|---|---|---|
| `GET /aws/lambda-microvms/runtime/v1/ready` | on the build VM, after your ENTRYPOINT starts | return 503 until warm, then 200; Lambda snapshots memory and disk at that instant |
| `POST /aws/lambda-microvms/runtime/v1/validate` | on a fresh VM restored from the new snapshot | 200 means the version is usable; anything else fails the build |

**`/ready` is your snapshot curator.** Everything alive when it returns 200 is cloned into every future VM: imports, caches, daemon processes, and any secret that happens to be in memory. Warm up what you want cloned and fetch what you do not in `/run`.

**`/validate` is a free cold-start optimizer.** Lambda records which snapshot regions the validate run touches and prefetches them on every launch. Exercise your hot path with a mock payload. The code-sandbox example runs a real numpy execution there, and the PDF example renders a throwaway document.

Return promptly. A hook request held open past its timeout fails the build.

## Runtime hooks

All runtime hooks are POSTs under `/aws/lambda-microvms/runtime/v1/`.

| Hook | When it is called | Use it for |
|---|---|---|
| `/run` | after clone; endpoint traffic starts only after you return 200 | restore uniqueness (RNG, IDs), parse `runHookPayload`, create AWS clients, fetch secrets |
| `/resume` | after resume; the VM presents as SUSPENDED until you return 200 | refresh credentials and connections that aged while frozen |
| `/suspend` | before the suspend snapshot | flush buffers, close what should not be frozen |
| `/terminate` | before teardown | final flush or checkpoint to S3 or EFS |

The `/run` body is `{"microvmId": "...", "runHookPayload": "<your string>"}`. This is the only per-VM input channel at launch. Image environment variables are shared by every clone, capped at 50, and need a rebuild to change, so tenant IDs and secrets never belong there.

A non-200 from `/run` terminates the VM. Wrap best-effort setup in a try block so a transient IAM hiccup degrades the VM instead of killing it. The data-analytics example does exactly this for its S3 credential bootstrap.

## Using HookApp

```python
from microvm_hooks import HookApp   # injected into every image by the builder

app = HookApp()

@app.on_ready
def ready(ctx):
    warm_caches()
    return True                      # False gives 503, and Lambda retries

@app.on_validate
def validate(ctx):
    run_hot_path_once()              # return False or raise to fail the build

@app.on_run
def run(ctx):                        # ctx is the /run body
    configure_tenant(ctx.get("runHookPayload"))

@app.on_resume
def resume(ctx):
    rebuild_clients()                # pre-suspend TCP connections are dead

@app.route("POST", "/work")
def work(body, headers):
    return 200, {"result": do(body)}

app.serve(port=8080)                 # hooks and app routes on one port
```

Route handlers receive the parsed JSON body (or `{"_raw": ...}` when the body is not JSON) and the request headers, and return `(status, payload)`. Payloads are JSON-encoded unless you return bytes. Unhandled exceptions become a 500 with a short traceback. `GET /healthz` is registered for you and reports the microVM id once `/run` has fired.

`HookApp` reseeds Python's global RNG on every `/run`. If you use `os.urandom` or the `secrets` module you are safe by construction; if you cache tokens or UUIDs at import time, you are not.

`app.serve(port, background=True)` returns the server instead of blocking, which is how the unit tests drive it without AWS.

## Timeouts

The builder's `default_hooks()` sets `/ready` to 300 s, `/validate` to 120 s, `/run` and `/resume` to 60 s, and `/suspend` and `/terminate` to 30 s. Pass your own `hooks` dict to `ImageBuilder.build` to change them.
