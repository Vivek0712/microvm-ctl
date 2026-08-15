# The hook contract, in depth

Lambda MicroVMs drives your app's lifecycle through plain HTTP endpoints that
**your app serves** on the port declared in the image's `hooks.port`. The
`microvm_hooks.HookApp` server in this repo implements all of them with zero
dependencies; this page explains what each one is for and what goes wrong if
you ignore it.

## Build-time hooks

| Hook | Called | Contract |
|---|---|---|
| `/ready` | on the build VM, after your ENTRYPOINT starts | return **503 until warm**, then 200 → Lambda snapshots memory + disk *at that instant* |
| `/validate` | on a **fresh VM restored from the new snapshot** | 200 = image version usable; non-200 fails the build |

Two things people miss about these:

1. **`/ready` is your snapshot curator.** Everything alive when it returns 200
   is cloned into every future VM — imports, caches, daemon processes, and
   unfortunately also any secrets in memory. Warm up what you want cloned;
   fetch what you don't in `/run`.
2. **`/validate` is a free cold-start optimizer.** Lambda records which
   snapshot regions the validate run touches and prefetches them on every
   launch. Exercise your hot path with a mock payload — our `code-sandbox`
   image runs a real numpy execution there, and the notebook image executes a
   warm-up cell.

Return immediately; a held-open hook request that hits the timeout kills the
build.

## Runtime hooks (POST, base `/aws/lambda-microvms/runtime/v1/`)

| Hook | Called | Use it for |
|---|---|---|
| `/run` | after clone; **endpoint traffic starts only after you return 200** | restore uniqueness (RNG, IDs), parse `runHookPayload` (tenant/session context), create AWS clients, fetch secrets |
| `/resume` | after resume; VM presents as SUSPENDED until 200 | refresh credentials and connections that aged while suspended |
| `/suspend` | before the suspend snapshot | flush buffers, close what shouldn't be frozen |
| `/terminate` | before teardown | final flush / checkpoint to S3/EFS |

The `/run` body is `{"microvmId": "...", "runHookPayload": "<your string>"}` —
this is the **only** per-VM input channel at launch. Image environment
variables are shared by every clone (max 50, rebuild to change): never put
tenant IDs or secrets there.

## Using HookApp

```python
from microvm_hooks import HookApp   # injected into every image by the builder

app = HookApp()

@app.on_ready
def ready(ctx):
    warm_caches()
    return True            # False → 503 → Lambda retries

@app.on_run
def run(ctx):              # ctx == the /run body
    configure_tenant(ctx.get("runHookPayload"))

@app.route("POST", "/work")
def work(body, headers):
    return 200, {"result": do(body)}

app.serve(port=8080)       # hooks + app routes on one port
```

`HookApp` reseeds Python's global RNG on every `/run` automatically — one
snapshot-uniqueness footgun handled for you. If you use `os.urandom`/`secrets`
you're safe by construction; if you cache tokens or UUIDs at import time,
you're not.
