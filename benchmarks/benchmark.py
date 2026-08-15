"""Benchmark protocol for the awesome-microvm plane.

Measures what AWS doesn't publish: launch latency, resume latency, warm
request latency, suspend/resume state fidelity, and scale-out wall time —
then prices the session with the CostModel. Emits JSON + an SVG terminal
snapshot per section (used in the blogs).

    MVM_PROFILE=heisenberg python3 benchmarks/benchmark.py --image code-sandbox --launches 5
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from rich.console import Console
from rich.table import Table

from microvm import CostModel, EndpointClient, Fleet, FleetManager, PlaneConfig
from microvm.fleet import IdlePolicy

RESULTS = Path(__file__).parent / "results"


def pctile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", default="code-sandbox")
    ap.add_argument("--launches", type=int, default=5)
    ap.add_argument("--warm-calls", type=int, default=20)
    ap.add_argument("--scale-to", type=int, default=3)
    ap.add_argument("--scale-image", default=None,
                    help="image for the scale section (default: --image); use a "
                         "small-baseline image to fit dense fleets in the memory quota")
    args = ap.parse_args()

    console = Console(record=True, width=100)
    cfg = PlaneConfig()
    fm = FleetManager(cfg)
    out: dict = {"image": args.image, "region": cfg.region, "date": time.strftime("%Y-%m-%d")}

    # 1 — launch latency ---------------------------------------------------------
    console.rule("[bold]1 · launch latency (RunMicrovm → RUNNING → first byte)")
    launches = []
    vms = []
    for i in range(args.launches):
        # stay inside the memory quota: TERMINATING samples still count
        deadline = time.time() + 180
        while time.time() < deadline:
            live = [v for v in fm.list(args.image) if v.state != "TERMINATED"]
            if len(live) <= len(vms):
                break
            time.sleep(5)
        t0 = time.time()
        vm = fm.run(args.image, idle_policy=IdlePolicy(max_idle=1800, suspended_for=7200))
        t_api = time.time() - t0
        vm = fm.wait_until(vm.microvm_id, "RUNNING", timeout=180)
        t_running = time.time() - t0
        client = EndpointClient(cfg, vm.microvm_id, endpoint=vm.endpoint)
        client.wait_ready("/healthz", timeout=120)
        t_first = time.time() - t0
        launches.append({"api_s": round(t_api, 2), "running_s": round(t_running, 2),
                         "first_byte_s": round(t_first, 2)})
        console.print(f"  vm {i+1}: api {t_api:.2f}s → RUNNING {t_running:.2f}s → serving {t_first:.2f}s")
        if vms:  # keep only the first VM as the probe — stay inside the memory quota
            fm.terminate(vm.microvm_id)
        else:
            vms.append(vm)
    fb = [l["first_byte_s"] for l in launches]
    rn = [l["running_s"] for l in launches]
    out["launch"] = {
        "samples": launches,
        "running_p50_s": round(statistics.median(rn), 2),
        "running_p95_s": round(pctile(rn, 95), 2),
        "first_byte_p50_s": round(statistics.median(fb), 2),
        "first_byte_p95_s": round(pctile(fb, 95), 2),
    }
    console.print(f"[bold green]p50 to serving: {out['launch']['first_byte_p50_s']}s   "
                  f"p95: {out['launch']['first_byte_p95_s']}s[/]")

    # 2 — warm request latency ---------------------------------------------------
    console.rule("[bold]2 · warm request latency (authenticated /execute)")
    client = EndpointClient(cfg, vms[0].microvm_id, endpoint=vms[0].endpoint)
    lat = []
    for _ in range(args.warm_calls):
        t0 = time.time()
        r = client.post("/execute", json={"code": "print(sum(range(1000)))"})
        assert r.status_code == 200, r.text
        lat.append((time.time() - t0) * 1000)
    out["warm_request_ms"] = {
        "p50": round(statistics.median(lat), 1),
        "p95": round(pctile(lat, 95), 1),
        "min": round(min(lat), 1),
        "samples": len(lat),
    }
    console.print(f"[bold green]p50 {out['warm_request_ms']['p50']} ms   "
                  f"p95 {out['warm_request_ms']['p95']} ms   min {out['warm_request_ms']['min']} ms[/]")

    # 3 — suspend / resume fidelity ---------------------------------------------
    console.rule("[bold]3 · suspend → resume: is state really preserved?")
    probe = vms[0]
    client.post("/execute", json={"code": "open('/tmp/workspace/marker.txt','w').write('survived')"})
    before = client.get("/state").json()
    console.print(f"  before suspend: pid={before['pid']} executions={before['executions']} "
                  f"files={before['workspace_files']}")
    t0 = time.time()
    fm.suspend(probe.microvm_id)
    fm.wait_until(probe.microvm_id, "SUSPENDED", timeout=120)
    t_suspend = time.time() - t0
    t0 = time.time()
    fm.resume(probe.microvm_id)
    fm.wait_until(probe.microvm_id, "RUNNING", timeout=120)
    after = client.get("/state").json()
    t_resume = time.time() - t0
    preserved = (after["pid"] == before["pid"]
                 and after["executions"] == before["executions"]
                 and "marker.txt" in after["workspace_files"])
    out["suspend_resume"] = {
        "suspend_s": round(t_suspend, 2), "resume_to_serving_s": round(t_resume, 2),
        "pid_before": before["pid"], "pid_after": after["pid"],
        "state_preserved": preserved,
    }
    console.print(f"  suspend {t_suspend:.1f}s · resume-to-serving {t_resume:.1f}s · "
                  f"pid {before['pid']} → {after['pid']} · "
                  + ("[bold green]STATE PRESERVED[/]" if preserved else "[bold red]STATE LOST[/]"))

    # 4 — auto-resume on traffic -------------------------------------------------
    console.rule("[bold]4 · auto-resume: hit a SUSPENDED VM with traffic")
    fm.suspend(probe.microvm_id)
    fm.wait_until(probe.microvm_id, "SUSPENDED", timeout=120)
    t0 = time.time()
    r = client.get("/state", resume_patience=90)
    t_auto = time.time() - t0
    out["auto_resume"] = {"first_request_s": round(t_auto, 2), "status": r.status_code}
    console.print(f"  first request to suspended VM: {r.status_code} in {t_auto:.1f}s "
                  f"(the request itself woke the VM)")

    # 5 — fleet scale-out --------------------------------------------------------
    scale_image = args.scale_image or args.image
    console.rule(f"[bold]5 · fleet scale: {scale_image} → {args.scale_to} VMs, then drain")
    fm.terminate(probe.microvm_id)  # free quota; the scale test owns the fleet now
    # Freed memory lags termination by minutes on reduced-quota accounts — settle hard.
    deadline = time.time() + 300
    while time.time() < deadline:
        if all(v.state == "TERMINATED" for v in fm.list()):
            break
        time.sleep(5)
    time.sleep(60)
    fleet = Fleet(fm, scale_image, idle_policy=IdlePolicy(max_idle=1800, suspended_for=3600))
    t0 = time.time()
    fleet.scale_to(args.scale_to, wait_running=True)
    t_scale = time.time() - t0
    size = fleet.size()
    console.print(f"  fleet at {size} RUNNING in {t_scale:.1f}s wall (throttled to service TPS)")
    t0 = time.time()
    drained = fleet.drain()
    t_drain = time.time() - t0
    out["scale"] = {"target": args.scale_to, "reached": size,
                    "scale_out_wall_s": round(t_scale, 1), "drain_wall_s": round(t_drain, 1),
                    "drained": drained}
    console.print(f"  drained {drained} VMs in {t_drain:.1f}s")

    # 6 — session economics -------------------------------------------------------
    console.rule("[bold]6 · session economics (measured shape)")
    model = CostModel(memory_gb=2, snapshot_gb=0.61)  # measured memory snapshot: 609 MB
    scenarios = {
        "8s one-shot job (terminate)": model.session(8, 0, cycles=0),
        "30 min active + 8 h suspended": model.session(30 * 60, 8 * 3600, cycles=1),
        "2 h active + 22 h suspended": model.session(2 * 3600, 22 * 3600, cycles=2),
    }
    t = Table(header_style="bold magenta")
    t.add_column("scenario"); t.add_column("total", justify="right")
    t.add_column("always-on", justify="right"); t.add_column("saved", justify="right")
    for name, s in scenarios.items():
        t.add_row(name, f"${s['total_usd']:.4f}", f"${s['vs_always_on_usd']:.4f}",
                  f"[green]{s['savings_pct']}%[/]")
    console.print(t)
    out["economics"] = scenarios

    # persist ----------------------------------------------------------------------
    RESULTS.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    (RESULTS / f"benchmark-{stamp}.json").write_text(json.dumps(out, indent=2))
    console.save_svg(str(RESULTS / f"benchmark-{stamp}.svg"), title=f"mvm benchmark — {args.image}")
    console.print(f"\n[dim]results → benchmarks/results/benchmark-{stamp}.json (+ .svg)[/]")


if __name__ == "__main__":
    main()
