"""`mvm` — the awesome-microvm CLI.

    mvm bootstrap                          one-time: S3 artifact bucket + build/execution roles
    mvm image build NAME DIR               zip -> S3 -> build -> ACTIVE version
    mvm image ls | versions NAME
    mvm run IMAGE [-n 5]                   spin up microVM(s)
    mvm ls [--image NAME]                  fleet listing
    mvm scale IMAGE N                      converge fleet to N
    mvm suspend|resume|terminate ID...     lifecycle control
    mvm drain IMAGE                        terminate the whole fleet
    mvm call ID /path [-X POST -d '{}']    authenticated request into the VM
    mvm top [--image NAME] [--watch]       live fleet dashboard
    mvm logs IMAGE                         CloudWatch tail
    mvm cost [--memory-gb 2 ...]           session economics
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from rich.console import Console
from rich.table import Table

from microvm.config import PlaneConfig
from microvm.endpoint import EndpointClient
from microvm.fleet import Fleet, FleetManager, IdlePolicy
from microvm.images import ImageBuilder
from microvm.monitor import CostModel, FleetMonitor

console = Console()

STATE_STYLE = {
    "PENDING": "yellow",
    "RUNNING": "bold green",
    "SUSPENDING": "yellow",
    "SUSPENDED": "cyan",
    "TERMINATING": "red",
    "TERMINATED": "dim",
}


def _cfg(args) -> PlaneConfig:
    kw = {}
    if args.region:
        kw["region"] = args.region
    if args.profile:
        kw["profile"] = args.profile
    return PlaneConfig(**kw)


def _state(s: str) -> str:
    return f"[{STATE_STYLE.get(s, 'white')}]{s}[/]"


# ---------------------------------------------------------------- bootstrap
def cmd_bootstrap(args):
    from microvm.bootstrap import bootstrap
    out = bootstrap(_cfg(args), prefix=args.prefix)
    console.print("[bold green]✓ control plane bootstrapped[/]")
    for k, v in out.items():
        console.print(f"  [cyan]{k}[/] = {v}")
    console.print(
        "\nExport these (or put them in your shell profile):\n"
        f"  export MVM_ARTIFACT_BUCKET={out['artifact_bucket']}\n"
        f"  export MVM_BUILD_ROLE_ARN={out['build_role_arn']}\n"
        f"  export MVM_EXECUTION_ROLE_ARN={out['execution_role_arn']}"
    )


# ---------------------------------------------------------------- images
def cmd_image_build(args):
    cfg = _cfg(args)
    builder = ImageBuilder(cfg)
    env = dict(kv.split("=", 1) for kv in args.env) if args.env else None
    with console.status(f"[bold]building image [cyan]{args.name}[/] from {args.dir}…"):
        built = builder.build(
            args.name,
            args.dir,
            memory_mib=args.memory,
            environment=env,
            os_capabilities_all=args.caps_all,
            description=args.description,
        )
    console.print(f"[bold green]✓ {built.name}:{built.version}[/] ({built.build_seconds}s)")
    if built.memory_snapshot_bytes:
        console.print(
            f"  memory snapshot: {built.memory_snapshot_bytes/1e6:.0f} MB   "
            f"disk snapshot: {(built.disk_snapshot_bytes or 0)/1e6:.0f} MB"
        )


def cmd_image_ls(args):
    t = Table(title="MicroVM images", header_style="bold magenta")
    for c in ("name", "state", "active version", "created"):
        t.add_column(c)
    for img in ImageBuilder(_cfg(args)).list_images():
        t.add_row(
            img["name"], img["state"], img.get("latestActiveImageVersion", "-"),
            str(img["createdAt"])[:19],
        )
    console.print(t)


def cmd_image_versions(args):
    t = Table(title=f"versions of {args.name}", header_style="bold magenta")
    for c in ("version", "build state", "status", "created"):
        t.add_column(c)
    for v in ImageBuilder(_cfg(args)).list_versions(args.name):
        t.add_row(v["imageVersion"], v["state"], v["status"], str(v["createdAt"])[:19])
    console.print(t)


# ---------------------------------------------------------------- lifecycle
def cmd_run(args):
    cfg = _cfg(args)
    fm = FleetManager(cfg)
    policy = IdlePolicy(max_idle=args.idle, suspended_for=args.suspended_ttl)
    for i in range(args.count):
        vm = fm.run(
            args.image,
            version=args.version,
            idle_policy=policy,
            run_payload=args.payload,
            max_duration=args.max_duration,
        )
        console.print(
            f"[bold green]✓[/] {vm.microvm_id}  {_state(vm.state)}  "
            f"[link=https://{vm.endpoint}]{vm.endpoint}[/link]"
        )
        if args.wait:
            vm = fm.wait_until(vm.microvm_id, "RUNNING")
            console.print(f"  now {_state(vm.state)}")


def cmd_ls(args):
    fm = FleetManager(_cfg(args))
    vms = fm.list(args.image)
    if not args.all:
        vms = [v for v in vms if v.state != "TERMINATED"]
    t = Table(title="microVMs", header_style="bold magenta")
    for c in ("microvm id", "state", "image", "version", "started"):
        t.add_column(c)
    for vm in vms:
        t.add_row(
            vm.microvm_id, _state(vm.state),
            vm.image_arn.split(":")[-1], vm.image_version, str(vm.started_at)[:19],
        )
    console.print(t)


def cmd_get(args):
    vm = FleetManager(_cfg(args)).api.get_microvm(microvmIdentifier=args.id)
    vm.pop("ResponseMetadata", None)
    console.print_json(json.dumps(vm, default=str))


def _lifecycle(args, verb: str):
    fm = FleetManager(_cfg(args))
    for vid in args.ids:
        getattr(fm, verb)(vid)
        console.print(f"[bold green]✓[/] {verb} {vid}")


def cmd_scale(args):
    cfg = _cfg(args)
    fleet = Fleet(FleetManager(cfg), args.image, idle_policy=IdlePolicy(max_idle=args.idle))
    before = fleet.size()
    with console.status(f"[bold]scaling [cyan]{args.image}[/] {before} → {args.n}…"):
        fleet.scale_to(args.n, wait_running=args.wait)
    console.print(f"[bold green]✓[/] fleet [cyan]{args.image}[/]: {before} → {fleet.size()} microVMs")


def cmd_drain(args):
    n = Fleet(FleetManager(_cfg(args)), args.image).drain()
    console.print(f"[bold green]✓[/] terminated {n} microVMs of [cyan]{args.image}[/]")


# ---------------------------------------------------------------- execution plane
def cmd_call(args):
    cfg = _cfg(args)
    client = EndpointClient(cfg, args.id, ports=[args.port] if args.port else None)
    resp = client.request(
        args.method, args.path,
        port=args.port,
        data=args.data.encode() if args.data else None,
        headers={"Content-Type": "application/json"} if args.data else {},
    )
    console.print(f"[bold]{resp.status_code}[/] in {resp.elapsed.total_seconds()*1000:.0f} ms")
    try:
        console.print_json(json.dumps(resp.json()))
    except ValueError:
        console.print(resp.text[:2000])


# ---------------------------------------------------------------- monitor
def _top_table(mon: FleetMonitor, image: str | None) -> Table:
    snap = mon.snapshot(image)
    counts = "  ".join(f"{_state(s)}×{n}" for s, n in sorted(snap["by_state"].items()))
    t = Table(
        title=f"fleet: {image or 'all images'} — {snap['total']} VMs   {counts}",
        header_style="bold magenta",
    )
    for c in ("microvm id", "state", "image", "started"):
        t.add_column(c)
    for vm in snap["members"]:
        t.add_row(
            vm["microvmId"], _state(vm["state"]),
            vm["imageArn"].split(":")[-1], str(vm["startedAt"])[:19],
        )
    return t


def cmd_top(args):
    mon = FleetMonitor(_cfg(args))
    if not args.watch:
        console.print(_top_table(mon, args.image))
        return
    from rich.live import Live
    with Live(_top_table(mon, args.image), refresh_per_second=0.5, console=console) as live:
        while True:
            time.sleep(args.interval)
            live.update(_top_table(mon, args.image))


def cmd_logs(args):
    mon = FleetMonitor(_cfg(args))
    events = mon.tail_logs(args.image, minutes=args.minutes)
    if not events:
        console.print(f"[dim]no events in /aws/lambda/microvms/{args.image}[/]")
    for e in events:
        console.print(f"[dim]{e['stream'][:20]}[/] {e['message']}")


def cmd_cost(args):
    model = CostModel(memory_gb=args.memory_gb, snapshot_gb=args.snapshot_gb)
    s = model.session(args.active * 60, args.suspended * 60, args.cycles)
    t = Table(title=f"session economics — {args.memory_gb} GB / {model.vcpu} vCPU", header_style="bold magenta")
    t.add_column("dimension"); t.add_column("USD", justify="right")
    t.add_row("running compute", f"${s['running_usd']:.6f}")
    t.add_row(f"suspend cycles ×{args.cycles}", f"${s['suspend_cycles_usd']:.6f}")
    t.add_row("suspended storage", f"${s['suspended_storage_usd']:.6f}")
    t.add_row("[bold]total[/]", f"[bold]${s['total_usd']:.6f}[/]")
    t.add_row("[dim]always-on equivalent[/]", f"[dim]${s['vs_always_on_usd']:.6f}[/]")
    t.add_row("[bold green]savings[/]", f"[bold green]{s['savings_pct']}%[/]")
    console.print(t)


# ---------------------------------------------------------------- parser
def main(argv: list[str] | None = None):
    p = argparse.ArgumentParser(prog="mvm", description="Control & execution plane for AWS Lambda MicroVMs")
    p.add_argument("--region", default=None)
    p.add_argument("--profile", default=None)
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bootstrap", help="provision S3 bucket + IAM roles")
    b.add_argument("--prefix", default="awesome-microvm")
    b.set_defaults(fn=cmd_bootstrap)

    img = sub.add_parser("image", help="image factory").add_subparsers(dest="sub", required=True)
    ib = img.add_parser("build")
    ib.add_argument("name"); ib.add_argument("dir")
    ib.add_argument("--memory", type=int, default=2048)
    ib.add_argument("--env", action="append", metavar="K=V")
    ib.add_argument("--caps-all", action="store_true", help="additionalOsCapabilities=ALL (EFS/FUSE/containerd/eBPF)")
    ib.add_argument("--description")
    ib.set_defaults(fn=cmd_image_build)
    il = img.add_parser("ls"); il.set_defaults(fn=cmd_image_ls)
    iv = img.add_parser("versions"); iv.add_argument("name"); iv.set_defaults(fn=cmd_image_versions)

    r = sub.add_parser("run", help="spin up microVM(s)")
    r.add_argument("image"); r.add_argument("--version")
    r.add_argument("-n", "--count", type=int, default=1)
    r.add_argument("--idle", type=int, default=300, help="suspend after N idle seconds")
    r.add_argument("--suspended-ttl", type=int, default=3600, help="auto-terminate after N suspended seconds")
    r.add_argument("--payload", help="runHookPayload (per-VM context)")
    r.add_argument("--max-duration", type=int, default=None)
    r.add_argument("--wait", action="store_true")
    r.set_defaults(fn=cmd_run)

    ls = sub.add_parser("ls"); ls.add_argument("--image"); ls.add_argument("--all", action="store_true")
    ls.set_defaults(fn=cmd_ls)
    g = sub.add_parser("get"); g.add_argument("id"); g.set_defaults(fn=cmd_get)

    for verb in ("suspend", "resume", "terminate"):
        v = sub.add_parser(verb)
        v.add_argument("ids", nargs="+")
        v.set_defaults(fn=lambda a, _v=verb: _lifecycle(a, _v))

    sc = sub.add_parser("scale", help="converge fleet to N")
    sc.add_argument("image"); sc.add_argument("n", type=int)
    sc.add_argument("--idle", type=int, default=300)
    sc.add_argument("--wait", action="store_true")
    sc.set_defaults(fn=cmd_scale)

    d = sub.add_parser("drain"); d.add_argument("image"); d.set_defaults(fn=cmd_drain)

    c = sub.add_parser("call", help="authenticated request into a microVM")
    c.add_argument("id"); c.add_argument("path")
    c.add_argument("-X", "--method", default="GET")
    c.add_argument("-d", "--data")
    c.add_argument("--port", type=int, default=None)
    c.set_defaults(fn=cmd_call)

    tp = sub.add_parser("top", help="live fleet dashboard")
    tp.add_argument("--image"); tp.add_argument("--watch", action="store_true")
    tp.add_argument("--interval", type=float, default=3)
    tp.set_defaults(fn=cmd_top)

    lg = sub.add_parser("logs"); lg.add_argument("image")
    lg.add_argument("--minutes", type=int, default=15)
    lg.set_defaults(fn=cmd_logs)

    co = sub.add_parser("cost", help="session economics")
    co.add_argument("--memory-gb", type=float, default=2)
    co.add_argument("--snapshot-gb", type=float, default=None)
    co.add_argument("--active", type=float, default=30, help="active minutes")
    co.add_argument("--suspended", type=float, default=480, help="suspended minutes")
    co.add_argument("--cycles", type=int, default=1)
    co.set_defaults(fn=cmd_cost)

    args = p.parse_args(argv)
    try:
        args.fn(args)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
