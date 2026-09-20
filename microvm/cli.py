"""`mvm`: the microvm-ctl command line.

    mvm bootstrap                          one-time: S3 artifact bucket + build/execution roles
    mvm quotas                             applied vs published quotas for this account
    mvm image build NAME DIR               zip -> S3 -> build -> ACTIVE version
    mvm image ls | versions NAME
    mvm run IMAGE [-n 5]                   spin up microVM(s)
    mvm ls [--image NAME]                  fleet listing
    mvm scale IMAGE N                      converge fleet to N
    mvm suspend|resume|terminate ID...     lifecycle control
    mvm drain IMAGE                        terminate the whole fleet
    mvm call ID /path [-X POST -d '{}']    authenticated request into the VM
    mvm status ID | watch ID               job telemetry from the hook runtime (/status, /events)
    mvm lease asl|policy|run               Step Functions ASL, IAM statements, one manual lease
    mvm top [--image NAME] [--watch]       live fleet dashboard
    mvm logs IMAGE                         CloudWatch tail
    mvm cost [--memory-gb 2 ...]           session economics
    mvm playground [--port 8765]           browser UI over everything above, live against AWS
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from rich.console import Console
from rich.table import Table

from microvm import __version__
from microvm.config import TPS, PlaneConfig
from microvm.endpoint import EndpointClient
from microvm.fleet import Fleet, FleetManager, IdlePolicy, applied_quotas
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


def cmd_quotas(args):
    cfg = _cfg(args)
    applied = applied_quotas(cfg)
    t = Table(title=f"Lambda MicroVMs quotas in {cfg.region}", header_style="bold magenta")
    for c in ("quota", "published default", "applied to this account", "plane throttles at"):
        t.add_column(c)
    for op, default in TPS.items():
        if op in ("GetMicrovm", "CreateMicrovmAuthToken"):
            continue
        got = applied.get(op)
        eff = (got or default) * FleetManager.QUOTA_HEADROOM
        t.add_row(f"{op} (TPS)", str(default), str(got) if got is not None else "[dim]unknown[/]",
                  f"{eff:g}/s")
    mem = applied.get("MaxMemoryGb")
    t.add_row("Max allocated microVM memory (GB)", "1024",
              str(mem) if mem is not None else "[dim]unknown[/]", "-")
    console.print(t)
    if not applied:
        console.print("[dim]Service Quotas did not answer (missing servicequotas:GetServiceQuota?); "
                      "showing published defaults.[/]")


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
    for _ in range(args.count):
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
        console.print(f"[dim]no events in {' or '.join(mon.log_groups(args.image))}[/]")
    for e in events:
        console.print(f"[dim]{e['stream'][:20]}[/] {e['message']}")


# ---------------------------------------------------------------- job telemetry
def _snapshot_table(snap: dict, title: str) -> Table:
    t = Table(title=title, header_style="bold magenta", show_header=False)
    t.add_column("field", style="cyan")
    t.add_column("value")
    prog = snap.get("progress") or {}
    done, total = prog.get("done"), prog.get("total")
    t.add_row("phase", str(snap.get("phase") or "-"))
    t.add_row("elapsed", f"{snap.get('elapsed_s', 0):.0f} s")
    t.add_row("progress", f"{done}/{total}" if total else str(done if done is not None else "-"))
    for k, v in sorted((snap.get("counters") or {}).items()):
        t.add_row(f"counter {k}", str(v))
    lease = snap.get("lease")
    if lease:
        state = "done" if lease.get("done") else ("lost" if lease.get("lost") else "active")
        err = lease.get("error")
        colour = "red" if err else "green"
        t.add_row("lease", f"{lease.get('kind')}  {lease.get('id') or ''}  "
                           f"heartbeats={lease.get('heartbeats', 0)}  [{colour}]{state}[/]")
        if err:
            t.add_row("error", f"[red]{err.get('error_type')}[/] {err.get('message')}")
    else:
        t.add_row("lease", "[dim]none[/]")
    return t


def _log_line(entry: dict) -> str:
    level = entry.get("level", "info")
    style = {"error": "red", "warning": "yellow", "warn": "yellow"}.get(level, "dim")
    extra = {k: v for k, v in entry.items() if k not in ("t", "level", "msg", "phase", "microvm_id", "seq")}
    tail = f"  [dim]{json.dumps(extra, default=str)}[/]" if extra else ""
    return f"[{style}]{level:<7}[/] [cyan]{entry.get('phase') or ''}[/] {entry.get('msg', '')}{tail}"


def cmd_status(args):
    client = EndpointClient(_cfg(args), args.id, ports=[args.port] if args.port else None)
    snap = client.status()
    console.print(_snapshot_table(snap, f"job on {args.id}"))
    for entry in snap.get("log_tail") or []:
        console.print(_log_line(entry) if isinstance(entry, dict) else str(entry))


def cmd_watch(args):
    from rich.live import Live
    client = EndpointClient(_cfg(args), args.id, ports=[args.port] if args.port else None)
    snap = client.status()
    with Live(_snapshot_table(snap, f"job on {args.id}"), refresh_per_second=2, console=console) as live:
        for obj in client.watch(timeout=args.timeout):
            if not isinstance(obj, dict):
                continue
            if "msg" in obj:
                live.console.print(_log_line(obj))
            else:
                live.update(_snapshot_table(obj, f"job on {args.id}"))


# ---------------------------------------------------------------- leases
def _lease_policy(args):
    from microvm.lease import LeasePolicy
    return LeasePolicy(budget_s=args.budget, heartbeat_timeout_s=args.heartbeat, slack_s=args.slack)


def _image_arn_offline(image: str, region: str, role_arn: str | None, profile) -> str:
    """Resolve a bare image name without calling STS when the account is readable
    from the execution role ARN, so `mvm lease asl` works without credentials."""
    from microvm.client import image_arn
    if image.startswith("arn:"):
        return image
    parts = (role_arn or "").split(":")
    if role_arn and role_arn.startswith("arn:") and len(parts) > 4 and parts[4]:
        return f"arn:aws:lambda:{region}:{parts[4]}:microvm-image:{image}"
    return image_arn(image, region, profile)


def cmd_lease_asl(args):
    from microvm.integrations.stepfunctions import lease_state_machine
    cfg = _cfg(args)
    role = args.execution_role or cfg.execution_role_arn
    if not role:
        sys.exit("mvm lease asl: --execution-role ARN (or MVM_EXECUTION_ROLE_ARN) is required")
    asl = lease_state_machine(
        image_arn=_image_arn_offline(args.image, cfg.region, role, cfg.profile),
        execution_role_arn=role, policy=_lease_policy(args), region=cfg.region,
        name=args.name, task_expr=args.task_expr, heartbeat_s=args.heartbeat_every,
    )
    print(json.dumps(asl, indent=2))


def cmd_lease_policy(args):
    from microvm.integrations.stepfunctions import iam_statements, orchestrator_statements, policy_document
    cfg = _cfg(args)
    out = {
        "execution_role": policy_document(iam_statements(args.kind, orchestrator_arn=args.orchestrator)),
        "orchestrator_role": policy_document(
            orchestrator_statements(args.execution_role or cfg.execution_role_arn)),
    }
    print(json.dumps(out, indent=2))


def cmd_lease_run(args):
    from microvm.lease import Lease
    cfg = _cfg(args)
    if args.kind != "none" and not args.token:
        sys.exit(f"mvm lease run: --token is required for kind {args.kind}")
    try:
        task = json.loads(args.task) if args.task else {}
    except ValueError as e:
        sys.exit(f"mvm lease run: --task is not valid JSON: {e}")
    lease = Lease(kind=args.kind, token=args.token or "", region=cfg.region, target=args.target,
                  heartbeat_s=args.heartbeat_every, id=args.id)
    fm = FleetManager(cfg)
    vm = fm.lease(args.image, lease, task, _lease_policy(args), version=args.version,
                  execution_role=args.execution_role)
    console.print(
        f"[bold green]✓[/] {vm.microvm_id}  {_state(vm.state)}  "
        f"[link=https://{vm.endpoint}]{vm.endpoint}[/link]  lease {lease.kind}"
    )
    if args.wait:
        vm = fm.wait_until(vm.microvm_id, "RUNNING")
        console.print(f"  now {_state(vm.state)}; follow it with: mvm watch {vm.microvm_id}")


def cmd_playground(args):
    from microvm.playground import serve
    serve(host=args.host, port=args.port, dry_run=args.dry_run, open_browser=not args.no_open, cfg=_cfg(args))


def cmd_cost(args):
    model = CostModel(memory_gb=args.memory_gb, snapshot_gb=args.snapshot_gb)
    s = model.session(args.active * 60, args.suspended * 60, args.cycles)
    t = Table(title=f"session economics: {args.memory_gb} GB / {model.vcpu:g} vCPU",
              header_style="bold magenta")
    t.add_column("dimension")
    t.add_column("USD", justify="right")
    t.add_row("running compute", f"${s['running_usd']:.6f}")
    t.add_row(f"suspend cycles x{args.cycles}", f"${s['suspend_cycles_usd']:.6f}")
    t.add_row("suspended storage", f"${s['suspended_storage_usd']:.6f}")
    t.add_row("[bold]total[/]", f"[bold]${s['total_usd']:.6f}[/]")
    t.add_row("[dim]always-on equivalent[/]", f"[dim]${s['vs_always_on_usd']:.6f}[/]")
    t.add_row("[bold green]savings[/]", f"[bold green]{s['savings_pct']}%[/]")
    console.print(t)


# ---------------------------------------------------------------- parser
def main(argv: list[str] | None = None):
    p = argparse.ArgumentParser(prog="mvm",
                                description="Control and execution plane for AWS Lambda MicroVMs")
    p.add_argument("--version", action="version", version=f"microvm-ctl {__version__}")
    p.add_argument("--region", default=None, help="service region (default: MVM_REGION or us-east-1)")
    p.add_argument("--profile", default=None, help="AWS profile (default: MVM_PROFILE or AWS_PROFILE)")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bootstrap", help="provision S3 bucket + IAM roles")
    b.add_argument("--prefix", default="microvm-ctl", help="name prefix for the bucket and roles")
    b.set_defaults(fn=cmd_bootstrap)

    q = sub.add_parser("quotas", help="applied vs published quotas for this account")
    q.set_defaults(fn=cmd_quotas)

    img = sub.add_parser("image", help="image factory").add_subparsers(dest="sub", required=True)
    ib = img.add_parser("build", help="zip -> S3 -> build -> ACTIVE version")
    ib.add_argument("name")
    ib.add_argument("dir")
    ib.add_argument("--memory", type=int, default=2048, help="minimum memory in MiB")
    ib.add_argument("--env", action="append", metavar="K=V",
                    help="image-level env var (shared by every clone)")
    ib.add_argument("--caps-all", action="store_true",
                    help="additionalOsCapabilities=ALL (EFS/FUSE/containerd/eBPF)")
    ib.add_argument("--description")
    ib.set_defaults(fn=cmd_image_build)
    il = img.add_parser("ls", help="list images")
    il.set_defaults(fn=cmd_image_ls)
    iv = img.add_parser("versions", help="list versions of one image")
    iv.add_argument("name")
    iv.set_defaults(fn=cmd_image_versions)

    r = sub.add_parser("run", help="spin up microVM(s)")
    r.add_argument("image")
    r.add_argument("--version", help="image version (default: latest ACTIVE)")
    r.add_argument("-n", "--count", type=int, default=1)
    r.add_argument("--idle", type=int, default=300, help="suspend after N idle seconds")
    r.add_argument("--suspended-ttl", type=int, default=3600, help="auto-terminate after N suspended seconds")
    r.add_argument("--payload", help="runHookPayload (per-VM context)")
    r.add_argument("--max-duration", type=int, default=None, help="hard lifetime cap in seconds")
    r.add_argument("--wait", action="store_true", help="poll until RUNNING")
    r.set_defaults(fn=cmd_run)

    ls = sub.add_parser("ls", help="list microVMs")
    ls.add_argument("--image")
    ls.add_argument("--all", action="store_true", help="include TERMINATED")
    ls.set_defaults(fn=cmd_ls)

    g = sub.add_parser("get", help="raw GetMicrovm JSON")
    g.add_argument("id")
    g.set_defaults(fn=cmd_get)

    for verb in ("suspend", "resume", "terminate"):
        v = sub.add_parser(verb, help=f"{verb} one or more microVMs")
        v.add_argument("ids", nargs="+")
        v.set_defaults(fn=lambda a, _v=verb: _lifecycle(a, _v))

    sc = sub.add_parser("scale", help="converge fleet to N")
    sc.add_argument("image")
    sc.add_argument("n", type=int)
    sc.add_argument("--idle", type=int, default=300)
    sc.add_argument("--wait", action="store_true", help="wait for new members to reach RUNNING")
    sc.set_defaults(fn=cmd_scale)

    d = sub.add_parser("drain", help="terminate every microVM of an image")
    d.add_argument("image")
    d.set_defaults(fn=cmd_drain)

    c = sub.add_parser("call", help="authenticated request into a microVM")
    c.add_argument("id")
    c.add_argument("path")
    c.add_argument("-X", "--method", default="GET")
    c.add_argument("-d", "--data", help="request body (sent as application/json)")
    c.add_argument("--port", type=int, default=None, help="non-default app port")
    c.set_defaults(fn=cmd_call)

    st = sub.add_parser("status", help="job snapshot from the hook runtime (GET /status)")
    st.add_argument("id")
    st.add_argument("--port", type=int, default=None, help="non-default app port")
    st.set_defaults(fn=cmd_status)

    w = sub.add_parser("watch", help="live job table plus streamed log lines (GET /events)")
    w.add_argument("id")
    w.add_argument("--port", type=int, default=None, help="non-default app port")
    w.add_argument("--timeout", type=float, default=600, help="stop after N seconds")
    w.set_defaults(fn=cmd_watch)

    le = sub.add_parser("lease", help="hand a VM a task through a lease").add_subparsers(
        dest="sub", required=True)

    def _policy_flags(sp):
        sp.add_argument("--budget", type=int, default=900,
                        help="seconds the orchestrator waits (task timeout)")
        sp.add_argument("--heartbeat", type=int, default=120, help="heartbeat timeout in seconds")
        sp.add_argument("--slack", type=int, default=120, help="VM outlives the budget by this many seconds")
        sp.add_argument("--heartbeat-every", type=int, default=30, help="seconds between VM heartbeats")

    la = le.add_parser("asl", help="print the Step Functions state machine (JSONata) for one lease")
    la.add_argument("--image", required=True, help="image name or ARN")
    la.add_argument("--execution-role", default=None,
                    help="VM execution role ARN (default: MVM_EXECUTION_ROLE_ARN)")
    la.add_argument("--name", default="Lease", help="name of the lease state")
    la.add_argument("--task-expr", default="$states.input", help="JSONata expression for the task")
    _policy_flags(la)
    la.set_defaults(fn=cmd_lease_asl)

    lp = le.add_parser("policy", help="print the IAM statements for the VM role and the orchestrator role")
    lp.add_argument("--kind", required=True, choices=["sfn", "durable", "sqs", "eventbridge", "http"])
    lp.add_argument("--orchestrator", default=None,
                    help="state machine, function, queue, or bus ARN the VM completes to")
    lp.add_argument("--execution-role", default=None, help="VM execution role for iam:PassRole")
    lp.set_defaults(fn=cmd_lease_policy)

    lr = le.add_parser("run", help="launch one lease by hand (manual tests)")
    lr.add_argument("image")
    lr.add_argument("--kind", required=True, choices=["sfn", "durable", "http", "sqs", "eventbridge", "none"])
    lr.add_argument("--token", default=None, help="task token, callback id, or bearer (not for kind none)")
    lr.add_argument("--target", default=None, help="http URL, SQS queue URL, or event bus name")
    lr.add_argument("--task", default=None, help="task JSON (pointers, not bodies; 4096 chars total)")
    lr.add_argument("--id", default=None, help="human label carried as lease.id")
    lr.add_argument("--version", help="image version (default: latest ACTIVE)")
    lr.add_argument("--execution-role", default=None)
    lr.add_argument("--wait", action="store_true", help="poll until RUNNING")
    _policy_flags(lr)
    lr.set_defaults(fn=cmd_lease_run)

    tp = sub.add_parser("top", help="live fleet dashboard")
    tp.add_argument("--image")
    tp.add_argument("--watch", action="store_true")
    tp.add_argument("--interval", type=float, default=3)
    tp.set_defaults(fn=cmd_top)

    lg = sub.add_parser("logs", help="tail CloudWatch logs for an image (/aws/lambda-microvms/<image>)")
    lg.add_argument("image")
    lg.add_argument("--minutes", type=int, default=15)
    lg.set_defaults(fn=cmd_logs)

    co = sub.add_parser("cost", help="session economics")
    co.add_argument("--memory-gb", type=float, default=2)
    co.add_argument("--snapshot-gb", type=float, default=None,
                    help="suspended snapshot size (default: memory)")
    co.add_argument("--active", type=float, default=30, help="active minutes")
    co.add_argument("--suspended", type=float, default=480, help="suspended minutes")
    co.add_argument("--cycles", type=int, default=1, help="suspend/resume cycles")
    co.set_defaults(fn=cmd_cost)

    pgp = sub.add_parser("playground", help="local browser UI that drives the SDK against the live service")
    pgp.add_argument("--host", default="127.0.0.1")
    pgp.add_argument("--port", type=int, default=8765)
    pgp.add_argument("--dry-run", action="store_true",
                     help="start with dry run on: mutating actions are recorded, not sent")
    pgp.add_argument("--no-open", action="store_true", help="do not open a browser tab")
    pgp.set_defaults(fn=cmd_playground)

    args = p.parse_args(argv)
    try:
        args.fn(args)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
