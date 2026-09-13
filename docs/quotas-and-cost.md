# Quotas and cost

## The quota walls

A fresh account does not get the published defaults. Ours, measured with `mvm quotas` on the day we started:

| Quota | Published default | Applied to a fresh account |
|---|---|---|
| `RunMicrovm` rate | 5 per second | 1 per second |
| `SuspendMicrovm` rate | 2 per second | 2 per second |
| `ResumeMicrovm` rate | 5 per second | 5 per second |
| `TerminateMicrovm` rate | 10 per second | 10 per second |
| Max allocated microVM memory | 1,024 GB | 8 GB |

![mvm quotas](img/mvm-quotas.png)

Two things count against the memory quota that you might not expect:

- **Image-build VMs.** Five concurrent 2 GB builds consumed 10 GB of a quota we did not have, and the next launch failed with `ServiceQuotaExceededException`.
- **TERMINATING VMs.** For a short window after `TerminateMicrovm` the memory is still allocated. Tests that churn VMs quickly must let terminations settle.

Both lessons are encoded in the plane. `FleetManager` reads the applied values at startup and throttles to 80% of them. `Throttled` waits much longer on `ServiceQuotaExceededException` than on a TPS throttle, because capacity frees up on the order of minutes. `Fleet.scale_to` terminates suspended members first on the way down, and the benchmark harness waits for terminations to settle before its scale section.

We filed a `RunMicrovm` raise from 1 to 5 per second with a single `request-service-quota-increase` call. The case closed with the applied value still at 1 per second, so every fleet number in this repo was produced under that limit. File yours on day one and plan for the answer to take time.

## What the rates are

Published `us-east-1` launch rates, per-second billing, vCPU fixed at memory divided by 2:

| Meter | Rate |
|---|---|
| vCPU | $0.0000276944 per vCPU-second |
| Memory | $0.0000036667 per GB-second |
| Snapshot write | $0.0038 per GB |
| Snapshot read | $0.00155 per GB |
| Suspended and image storage | $0.08 per GB-month |

These live as constants in `microvm/monitor.py` and feed `mvm cost` and `CostModel`. Re-verify them against the pricing page before quoting anyone.

## Worked examples

All on a 2 GB / 1 vCPU VM with the measured 0.61 GB memory snapshot of the code-sandbox image.

| Session shape | MicroVM | Always-on 2 GB | Saved |
|---|---|---|---|
| 8 second one-shot job, terminate | $0.0003 | n/a | n/a |
| 30 min active + 8 h suspended | $0.0669 | $1.0719 | 93.8% |
| 2 h active + 22 h suspended | $0.2602 | $3.0264 | 91.4% |
| Running 24 hours | n/a | $3.03 per day | the shape where Fargate wins |

![mvm cost](img/mvm-cost.png)

## The levers, in order of impact

1. **Suspend ratio.** A VM that is suspended most of the day bills as storage most of the day. This is the whole economic case.
2. **Terminate one-shots.** A suspend and resume cycle on a 0.61 GB snapshot costs about $0.0034 in snapshot write plus read, more than ten times the compute cost of an 8 second job. If nobody is coming back, terminate.
3. **Do not thrash.** An idle timeout aggressive enough to suspend and wake every minute pays that cycle cost every minute. Interactive workloads want a generous `--idle`.
4. **Right-size the baseline.** vCPU is tied to memory, so a 2 GB VM is 1 vCPU. Pick the smallest memory that fits the working set, and remember the memory quota counts suspended VMs too.
5. **Keep snapshots small.** Snapshot size sets resume time, cycle cost, and storage cost. Lean images (WeasyPrint rather than headless Chromium, for example) pay off three times.

## Idle detection details

Idle detection keys off endpoint traffic in both directions. A user reading results generates no requests and gets suspended. A frontend that polls a health route every 30 seconds keeps the VM billing forever. Keep health checks outside the idle window.

`suspendedDurationSeconds` is an auto-terminate timer. When it elapses the VM is destroyed with its state. Size it to the longest absence you want to survive, inside the 8 hour total lifetime ceiling.
