# E8: Control-plane overhead and scaling

## Question

What resources does Wayline consume when idle, how quickly does it reconcile
object-realization requests as the number of objects and copies grows, and at
what offered load do the controller or data agents become the bottleneck?

E8 completes the overhead and scaling half of the paper's ``Correctness,
overhead, and scale'' subsection.  It is an absolute-cost and saturation
experiment, not an application-performance comparison.  It needs no external
system baseline.

## Testbed and common rules

Use the eight schedulable Intel workers (`anrg-1`, `anrg-3`--`anrg-9`) and the
controller on `anrg-2`.  Keep all Raspberry Pis cordoned.  Freeze the Wayline
controller, data-agent, SDK, template, and harness versions before the pilot;
record their commit and image digests with every dataset.

Before each trial, require:

- no non-E8 ODAG, application pod, or active data-agent transfer;
- all eight workers and all data-agent pods Ready;
- no shaping qdisc, except during Part C;
- no controller or data-agent restart since the beginning of the cell; and
- at least 10 GB free on every worker used by the cell.

Use only E8-prefixed resources and delete only those resources during cleanup.
Do not clear unrelated retained objects.  Record the pre-existing per-agent
run count and disk bytes so the idle result states the starting condition.

Collect resource samples once per second from the controller container and all
eight data-agent containers.  Read cgroup-v2 `cpu.stat`, `memory.current`, and
`memory.peak`; also poll each data agent's `/metrics` endpoint for goroutines,
heap allocation, transfer counters, bytes, and `push_inflight`.  Convert CPU
counter deltas to millicores over each sample interval.  Do not use isolated
`kubectl top` readings as the primary CPU measurement because the metrics
server refresh interval is too coarse for reconciliation bursts.

Every active trial must record:

- client request start and Kubernetes patch acknowledgement;
- patched resource version and realization generation;
- controller observation of that generation;
- first actuation request;
- every transfer start and terminal state;
- copy installation and serving-binding times for every object;
- time at which the complete requested state has converged;
- controller and per-agent CPU/RSS samples;
- patch and status-object byte sizes;
- application placements and restart counts, where Part D uses pods; and
- object sizes, digests, directed flow bytes, qdisc state, and provenance.

Report the latency decomposition as client-to-acknowledgement,
acknowledgement-to-controller-observation, observation-to-first-actuation,
actuation-to-installation, and installation-to-serving-binding.  The primary
end-to-end control metric is patch acknowledgement to convergence.

## Part A: Idle cost

Measure two idle windows, one before and one after Parts B--D.

1. Reach the common idle precondition and wait five minutes for startup work
   and metric counters to stabilize.
2. Sample for ten minutes at one-second resolution.
3. Submit no ODAG, patch, or transfer during the window.

For the controller, report median and 95th-percentile CPU millicores and median
and maximum RSS.  For data agents, report the distribution across eight nodes,
the maximum single-agent value, and the aggregate CPU and RSS.  Verify that
`push_inflight` remains zero and that traffic and request counters do not
increase.  Compare the pre- and post-campaign windows; a sustained CPU or RSS
increase greater than 10% requires investigation before reporting one idle
number.

## Part B: Scaling with named objects and desired copies

This part uses small objects so reconciliation and request processing dominate
payload transmission.  A setup container publishes digest-distinct 1 MiB
objects and keeps the run live behind a final gate.  Distribute source objects
round-robin across the eight workers.  One policy patch requests the complete
copy set for every object and selects the first requested target as that
object's serving copy.  Targets are distinct from the source and rotate across
workers.

Run these seven configurations:

| Objects (`O`) | New copies per object (`C`) | Total copy actions |
|---:|---:|---:|
| 1   | 1 | 1   |
| 8   | 1 | 8   |
| 32  | 1 | 32  |
| 128 | 1 | 128 |
| 64  | 2 | 128 |
| 32  | 4 | 128 |
| 16  | 7 | 112 |

The first four points scale object count with one requested copy.  The final
four points hold total copy work near 128 while changing how that work is
grouped among object entries.  The `C=7` point uses every worker other than an
object's source.  Run on the unshaped E0 network.

For each trial, measure patch size, maximum status size, patch-to-convergence
latency, object-level median/p95/p99 patch-to-install and patch-to-bind
latencies, successful copy actions per second, controller CPU core-seconds and
peak RSS, aggregate agent CPU core-seconds and peak RSS, and maximum
`push_inflight`.  Remove all destination copies and verify their absence before
the next trial so idempotent hits cannot shorten later cells.

## Part C: Scaling with simultaneously active transfers

This part holds transfers open long enough to obtain stable resource samples.
Preinstall digest-distinct 256 MiB objects at balanced source nodes.  For each
offered concurrency

`A in {1, 4, 8, 16, 32}`,

submit one realization patch that requests one new copy and selects it for
service for each of `A` objects.  Use distinct directed source--destination
pairs and balance them so no worker sources or receives more than four flows
at `A=32`.  Cap every selected pair at the E0-derived
`B/16 = 59 Mbit/s`; leave all other paths unshaped.  This limits each worker's
offered data rate to at most 236 Mbit/s, below its measured 942 Mbit/s NIC
capacity, so a shared NIC is not the intended bottleneck.

The harness must observe all `A` transfers in `Transferring` concurrently.  At
`A >= 4`, require that concurrency for at least five consecutive one-second
samples.  If the pilot cannot establish this overlap, increase the objects to
300 MiB; do not reduce the link rate.  Record achieved concurrent transfers,
aggregate goodput, per-transfer completion latency, patch-to-convergence
latency, controller and agent CPU/RSS, hashing/fsync time when available, and
the source and destination agent's `push_inflight` values.  Remove and verify
all shaping after every trial.

## Part D: Scaling with concurrent runs

Use a minimal two-task container DAG.  A one-second producer emits one
digest-checked 1 MiB named object and a one-second consumer receives and
verifies it locally.  Pin each producer and its consumer to the same worker,
distributing runs round-robin across all eight workers.  Give each application
pod a one-core CPU request.  Use fixed placements and no external scheduler so
this part measures admission, dependency gating, pod lifecycle, and object
bookkeeping rather than scheduling computation or network transfer.

For each offered concurrency

`R in {1, 4, 8, 16, 32, 64}`,

submit all `R` runs within a two-second window.  Measure submission-to-first-pod
latency, per-run makespan, time until all `R` runs finish, successful runs per
minute, controller CPU/RSS, aggregate agent CPU/RSS, and maximum live ODAG,
pod, and goroutine counts.  The `R=64` endpoint equals the testbed's 64 worker
cores and is the capacity endpoint; do not add a larger point unless all 64
runs start without queueing and the controller shows no saturation.

## Acceptance criteria

Every non-idle trial must satisfy all of the following:

1. The submitted patch or run is acknowledged, and every requested object or
   run converges within 300 seconds.
2. Every installed object has the expected length and SHA-256 digest.
3. No serving copy becomes active before its complete copy is installed.
4. The observed copy set contains every requested copy and no extra ready copy.
5. No controller, data-agent, or application container restarts, is OOM-killed,
   or is CPU-throttled for more than 5% of the measurement window.
6. Part D preserves every fixed placement and executes each application task
   once.
7. At least 95% of expected one-second resource samples are present for every
   measured controller or agent.
8. Measured flow pairs and bytes match the requested copies.  Part C also
   satisfies its concurrent-transfer precondition.
9. Network setup and teardown checks pass, and no E8 resource or active
   transfer remains after cleanup.

A missed event precondition, missing metric window, or harness failure makes a
trial invalid and requires a replacement.  A correctly observed timeout,
rejected request, restart, digest mismatch, premature ready marker, or failure
to converge is a Wayline failure and remains in the result.

## Scale and stopping rule

First run a pilot with three randomized blocks for every Part B, C, and D
configuration.  This is 21 Part-B trials, 15 Part-C trials, and 18 Part-D
bursts, plus the two idle windows.  Inspect the full timelines and resource
traces.  In particular, confirm that the 1 MiB Part-B objects expose controller
work, that Part C reaches the requested overlap, and that Part D's fixed
placements distribute pods evenly.

After all semantic and measurement criteria pass, freeze the harness and run
20 randomized blocks per Part B, C, and D configuration.  The paper campaign
contains 140 Part-B trials, 100 Part-C trials, and 120 Part-D bursts.  Randomize
configuration order within each part and run the parts in the order A-before,
B, C, D, A-after.

## Analysis and paper role

The main-paper result should use one compact single-column figure and a small
idle-cost table.  Plot patch-to-convergence latency and achieved operations per
second against total desired copy actions for Part B; distinguish object/copy
groupings by marker rather than color alone.  Plot controller CPU and drain
throughput against concurrent runs for Part D.  Keep the active-transfer
resource curve, tail latencies, status sizes, and full idle-agent distribution
in the appendix unless they reveal the first saturation point.

Report the largest tested load before tail latency grows sharply or throughput
stops increasing, and name the measured resource that saturates there.  If no
tested point saturates, report the result as a lower bound (for example,
``no controller saturation through 64 concurrent runs''), not as an unbounded
scalability claim.  Separate fixed per-pod Kubernetes lifecycle cost from
object reconciliation and transfer work in the interpretation.
