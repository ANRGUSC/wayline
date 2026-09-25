# E9: Per-task overhead of pod-based execution

## Question

Where does a task's time go when the task itself is short? Wayline runs every
task as a pod, so each task pays for dispatch, pod scheduling, container start,
Python/SDK start-up, and exit reporting before and after its own compute. Ray and
serverless-style runtimes keep warm workers and pay milliseconds for the same
steps. E9 measures that per-task overhead, phase by phase, as a function of task
duration, to decide whether warm execution (functions on long-lived runners) is
worth building and at which task sizes it matters.

## Workload

A 5-task chain of the generic sleep task (`wl-multi-odag-task`): each task reads
its input, sleeps `d` seconds, and sends a 1 KB output. Submitted as a plain
`ODAG` (no template, so the profiler plays no part).

- `d` in {0.5, 1, 2, 5, 10} s.
- Layouts: `same` (all five tasks on one node: no network) and `cross` (tasks
  alternate between two nodes: one small transfer per edge).
- Nodes: `anrg-4` and `anrg-5` by default (reachable from the control node).
- Image already cached on both nodes before measuring (the first run per node
  warms the cache and is discarded if it pulled).

## Measurement

The harness runs on `anrg-2`, watches the run's pods through `kubectl get pods -w
--output-watch-events` (event arrival on the control clock, sub-millisecond),
and after the run reads each task's SDK phase boundaries (`/timings`) and the
agent's install records (`/installed`) on the worker clock. The worker-control
clock offset is measured per node before the sweep with an NTP-style probe (a
hostNetwork pod answering `time.time()` over HTTP; minimum-RTT sample).

Phases per task (seconds):

| phase | from → to |
|---|---|
| dispatch | upstream output installed on this node (first task: run created) → pod ADDED |
| schedule | pod ADDED → bound to a node |
| start | bound → `WlTask()` constructed (container create + start + Python/SDK import; cross-clock, offset-corrected) |
| input | SDK start → last input read |
| compute | last input read → first output handoff (should equal `d`) |
| handoff | output handoff → `close()` |
| exit | `close()` → container terminated as seen by the API server (off the critical path) |
| running_report_lag | SDK start → the API server's "running" status (kubelet reporting delay; diagnostic) |
| critical_overhead | dispatch + schedule + start + input + handoff: what each task adds to a chain's makespan |

The kubelet reports container state to the API server with a delay, so the
watch's "running" event trails the process actually starting; container start is
therefore measured from binding to the SDK's own start timestamp. The next task
is dispatched when the output is installed, not when the pod terminates, so
`exit` does not lengthen a chain.

## Runs

Pilot: one run per (`d`, layout). Randomized order. More repetitions only if the
pilot's spread makes the per-phase medians unclear.

## Output

`$RES/<stamp>/`: `tasks.csv` (one row per task with every phase), `runs.csv`
(per run: phase, makespan, wall time, total compute), `offsets.json`,
`raw.json` (all watch events).

## Run

```bash
# on anrg-2, from the experiment directory
RES=~/E9-results python3 e9.py --durations 1 5 --reps 1
```
