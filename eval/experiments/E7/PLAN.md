# E7: Reconciliation safety under faults and superseding revisions

## Question

Can Wayline converge a live object's physical realization safely when policy
requests repeat or change and when the controller or a data agent fails during
an active transfer?

This experiment supports the safe-actuation claim. It is not a comparison of
scheduling policies and needs no external system baseline: the relevant
evidence is whether Wayline preserves the object invariants promised by its
own contract under each disturbance.

## Common workload

Use one fixed DAG and placement in every arm:

- `produce` on `anrg-3`: compute for 5 seconds and emit the named object
  `produce.payload`, exactly 300 MiB;
- a pod-less `serve` vertex, initially bound to `anrg-3`;
- three 2-second consumers on `anrg-6`, `anrg-8`, and `anrg-9`;
- a 1-second report task on `anrg-1`;
- `anrg-7` is the first revision target and `anrg-5` is the alternate target;
  neither target may run an application container.

Every consumer verifies the payload length and SHA-256 digest. The report
verifies the three consumer results. Hold the DAG, container images, task
placement, and per-node order fixed across all arms.

## Network treatment

E0 measured `B = 942 Mbit/s`. Cap data-agent traffic from `anrg-3` to
`anrg-5`, `anrg-6`, `anrg-7`, `anrg-8`, and `anrg-9` at `B/16 = 59 Mbit/s`.
Leave every other directed path unshaped. This gives a roughly 43-second
window in which a 300 MiB transfer is active, while a revised serving point
can fan out over the measured 1 GbE fabric.

Apply shaping before each run, verify all five 59 Mbit/s classes live, and
remove and verify all qdiscs after each run. Set the run deadline to 300
seconds; do not shorten transport deadlines to manufacture failures.

## Event triggers

Drive injections from observed state, not wall-clock guesses:

1. Wait until `produce.payload` is `Installed` on `anrg-3` and at least one
   original fan-out is `Transferring`.
2. For arms that revise to `anrg-7`, patch the live run to request a copy on
   `anrg-7` and select it as `servingCopy`.
3. Wait until the revision transfer `anrg-3 -> anrg-7` is reported
   `Transferring`, then apply the arm-specific disturbance.

Record the timestamp and observed precondition for every injection. A run is
invalid, rather than a failed Wayline run, if the harness never observes the
required precondition.

## Arms

Randomize arm order within each block.

| Arm | Disturbance and expected final state |
|---|---|
| `revision-control` | Apply one revision to `anrg-7` and inject no fault. Final serving copy is `anrg-7`. |
| `repeat-identical` | Once the `3 -> 7` copy is active, apply the identical desired realization five more times, 200 ms apart. The copy must not be repeatedly restarted; final serving copy is `anrg-7`. |
| `controller-restart` | Force-delete the controller pod while the `3 -> 7` copy is active and wait for its replacement to become Ready. The live run must be rediscovered and converge to serving from `anrg-7`. |
| `source-agent-restart` | Force-delete the data-agent pod on `anrg-3` while the `3 -> 7` copy is active and wait for its DaemonSet replacement. Durable transfer state must recover and converge to `anrg-7`. |
| `target-agent-restart` | Force-delete the data-agent pod on `anrg-7` while receiving the active copy. No partial payload may become ready; retry must eventually install the complete object and bind service to `anrg-7`. |
| `superseding-revision` | While `3 -> 7` is active, issue a newer revision requesting only a copy on `anrg-5`, selecting `anrg-5` for service, and evicting `anrg-7`. The older reconcile must stop. Final service is from `anrg-5`, and `anrg-7` is absent or `Evicted`. |
| `conflicting-request` | Submit one entry that lists `anrg-7` in both `copies` and `evict`, with no serving override. The entry must be refused, the source copy on `anrg-3` must remain valid, and the original realization must complete. |
| `last-copy-eviction` | Request eviction of `anrg-3` before any other copy is installed. The eviction must be refused while consumers remain, and the original realization must complete. |

For restart arms, use the Kubernetes pod UID to prove that the injected pod
was replaced. Do not count a naturally occurring restart as the injection.

## Measurements

Record for every run:

- run phase and completion time;
- every spec generation, patch body, and patch timestamp;
- controller and data-agent pod UIDs before and after injection;
- copy and serving state over time from the run object;
- every transfer state transition and source/destination pair;
- successful and attempted bytes per directed pair;
- consumer dispatch times, payload sizes, and digests;
- application placement, starts, and restart count;
- fault-to-recovery and last-patch-to-convergence latency;
- qdisc verification and teardown state; and
- controller, agent, template, and harness commit/image provenance.

For `target-agent-restart`, poll the target during recovery and record whether
`.wl-ready` exists together with the observed byte count and digest. A ready
marker over a partial or mismatched payload is a safety failure.

## Per-run acceptance criteria

All eight arms must satisfy the following unless an arm states a stronger
condition:

1. The run reaches `Succeeded` within 300 seconds.
2. All three consumers and the report verify the expected digest.
3. No consumer is dispatched from a revised serving point before a complete,
   digest-valid copy is installed there.
4. No partial object is ever marked ready.
5. Each application task executes once, with zero application restarts and
   no placement or order change.
6. No application container runs on `anrg-5` or `anrg-7`.
7. The final serving and copy state matches the newest valid request.
8. A superseded generation performs no action after the newer generation has
   converged; in particular, `anrg-7` cannot become the serving copy or retain
   a ready copy at the end of `superseding-revision`.
9. `conflicting-request` preserves the source copy and records an explicit
   refusal.
10. `last-copy-eviction` preserves the last required copy and records an
    explicit refusal.
11. Network treatment and teardown checks pass.

`repeat-identical` additionally requires one successful installation on
`anrg-7`. Report attempted bytes separately so redundant partial retransfers
cannot be hidden by eventual idempotence. Treat repeated resetting of the
active transfer as a failed pilot that requires a system fix.

## Scale and stopping rule

First run a pilot of 3 randomized blocks x 8 arms = 24 runs. Inspect every
timeline; do not scale a failing semantic case. After all criteria pass,
freeze the harness and run 20 blocks x 8 arms = 160 runs.

This is a race-oriented correctness campaign, so the paper should report
success counts for every arm plus median and tail recovery latency. A compact
table is the primary presentation. Do not turn completion-time differences
between fault types into a performance ranking.

## Paper role

This campaign completes the `Correctness, overhead, and scale` subsection's
correctness half. It demonstrates that live realization is a reconciled
contract rather than a one-shot copy API. E8 should separately measure idle
overhead and scaling with runs, objects, copies, and transfers.
