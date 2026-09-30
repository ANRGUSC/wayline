# E16: plan-driven copies under loss and link degradation

Agreed 2026-09-29. Tests the thesis that the plan should decide where each
output's copies live, not only where tasks run, and that the runtime should
keep both true on a live run.

## What the evaluation must show

1. **Survival.** Runs survive losses that plan-only placement cannot survive:
   data pinned to a lost node (sensor captures) is gone unless copied.
2. **Cost.** When nothing fails, the protection costs less than static
   replication (fewer bytes, no slowdown on the critical path).
3. **Recovery time.** Recovery is faster than lineage re-execution when
   producers are expensive.
4. **Adaptation.** The runtime reacts to events the plan did not anticipate
   (a link degrading after planning, a second loss), which copies fixed at
   plan time cannot.

## Workloads

| Workload | Shape | Why |
|---|---|---|
| GDTM | 29 tasks, mixed warm/pod, 3 pinned sensors, captures about 1.6 MB | real pipeline; small pinned data |
| iot | 14 tasks, 4 pinned cameras, captures 80 to 150 MB | copy cost is not free |
| FOCAL | 7 tasks, 2 pinned sources (small samples), cheap producers (under 3 s) | negative control for recovery time: losing a non-source node costs lineage under a second, so W should match plan-only; losing a source node still needs the copied samples |
| Synthetic | E11 generator, swept | map where the thesis holds and where each baseline catches up |

Synthetic sweep axes: producer cost, object size, link spread (B to B/8
versus uniform), fraction of co-located chains, fraction of pinned sources
(including none: the case where no loss is unrecoverable).

## Conditions

- Network: E11 CPU classes with E13 link classes; uniform network as control.
- Faults:
  - none (overhead);
  - node crash (`fault.py crash`): a sensor node, or a compute node holding a
    co-located chain, at early, middle, and late stages;
  - partition (`fault.py partition`), healed after the run;
  - link degraded after planning (`fault.py link <node> <mbit>`).

## Arms

| Arm | Stands for | Configuration |
|---|---|---|
| W | the thesis | `{mode: planAware, revise: true}`: plan copies, link re-planning, re-protection after a repair |
| W-plan-only | Jupiter-style placement, lineage recovery | `{mode: none, revise: false}` |
| W-static | Arvados replication level / Rucio rule | `{mode: static, replicas: 2, revise: false}`: rendezvous-hash placement, plan-blind, written at once |
| W-frozen | Iridium-style plan-time copies | `{mode: planAware, revise: false}` |
| W-replan (extra) | Tactical-Jupiter-style re-planning without copy control | `{mode: none, revise: true}` |
| Ray | plan-pinned placement, lineage reconstruction | Ray on the same nodes, tasks pinned to the plan (soft), sources hard-pinned |
| Ray + app copies | a Ray user emulating copies in application code | as Ray, plus copy tasks and fallback logic in the driver |
| Argo + S3 | central artifact repository | Argo workflow, outputs in an S3 repository (`rclone serve s3`; MinIO's images are no longer published), retry on another node |

The W arms form copies (none, static, plan) x adaptation (off, on). Node-loss
recovery (re-placing lost tasks, lineage when no copy survives) is on in every
arm; a run would otherwise hang.

The four W arms share one runtime and isolate the policy. Ray and Argo are the
real systems a reader will compare against; they differ in more than the
policy, which the analysis must say.

## Metrics

- completion rate (share of runs that finish);
- completion time, and its ratio to the fault-free prediction;
- recovery time: fault to the affected consumer's start;
- producers re-run;
- extra bytes moved (copies, re-runs, re-sourcing);
- fault-free overhead (time and bytes);
- for Ray + app copies: application lines needed, and what cannot be expressed
  (serving-copy choice, eviction, per-object durability).

## Order of work

1. W-static and W-frozen in the controller; re-protection after repair for W.
2. Campaign harness (`e16.py`): workload x arm x network x fault x repetition,
   fault injection, metric collection, results as JSON.
3. Pilot: the four W arms on GDTM and iot, crash sweep and no fault, three
   repetitions. Go on only if the gaps exceed the run-to-run spread.
4. Ray ports (GDTM, iot, FOCAL, synthetic), then Ray + app copies.
5. Argo + S3 repository ports.
6. Full campaign.
