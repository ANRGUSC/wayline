# E16 notes: headline results so far

Not paper-final. Small runs to reason about before the full campaign
(PLAN.md). E13 link classes, default clocks, one controller build per round.
Results live on anrg-2 under `~/E16-results/<tag>/`; `analyze_e16.py <dir>`
prints the tables below. Completion is wall time from submission to the
run's terminal phase.

## Survival: sensor node crashed mid-run (headline, 1 run per cell, controller recover4)

| | W | W-frozen | W-static | W-plan-only |
|---|---|---|---|---|
| GDTM | 38.5 s, 57 MB copied | 39.2 s, 24 MB | 41.1 s, 302 MB | Failed: `source-n2` cannot run elsewhere |
| iot | 50.7 s, 600 MB | 50.3 s, 600 MB | 63.9 s, 750 MB | Failed: `capture-4` cannot run elsewhere |

- Without copies, losing a sensor node ends the run: the pinned capture is
  gone and its producer cannot run on any other node.
- Loss detected 4 to 6 s after the crash; interrupted tasks restarted 6 to
  7 s after it with plan copies. W-static restarted in 8.8 s (GDTM) and
  15.6 s (iot); on iot it had no surviving copy of the preprocess output
  finished just before the crash and re-ran it.
- Predates the copy-spreading fix (recover6); iot W's copies all went to one
  node.

## Recovery time: expensive co-located producer's node crashed (headline, 1 run)

| | W | W-frozen | W-static | W-plan-only |
|---|---|---|---|---|
| chain | 47.1 s | 49.4 s | 46.4 s | 60.6 s (re-ran the 12 s producer) |

Interrupted task restarted 6.5 s after the crash with a copy, 19.7 s with
lineage. W-static matched W here: its hash-placed copy was on a surviving
node that recovery could move the consumer to.

## Overhead with no fault (round2, 3 runs per cell, controller recover6)

| | W | W-plan-only | W-static |
|---|---|---|---|
| GDTM completion, mean (min to max) | 31.5 s (28.3 to 33.3) | 31.5 s (30.4 to 32.3) | 31.9 s (30.8 to 33.5) |
| GDTM bytes copied | 27 MB | 0 | 309 MB |
| iot completion | 42.0 s (40.0 to 44.8) | 40.8 s (40.5 to 40.9) | 40.2 s (38.2 to 41.7) |
| iot bytes copied | 900 MB | 0 | 900 MB |

- No time overhead resolvable at three runs: the ranges overlap.
- Bytes: plan-aware copies 11x fewer bytes than the static rule on GDTM;
  on iot both copy 900 MB, because nearly every iot object is a pinned
  camera chain the plan-aware policy protects anyway.
- W and W-frozen are the same policy when nothing fails (not repeated).

## Adaptation: link throttled after planning

Headline (chain-link, controller recover4/5): arms that re-plan finished in
38 to 47 s, arms that do not waited out the transfer (99 to 117 s). The gain
there is compute re-planning (the consumer moved to its input); no second
copy existed.

Round2 (fan, a second copy on another consumer's node): W served the waiting
consumer from that copy over a healthy link and kept it on its fast node.
Two artifacts, both fixed and re-run in round3:
- W-replan ran right after W-plan-only and inherited that run's evidence that
  the link was degraded (controller-wide, 60 s), although the harness had
  restored the link in between. It re-planned 1 s after the producer
  finished, before the second copy existed, and moved the consumer instead.
  The headline adaptation cell had the same ordering. Fix: 65 s cooldown
  between link-fault runs (e16.py).
- W sent the 30 MB copy twice: a reconcile pass checked readiness, then saw
  the first copy's transfer finished and took it for failed. Fix: re-check
  readiness before re-sending (realization.go, recover7).

Round3 (controller recover7, 65 s cooldown between link-fault runs, 1 run per cell):

| | W | W-replan | W-frozen | W-plan-only |
|---|---|---|---|---|
| chain-link (consumer moves to its input) | 47.0 s | 47.9 s | 100.0 s | 100.5 s |
| fan (consumer served from another copy) | 44.5 s | 43.7 s | 100.1 s | 100.1 s |

- Adapting arms reacted 18 to 20 s after the fault (the transfer must run
  more than twice its prediction plus 2 s, then staying must look worse
  than moving); the others waited about a minute for the throttled transfer.
- In fan both adapting arms re-served the consumer from the second copy with
  one 30 MB transfer (no duplicate) and kept it on its fast node.
- W equals W-replan in both cells: the gain comes from runtime adaptation
  (moving the consumer, or serving it from another copy), which does not
  depend on the plan-aware copy policy. The policy's own contribution shows
  under loss (survival, recovery time), not here.

## Ray (ray1, 1 run per cell; plan and copies taken from Wayline's runs)

Ray is pinned to the plan Wayline used (soft affinity; pinned sources hard).
"Ray + copies" adds application-level copies of exactly the objects W's
policy copied, on the same nodes, and a fallback in every consumer (7 marked
code sites in `ray/e16_ray.py`; the copy decisions come from Wayline, Ray has
nothing to compute them with).

| | Ray | Ray + copies | Wayline W | Wayline W-plan-only |
|---|---|---|---|---|
| iot, no fault | 23.0 s | 23.2 s | 42.0 s (mean of 3) | 40.8 s (mean of 3) |
| iot, sensor node crashed | Failed: capture-4 unschedulable (pinned to the lost node) | 30.1 s | 50.7 s | Failed |
| chain, co-located producer's node crashed | 54.1 s (lineage re-ran p) | 41.3 s | 47.1 s | 60.6 s |

- Survival: plain Ray fails exactly like W-plan-only; Ray with application
  copies survives, like W.
- Absolute times are not comparable: the iot and chain templates run every
  Wayline task as a pod (creation, image check, data install and push),
  while Ray runs every task on a warm worker. Ray's 23 s for iot is the
  critical path's compute; Wayline's 41 s adds about 17 s of pod dispatch
  and handoff. A fair comparison needs these workloads on Wayline's warm
  runners.
- A chain run first taken as "Ray + copies" had no copies (the harness read
  W's decisions after re-protection had overwritten them); it is kept as
  `recovery-chain-Ray+copies.nocopies.json` and was re-run.

## Wayline on warm runners vs Ray (warm1; iot overhead 3 runs per arm, faults 1 run)

The same iot and chain tasks as calls on Wayline warm runners
(`runner/Dockerfile.iot`, `runner/runners.yml`), the dispatch Ray uses:

| | Wayline W (warm) | W-plan-only (warm) | Ray | Ray + app copies |
|---|---|---|---|---|
| iot, no fault | 24.2 s (24.0 to 24.5) | 23.9 s (23.7 to 24.0) | 23.0 s | 23.2 s |
| iot, sensor node crashed | 37.8 s | Failed (capture-4 stuck) | Failed | 30.1 s |
| chain, co-located producer's node crashed | 44.4 s | 55.4 s (re-ran p) | 54.1 s (lineage) | 41.3 s |

- On equal dispatch the two runtimes are level without faults: the 17 s gap
  of the pod variants was pod dispatch, not copies or the data plane.
- Under a crash, Wayline with plan copies finishes 3 to 8 s behind Ray with
  hand-written copies (which were given Wayline's copy decisions), and
  ahead of Ray's lineage recovery by 10 s on the chain.
- The iot-warm plan copies were spread over several fallbacks (recover6):
  750 MB in 9 copies.

## GDTM on Ray (ray-gdtm, 1 run per cell)

`examples/gdtm/Dockerfile.ray`: the model image plus the segment, Ray 2.58.0
and `ray/e16_ray_gdtm.py` (one Python for the whole cluster; built on anrg-6
with a BuildKit pod, `ray/buildkit.yml`). Head on anrg-8, doubling as its
worker. Same stage code as the Wayline stages; models cached per Ray worker.

| | Ray | Ray + app copies (17 objects Wayline's W copied) |
|---|---|---|
| GDTM, no fault | 17.5 s | 22.8 s |
| GDTM, sensor node crashed | Failed: the lost chain unschedulable (source pinned) | 31.0 s |

- The emulated copies cost Ray about 5 s without a fault: they start as
  soon as each object exists and compete with the critical path's
  transfers, and consumers with a copy to fall back to fetch their inputs
  themselves instead of having Ray prefetch them. Wayline defers its plan
  copies until the object's own transfers are done.
- Compare with Wayline's GDTM warm variant (below), not the mixed one (its
  embeds are pods that load the model each time).

## GDTM on warm runners vs Ray (warm1 and ray-gdtm, 1 run per cell)

GDTM's warm mode (every stage a call on the gdtm / gdtm-model runners):

| GDTM | Wayline W (warm) | W-plan-only (warm) | Ray | Ray + app copies |
|---|---|---|---|---|
| no fault | 17.1 s | 17.0 s | 17.5 s | 22.8 s |
| sensor node crashed | 26.8 s | Failed (source-n2 stuck) | Failed | 31.0 s |

- Level with Ray without faults. Wayline's 17 plan copies (27 MB) cost
  nothing measurable; Ray's emulation of the same copies cost 5 s.
- Under the crash Wayline recovers 4 s sooner than Ray with hand-written
  copies, with no application code.

## FOCAL (focal-sweep, 1 run per cell, mixed mode)

| | W | W-plan-only | W-static |
|---|---|---|---|
| no fault | 8.1 s, 5 copies, 13 MB | 7.4 s | 8.1 s, 6 copies, 13 MB |
| source node (anrg-9) crashed mid-run | 14.4 s | Failed (source-audio stuck) | not run |

- HEFT places all of FOCAL on its two source nodes (anrg-4, anrg-9), so a
  crash of a node without a pinned source does not exist for FOCAL; the
  recovery-time negative control is the sweep's cheap producer instead.
- The first W-plan-only run hung for 300 s before its fault: after the
  previous run's crash and restore of anrg-9 (16 s, under the 40 s node
  grace), the focal runner there kept its old address in its status and
  then took minutes to preload its model. The controller now runs a warm
  task as a pod after 15 s without a reachable runner, and the harness
  probes every runner before a run (recover9). Re-run: fails in 7.3 s, as
  expected. The hung run is kept as `*.stale-runner.json`.

## Sweep: producer cost x object size (focal-sweep, chain on warm runners, 1 run)

The chain's producer p runs r s (on its planned node; 4/3 r elsewhere) and
emits s MB, co-located with its consumer; p's node crashes 3 s after p
finishes. Restart: fault to the interrupted consumer's restart.

| r, s | W copied | W completion | W-plan-only | W restart | plan-only restart |
|---|---|---|---|---|---|
| 2 s, 8 MB | yes (8 MB) | 33.3 s | 34.9 s | 5.9 s | 6.8 s |
| 2 s, 100 MB | no | 35.3 s | 35.3 s | 7.2 s | 6.9 s |
| 12 s, 8 MB | yes | 43.3 s | 57.0 s | 5.8 s | 17.6 s |
| 12 s, 100 MB | yes, not landed | 56.3 s | 56.8 s | 17.4 s | 18.4 s |
| 30 s, 8 MB | yes | 62.1 s | 94.4 s | 6.3 s | 37.4 s |
| 30 s, 100 MB | yes, not landed | 92.4 s | 95.2 s | 35.4 s | 37.6 s |

- The gain grows with the producer's cost (1.6 s, 13.7 s, 32.3 s for 8 MB):
  a copy replaces a re-run whose cost is the producer's.
- A cheap producer of a large object is not copied (2 s, 100 MB), and W then
  matches plan-only: the negative control holds.
- A copy protects only once it has landed. 100 MB over p's B/8 link takes
  about 7 s; the fault came 3 s after p finished, the copy was lost with the
  node, and p ran again. The policy prices a copy's cost, not its window of
  vulnerability; faults sooner after production than the copy's transfer
  time defeat it.
- In the 2 s / 100 MB W run, re-protection after the repair copied p's new
  output (100 MB, 0.85 s over the fast links of its new node): the policy
  re-decides for the new placement.

## Live revision and the landing window (gaps, controller recover10, 1 run per cell)

Two changes before these runs. (1) The policy weighs a copy's landing time:
its avoided delay counts only for the share of the consumer's exposure
(from the object's production to the consumer's finish) left once the copy
lands, `protected` in the decisions. (2) Re-protection after a repair no
longer counts the lost node: the repaired model dropped it from its nodes
but re-added it through the finished producer's assignment, so its copy
looked alive and it was offered as a fallback (`repairedModel`).

Second loss, chain on warm runners: crash p's node 3 s after p finished,
then the node c moved to, 3 s after c restarted there.

| | W | W-frozen | W-plan-only |
|---|---|---|---|
| completion | 53.9 s | 66.1 s | 79.7 s |
| second loss | c served from the re-protection copy (anrg-4), no re-run | plan-time copy used up by the first loss, p ran again | p ran again (third run) |

The only cell so far where revision decides the result: W-frozen equals W
until the plan changes.

Window, 100 MB over p's B/8 link (about 7 s to land), exposure 20 s:

| | fault 3 s after p | fault 10 s after p |
|---|---|---|
| r = 12 s, W | no copy now (66% protected, not worth it); 57.4 s, matches lineage | |
| r = 30 s, W | copy lost before landing: 92.4 s (sweep, before weighting) | copy landed: 68.1 s |
| r = 30 s, W-plan-only | 95.2 s (sweep) | 98.8 s |

After each repair the policy re-decided for the new placement: the re-run
producer's node has fast links, and its output was copied in under a second.

## Argo + S3 repository (argo1, 1 run per cell)

Argo Workflows v3.5.5 with every artifact in a central S3 repository
(`rclone serve s3` on anrg-2, memory-backed; MinIO's images are no longer
published). Same task bodies and plan placement as Ray; retries on another
node; pods leave a NotReady node after 5 s instead of 300 s. The repository's
traffic is not shaped by E13, which favors this arm.

| | Argo | Argo + out-of-service taint |
|---|---|---|
| iot, no fault | 91.7 s | (same) |
| iot, sensor node crashed | hung until the 900 s limit | 160.6 s |
| chain, co-located producer's node crashed | hung until the 900 s limit | 123.4 s |

- Default Argo does not recover from a crashed node: Kubernetes evicts the
  node's pods but cannot delete them while the kubelet is unreachable, so
  they stay Running or Terminating, and the workflow waits on them until the
  node returns.
- With automation that taints the node `out-of-service` once Kubernetes
  marks it NotReady (about 49 s after the crash, the node-monitor grace), the
  pods are force-deleted and Argo retries elsewhere from the repository.
- Nothing is lost with a worker: every output is already in the repository.
  In the iot run the crash touched no running task (infer-4 had been
  scheduled on anrg-8, not the sensor node) and no retry was needed; that
  infer-4 took 89 s for a 10 s task, unexplained from one run.
- The price is the fault-free path: 91.7 s for iot, against 41 s on
  Wayline (pods) and 23 s on Ray (warm workers), because every artifact goes
  up to the repository and back down and each stage is a new pod.

## Controller issues found by these runs

| Issue | Status |
|---|---|
| status.makespan counts only surviving pods, wrong after recovery | fixed (earliest start kept per run), analysis uses wall time |
| re-protection priced a copy at planned, not live, bandwidth | fixed, verified |
| link re-planning checked every 5 s | now 2 s |
| plan copies all went to the single best fallback (iot: 650 MB onto one node) | fixed: spread across fallbacks within 1 s or 5% |
| HEFT broke ties in random map order: placements varied between runs of one template | fixed: sorted candidates |
| duplicate copy on a reconcile race | fixed (recover7) |
| re-protection overwrote the plan-time decisions in status.realizationPolicy | fixed: recorded under `reapplication` (recover8) |
| a warm task waited without limit for a node's runner that was not reachable | fixed: runs as a pod after 15 s (recover9) |
| re-protection counted the lost node's copy and offered it as a fallback | fixed: `repairedModel` drops lost nodes (recover10) |
| the policy priced a copy's cost, not its window of vulnerability | fixed: benefit weighted by the protected share of the exposure (recover10) |
