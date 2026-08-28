# E6: Application workloads

Two parts, one campaign. Part A is WOW-derived scientific workflows;
Part B is the targeted AI City MCMT rerun defined by
`eval/mcmt/AUDIT.md`.

Goal: show the object contract working in complete applications, not in
synthetic mechanism DAGs. Every claim rests on measurements this plan
collects, and the application adaptation is stated honestly rather than
presented as an unchanged program.

## Part A workloads (from the WOW paper)

Source: Lehmann et al., CCGrid 2025, open-access PDF at
`lauritzthamsen.org/assets/texts/LehmannBaderTschirpkeDeMecquenem
LoesserBeckerLewinskaThamsenLeser_2025_WOWWorkflowAwareDataMovementAnd
TaskSchedulingForDynamicScientificWorkflows.pdf`

WOW evaluates three families:

- **Real workflows:** RNA-Seq, Sarek, Chip-Seq, Rangeland
- **WfCommons-derived:** BLAST, BWA, Cycles, 1000Genome, Montage,
  Seismology, SoyKB
- **Synthetic patterns:** All-in-One, Chain, Fork, Group, Group-Multiple

**Scale is the constraint.** Their real workflows generate 274-919 GB and
their WfCommons cases roughly 150-169 GB, against 57 GB total disk per
node on our testbed. Reproducing them at original scale is not possible
here and we will not pretend otherwise.

**Selection: scaled-down Montage and SoyKB.** Two WfCommons-derived
recipes from different scientific domains (astronomy image mosaicking
and plant genomics) with different structures, so the claim is about
workflow shape rather than one pipeline. AI City remains the genuine
full-scale end-to-end application.

**Mandatory honesty condition.** Every mention must state that task and
data scale were reduced while the recipe topology and inter-stage data
ratios were preserved. Record the reduction factor and both the original
and scaled per-stage sizes in the results provenance, so a reader can see
exactly what was shrunk. We are not reproducing WOW's results and must
not imply a head-to-head comparison.

## Framing (do not overstate)

WOW is the closest prior architecture: it makes copies explicit, records
replicas and in-flight copy operations, and creates copies to prepare
candidate nodes for tasks. We are **not** reimplementing WOW and must not
claim a head-to-head comparison against it. What we claim is narrower and
defensible: the same workflow structures, run on our substrate, where the
realization is a policy decision separable from the task map. The
comparison is Wayline-internal (policy-selected direct vs fixed or
store-mediated), plus the external Argo+MinIO referent we already use.

## Part A arms (WOW-derived), per workflow

| arm | placement | realization | purpose |
|---|---|---|---|
| `direct-policy` | scheduler-chosen | policy-selected direct | the contract in use |
| `direct-frozen` | replayed from `direct-policy` | direct | control for arm 3 |
| `store-frozen` | same frozen schedule + per-node order | all objects via gateway | isolates realization |
| `argo-minio` | Argo default | MinIO artifacts | external referent |

Arms 2 and 3 differ only in the data path, as in E5. Arm 1 vs 2 exposes
any cost of scheduling itself. Arm 4 is the outside comparison and is not
placement-matched, which must be said explicitly.

## Part B: AI City MCMT pilot (specified, runnable)

Cell `n4-d120-png` only, the decisive cell (20/20 wins, +18.9%,
866 MB/rep). Pilot is **3 blocks x 3 arms = 9 runs**; scale to 20 blocks
(60 runs) only if the pilot passes unchanged.

| arm | notes |
|---|---|
| `wl-direct-frozen` | named outputs, frozen placement + per-node order |
| `wl-store-frozen` | same frozen schedule, every named object via `anrg-9` |
| `argo-minio` | external referent, NOT placement-matched |

### Network matrix (derived from E0's measured B=942 Mbit/s)

| link class | rate |
|---|---|
| within edge tier (anrg-1,3,4,5) or within compute tier (anrg-6,7,8) | 942 Mbit/s |
| edge <-> compute | 118 Mbit/s (B/8) |
| any link incident to anrg-9 | 59 Mbit/s (B/16) |

anrg-9 hosts `cross-camera-match` and `report`, so the fan-in always
crosses the slowest class; the store arm additionally routes every
intermediate through it.

### Frozen schedule

Generate ONE deterministic HEFT schedule and per-node order under the
matrix above, freeze it, and replay it unchanged for both Wayline arms.
Freeze from the deployed controller's own scheduling call, not an
offline reconstruction, and with `PYTHONHASHSEED` pinned: HEFT
tie-breaks through a hash-randomized set and moves under 9 of 10 seeds
(see `saga-scheduling-determinism`). Argo is not placement-matched and
this must be stated wherever its numbers appear.

### Pilot pass criteria (all must hold)

1. 9/9 workflows complete within a 900 s deadline
2. all three arms produce equivalent reports
3. both Wayline arms match the frozen placement and order exactly
4. every intermediate is a named object with valid digest records
5. directed-pair and gateway bytes present for every run
6. store lowering sends every intermediate through `anrg-9`
7. direct execution follows the frozen producer-consumer paths
8. zero restarts, fallbacks, or constraint overrides
9. shaping verified live before each run and cleared after

### Relationship to the old dataset

The 60-run campaign **replaces** the disputed `d120-png` timing rather
than being combined with it (see `eval/mcmt/AUDIT.md`, section 5). The
old correctness result (80/80) and the cross-cell byte-volume trend
remain reusable and are not rerun.

### Required port before any run

The MCMT renderers and task scripts predate named objects:
`wayline/tasks/*.py` all call one-argument `send_raw(blob)`, and
`render.py` emits no `outputs:`/`inputs:`. Porting means naming one
object per producer (decode->`frames`, preprocess->`prepped`,
detect_embed->`dets`, track->`tracks`, cross_camera_match->`matches`),
switching five send sites to the two-argument form, resolving inputs by
`producer.object` key with a `WL_INPUT_PEERS` override for the store
arm, and rebuilding the six images. This is the application adaptation
and is reported as such, not hidden.

## Recorded for every run (both parts)

- end-to-end completion time; per-task start/close
- **bytes per directed node pair**, and gateway in/out separately
- output correctness against the stated equivalence rule
- schedule hash and observed per-node order vs the scheduler's order
- digests on every object; task restart count
- constraint overrides, scheduler fallbacks
- `PROVENANCE.txt`: controller/agent imageIDs, frozen refs, seeds,
  campaign and policy seeds, network treatment verified live
- at least one full trace per workflow showing each named object's
  copies, serving node, and install times

## Application adaptation, to be stated in the paper

Record exactly what changed per workflow, as a table: SDK `send`/`recv`
call sites added, wrapper scripts, and any container modification. The
claim is "adapted once to name and exchange outputs through the SDK",
never "unchanged application containers". If a wrapper reads and rewrites
files around an unmodified binary, say so and count it as adaptation.

## Validity checks (blocking, per run)

Carried over from E5, plus application-specific:

- direct arms: every transfer occurs between nodes the schedule used
- store arms: every named object crosses the gateway exactly once
- frozen arms: schedule hash and per-node order match the frozen ref
- correctness: report equivalent to the reference under the stated rule
- disk headroom above 15% at block start; network treatment verified
  before each run and cleared after
- `PYTHONHASHSEED` pinned for frozen-replay arms and restored on teardown
  (see `saga-scheduling-determinism` findings)

## Sequencing

1. Fill the WOW workload row above from the paper.
2. Part B first: it is fully specified, reuses a working harness, and
   closes the named-object gap the audit identified.
3. Part A once the workload row is filled.
4. Then the compact correctness/overhead/scale study.
