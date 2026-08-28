# E6: Application workloads

Two parts, one campaign. Part A is WOW-derived scientific workflows;
Part B is the targeted AI City MCMT rerun defined by
`eval/mcmt/AUDIT.md`.

Goal: show the object contract working in complete applications, not in
synthetic mechanism DAGs. Every claim rests on measurements this plan
collects, and the application adaptation is stated honestly rather than
presented as an unchanged program.

## Open item that gates Part A

The workflow structures must be taken from the WOW paper
(Lehmann et al., CCGrid 2025), not invented. **I have not read the paper
in this session and will not guess which workflows it uses.** Before
building anything, extract and record here:

- which workflows WOW evaluates, and their source (nf-core? which
  versions?)
- task counts, dependency structure, and intermediate file sizes
- the cluster shape WOW assumes, so ours is comparable or its
  differences are stated

Until that is filled in, Part A is defined structurally below but its
workload row is a placeholder. Everything in Part B is ready to run now.

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

## Part B arms (AI City MCMT, targeted rerun)

Cell `n4-d120-png` only, the decisive cell (20/20 wins, +18.9%,
866 MB/rep).

| arm | notes |
|---|---|
| `wl-direct-frozen` | named outputs, frozen placement + per-node order |
| `wl-store-frozen` | same schedule, all named objects via `anrg-9` |
| `argo-minio` | rerun for provenance under the same cluster state |

Reuses: correctness rule and report equivalence (already 80/80), the
existing dataset for the byte-volume trend across cells. Does not rerun
d30/d60/d120-jpg.

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
