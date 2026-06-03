# E0 — Two-Hop Microbenchmark

The framing-gate experiment for the ATC 2026 submission. Measures the
penalty paid by K8s-native workflow systems that conflate task
completion with data availability via shared object storage, isolated
from any controller-specific overhead.

**Pass criterion** — same-node, ≥100 MB payload: Wayline mean end-to-end
≤ 0.5× MinIO mean end-to-end (i.e., ≥ 2× gap). If this fails, the C1
contribution framing must be reconsidered before proceeding to E1.

See [the spec section below](#design) for the full pass/conditional/fail
breakdown.

---

## Claim under test

Architectural claim (C1, Wayline's lead contribution):

> Task completion and data availability are two distinct events
> separated by a network transfer. Existing K8s-native workflow systems
> (Argo Workflows, Tekton, Kubeflow Pipelines, Flyte) conflate them via
> shared storage — producer pods hold compute until upload completes
> and consumer pods can't start until download completes, even when
> they are co-located.

E0 measures the size of that penalty under controlled conditions.

## Design

### Variables — 16 cells

| Axis | Levels |
|---|---|
| Payload | 1 MB, 10 MB, 100 MB, 500 MB |
| Co-location | same-node (anrg-3 ↔ anrg-3), cross-node (anrg-3 ↔ anrg-6, S-link 50 Mbps) |
| System | `wayline`, `minio` |

- Matrix v2 tc shaping is **on** throughout (`../network-aware/setup-tc-matrix.sh`).
- MinIO single-pod on a *neutral* node (anrg-9) so its reachability is
  symmetric and stable across cells.

### What we measure

The producer is `sleep 5s → generate payload`. The consumer is
`receive → exit`. Each pod logs a single JSON line tagged
`WL_E0_TIMESTAMPS` capturing:

| Symbol | Event | Where recorded |
|---|---|---|
| `t0` | producer compute start | producer pod (wall clock) |
| `t1` | producer compute end / handoff start | producer pod |
| `t2` | producer pod terminates | K8s Pod `containerStatuses[].state.terminated.finishedAt` |
| `t3` | consumer enters `recv`/`get` | consumer pod (wall clock) |
| `t4` | consumer has full payload | consumer pod |

Three derived metrics:

| Metric | Definition | What it shows |
|---|---|---|
| **E2E** | `t4 − t0` | Headline: producer-compute-start → consumer-data-ready. |
| **Producer hold-time** | `t2 − t1` | How long the producer holds CPU/memory after compute is done. |
| **Consumer wait-time** | `t3 − t2` | Gap between producer-freed and consumer-can-start. |

### Pass / fail criteria

- **PASS** → continue to E1. Wayline mean E2E ≤ 0.5 × MinIO mean E2E on the
  **same-node, ≥ 100 MB** cells (runs 5–20, N=16).
- **CONDITIONAL PASS** → continue, reframe C1 paragraphs to emphasize
  the same-node case. Same-node ≥ 2× but cross-node < 2×.
- **FAIL** → STOP and rethink C1. Same-node large-payload gap < 2×.

Pre-registered sanity checks (not gates, but reported):
- MinIO same-node E2E ≈ MinIO cross-node E2E (within 20%, plus a
  one-link tax on the cross-node download).
- Wayline cross-node E2E ≈ payload / 50 Mbps + ~5 s compute + 1–2 s overhead.

### What's *not* measured here

- Real Argo Workflows lifecycle overhead → **E1**.
- K8s scheduler-plugins NetworkOverhead → **E2**.
- CDAG streaming → **E3**.
- ε-HEFT under jitter → **E4**.

E0 is intentionally minimal: two synthetic tasks, one direct comparator,
the cleanest possible window into the architectural property.

---

## Layout

```
eval/e0-microbench/
├── README.md                  this file
├── preflight-idle.sh          cluster-idle guard — invoked before every cell
├── deploy-minio.sh            one-time MinIO setup
├── teardown-minio.sh          remove MinIO
├── cells.txt                  16-line cell manifest (system,colocation,payload)
├── wayline/
│   ├── producer/{task.py,Dockerfile}
│   ├── consumer/{task.py,Dockerfile}
│   ├── odag.yml.tpl           parameterised ODAGTemplate
│   └── run.sh                 Wayline sweep driver (idempotent, resumable)
├── minio/
│   ├── deployment.yml         single-pod MinIO + Service
│   ├── producer/{task.py,Dockerfile}
│   ├── consumer/{task.py,Dockerfile}
│   ├── job.yml.tpl            parameterised paired K8s Jobs
│   └── run.sh                 MinIO sweep driver
├── sweep.sh                   orchestrator: preflight → Wayline cells → MinIO cells
├── harvest.py                 unify per-run timestamps into a single CSV
├── plot.py                    figures from the CSV
├── results/                   populated by drivers (per-run JSON + per-cell summary.csv)
├── figures/                   populated by plot.py
└── RESULTS.md                 written after the sweep
```

---

## Run instructions

### Pre-requisites

- 8-node k3s cluster healthy (`kubectl get nodes`).
- Wayline controllers + data-agent DaemonSet running in `wl-system` (paper builds rely on
  the existing matrix-v2 deployment — see `../network-aware/README.md`).
- Bandwidth matrix v2 applied (`../network-aware/setup-tc-matrix.sh`).
- Local image registry at `192.168.1.163:5000` reachable from every node.

### Smoke test (run this first)

```bash
./preflight-idle.sh           # must exit 0
./deploy-minio.sh             # one-time
SMOKE=1 ./sweep.sh            # 1 cell (same-node 10 MB) × 2 reps × 2 systems
python harvest.py results/    # produces smoke.csv
```

Inspect `smoke.csv` and `results/{wayline,minio}/...` — every run must have
all five timestamps populated and the deltas must be sane (E2E within
2× of payload/bandwidth + 5 s compute).

### Full sweep

```bash
./preflight-idle.sh
./sweep.sh                    # 16 cells × 20 reps × 2 systems = 320 runs (~3 h)
python harvest.py results/    # produces all.csv
python plot.py                # writes figures/e0-*.png|pdf
```

Drivers are **resumable**: re-running `./sweep.sh` with an existing
`results/` skips cells with complete summary.csv files. Use this to
recover from partial failures without restarting from zero.

Every cell starts with a `preflight-idle.sh` check. If it fails, the
driver aborts that cell and continues to the next.

### Cleanup

```bash
./teardown-minio.sh
```

(Wayline leaves behind only per-run ODAG resources and their pods, which
the data-retention policy already deletes immediately.)

---

## Anonymization note

This experiment dir uses the project name `Wayline` throughout. The
submitted paper must replace `Wayline` with `ANON-WMS` in all text and
figure captions. Anonymization happens at paper build time, not here.
