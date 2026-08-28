# AI City MCMT evidence: what is reusable under the current contract

Audit date 2026-08-28. Data collected 2026-08-14..18, which predates named
output objects, the realization revision interface, and per-node order
enactment. This records what the existing runs can and cannot support.

## Requirement-by-requirement

| Requirement | Status | Evidence |
|---|---|---|
| End-to-end completion time | **reusable** | `results/curve.csv`, 4 cells x 20 reps |
| Output correctness | **reusable** | `results/correctness.md`, 80/80 equivalent |
| Bytes / physical path | **partial** | totals only; no gateway breakdown |
| Placement held constant | **partial** | true for the static arm, not the headline arm |
| Application adaptation stated | **needs writing** | code exists, never documented |
| Named-object realization trace | **absent** | predates named objects entirely |

## 1. Completion time (reusable)

`results/curve.csv` is the authoritative paired summary; `paired-ci.csv`
adds bootstrap CIs over `Argo_i - Wayline_i` (10,000 resamples).

| cell | n | Wayline (s) | Argo (s) | delta | speedup | 95% CI | wins |
|---|---|---|---|---|---|---|---|
| n4-d30-jpg | 20 | 109.7 | 141.1 | 31.4 | 22.2% | [17.7, 26.0]% | 19/20 |
| n4-d60-jpg | 20 | 172.8 | 177.7 | 4.8 | 2.7% | [-0.6, 6.2]% | 12/20 |
| n4-d120-jpg | 20 | 169.2 | 178.5 | 9.3 | 5.2% | [2.1, 9.6]% | 13/18 |
| n4-d120-png | 20 | 170.9 | 210.8 | 39.9 | 18.9% | [16.8, 21.3]% | 20/20 |

Two cells are decisive (d30-jpg, d120-png); d60-jpg spans zero and must
not be reported as a win. This spread is itself the useful result: the
advantage tracks how much intermediate data the encoding produces
(336 MB/rep at d30-jpg rising to 866 MB/rep at d120-png), not the
workflow structure, which is identical across cells.

## 2. Correctness (reusable)

80/80 reps across four cells produce reports equivalent to Argo's under a
stated rule: identical `n_global_tracks`, `counts_by_class`, and per-track
`class`/`cameras`/`hop_count`. 30-32 global tracks per cell over
bus/car/motorcycle/truck. This is the strongest part of the existing
dataset and needs no rerun.

## 3. Bytes and physical path (partial, gap)

- `curve.csv` has `wl_bytes_per_rep_mean` (336.6 / 413.3 / 488.7 /
  866.0 MB per rep across cells).
- `results/ablation-static-n4-d120-png/summary.csv` has real totals:
  76.18 GB in, 18.31 GB out per rep.
- **Gap:** the four headline `distributed-minio-*` and `tuned-minio-*`
  cells record `bytes_in_total,bytes_out_total` as `NA` for every rep.
- **Gap:** no per-directed-pair breakdown and no gateway-specific
  accounting, so "Wayline avoids the store, Argo does not" is currently a
  structural claim about the templates rather than a measured one.

## 4. Placement constancy (partial, matters)

- `wayline/render-static.py` pins every task to its modal node, so the
  **static** arm holds placement fixed by construction.
- Argo reps carry `placement.json`, so their placement is recoverable.
- **But** the headline `curve.csv` Wayline arm is scheduler-placed, so
  the 2.7-22.2% numbers above compare *different* placements as well as
  different data planes. That is a legitimate system-level comparison but
  it is NOT the "identical placement, realization only" comparison, and
  it should not be described as one.

## 5. Unresolved discrepancy (blocking for any citation)

`results/ablation-fair-d120png.csv` and `results/curve.csv` disagree on
the same cell (n4-d120-png):

| source | Argo | Wayline |
|---|---|---|
| `curve.csv` | 210.8 | 170.9 |
| `ablation-fair-d120png.csv` | 214 (rep1) | 109 static / 124 heft (rep1) |

Argo agrees across both (210.8 vs 214). Wayline differs by ~40%. The two
were run under different conditions (a `tc`-shaped variant exists as
`nfs-mcmt-d120png-tc.csv` at 218 s, and `notc-sweep.log` implies an
unshaped sweep), but the conditions are not recorded with the data.
**Resolve which condition each dataset represents before either number
appears anywhere.** Until then neither is safe to cite.

## 6. Named-object trace (absent, requires rerun)

`wayline/render.py` and `render-static.py` emit no `outputs:` or
`inputs:` stanzas; they use the pre-named-object single-output model.
There is therefore no run in this dataset in which a named object follows
a policy-selected realization, which is the one thing the applications
section is supposed to demonstrate about the current contract.

## Recommendation

Keep and reuse: completion time (2 decisive cells), correctness (80/80),
and the byte-volume-vs-advantage trend.

Rerun, small and targeted rather than the full sweep: one cell
(n4-d120-png, the decisive one) under the current contract with named
outputs, recording per-directed-pair bytes, gateway in/out, schedule hash
and per-node order, digests, and provenance. Add a frozen
direct-vs-store arm at identical placement and order so the applications
section carries the same controlled realization comparison as E5. That is
roughly 3 arms x 20 blocks rather than the original 4 cells x 3 configs.

## 7. Cell labels overstate clip duration (found 2026-08-28)

After re-fetching the dataset, ffprobe on the sliced clips shows the
`d120` cell does not contain 120-second clips. AI City S04 cameras
c016-c019 are shorter than the requested duration, so `ffmpeg -t 120`
produced the whole source:

| camera | clip_30s | clip_120s | source length |
|---|---|---|---|
| cam-1 | 30.0 s | 31.0 s | 31.0 s |
| cam-2 | 28.1 s | 28.1 s | 28.1 s |
| cam-3 | 30.0 s | 41.8 s | 41.8 s |
| cam-4 | 30.0 s | 46.0 s | 46.0 s |

Consequences:

- **Do not describe the workload as "120-second clips."** The honest
  description is that each clip is the full source camera video, 28.1 to
  46.0 s, and that the requested duration was capped by source length.
- The `d30` and `d120` cells are only partly distinct: cam-2 is
  byte-identical between them and cam-1 differs by one second. The
  measured difference between those cells comes from cameras 3 and 4.
- The historical byte volumes are consistent with these lengths
  (866 MB/rep for d120-png at ~36 s mean across 4 cameras, 5 fps, PNG at
  640x640), so the old measurements are internally fine. Only the label
  is wrong, and it was wrong in the original campaign too, since the
  fetch is deterministic from the same source.
- Renaming is preferable to re-scoping: the cell is the decisive one and
  the data is real. Call it by what it is (full-length source clips)
  rather than by a duration the source cannot supply.
