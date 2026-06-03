# Real-workload correctness validation

Per-cell Wayline↔Argo report equivalence across all reps.
Equivalence rule: identical `n_global_tracks`, `counts_by_class`, and per-track `class`/`cameras`/`hop_count`.

| cell | reps | OK | FAIL | n_tracks (mean) | classes |
|------|------|----|------|-----------------|---------|
| n4-d120-jpg | 20 | 20 | 0 | 32.0 | bus,car,motorcycle,truck |
| n4-d120-png | 20 | 20 | 0 | 30.0 | car,motorcycle,truck |
| n4-d30-jpg  | 20 | 20 | 0 | 30.0 | bus,car,motorcycle,truck |
| n4-d60-jpg  | 20 | 20 | 0 | 32.0 | bus,car,motorcycle,truck |
