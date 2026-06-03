# E1 Results Summary

Warm window: runs 5..20, N=16 per cell.

## Per-cell stats

| Benchmark | System | n | mean (s) | std (s) | p95 (s) |
|---|---|---:|---:|---:|---:|
| iobt | Wayline | 16 | 42.12 | 0.86 | 43.00 |
| iobt | Argo Workflows | 16 | 152.38 | 4.46 | 159.00 |
| hetero-compute | Wayline | 16 | 64.06 | 3.47 | 67.00 |
| hetero-compute | Argo Workflows | 16 | 110.19 | 1.24 | 112.00 |
| wide-pipeline-flex | Wayline | 16 | 41.19 | 0.63 | 42.00 |
| wide-pipeline-flex | Argo Workflows | 16 | 173.12 | 2.32 | 175.00 |

## Wayline vs Argo ratio (Argo/Wayline, warm mean)

| Benchmark | Wayline (s) | Argo (s) | Ratio | Std ratio |
|---|---:|---:|---:|---:|
| iobt | 42.12 | 152.38 | **3.62×** | 5.20× |
| hetero-compute | 64.06 | 110.19 | **1.72×** | 0.36× |
| wide-pipeline-flex | 41.19 | 173.12 | **4.20×** | 3.65× |
