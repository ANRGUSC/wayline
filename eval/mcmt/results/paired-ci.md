# Paired bootstrap CIs (AI City MCMT, 20 reps/cell)

95% percentile CIs from 10,000 bootstrap resamples of `Argo_i - Wayline_i`. Wins is the count of paired reps where Wayline strictly beat Argo.

| cell | n pairs | mean Δ (s) | 95% CI (s) | speedup | 95% CI (%) | wins |
|------|---------|------------|------------|---------|-----------|------|
| n4-d120-jpg | 18 | +10.2 | [+3.7, +17.1] | +5.7% | [+2.1%, +9.6%] | 13/18 |
| n4-d120-png | 20 | +39.9 | [+35.4, +44.9] | +18.9% | [+16.8%, +21.3%] | 20/20 |
| n4-d30-jpg | 20 | +31.4 | [+25.0, +36.7] | +22.2% | [+17.7%, +26.0%] | 19/20 |
| n4-d60-jpg | 20 | +4.8 | [-1.0, +11.1] | +2.7% | [-0.6%, +6.2%] | 12/20 |

