# E14: FOCAL on MOD, the first ported IoBT pipeline

The first IoBT pipeline from `slides/iobt-sensor-ml-pipelines.pdf` ported to
Wayline: FOCAL (UIUC) on MOD, in `examples/focal-mod/`. Its real shape is
all functions (one current PyTorch stack); the "mixed" variant below runs the
encoders as pods only to exercise the mixed path. E15 (GDTM) is the pipeline
whose real shape is mixed.

Seismic source pinned to anrg-4, audio source to anrg-9; built-in HEFT,
batch of 64 two-second windows, uncapped clocks, clean network.

`focalrun.py <mode> <batch> <seed>` runs one variant, prints the stage
timeline, and checks the pipeline's logits against the unsplit model run in
the same image. `timeline.py` prints saved timelines.

## Results (2026-09-29, two runs per variant)

| variant | makespan | logits match unsplit model |
|---|---|---|
| all pods | 15.6, 16.2 s | yes |
| mixed (functions + encoder pods) | 4.71, 4.72 s | yes |
| all warm functions | 1.79, 1.77 s | yes |

Mixed timeline: functions start within 0.1 s of their inputs; each encoder
pod starts about 3.4 s after its input (pod start, importing torch, building
the model) and computes for 0.5-0.8 s. All-pods pays that start on every
stage, including millisecond FFTs. Compute is small here (about 1.3 s on the
critical path), so dispatch overhead decides the makespan.

Found on the way: the controller's new shared CPU admission (capacity.go)
left a fast warm call's reservation counted until its 60 s TTL, which held
later tasks back 13 to 27 s at random. Fixed before the results above
(`results/` keeps the affected runs too; they are the ones with makespans of
8.9 to 49 s).
