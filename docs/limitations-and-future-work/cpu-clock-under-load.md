# CPU clock under load

## Problem

A node's speed depends on how many of its cores are busy, and a single
benchmark process does not see that.

The workers are Intel i3-N305 (8 cores, no SMT, base 1.8 GHz, turbo 3.8
GHz). With only a frequency ceiling set, the per-process SHA-256 rate on
anrg-1 falls as more copies run at once:

| copies at once | 1 | 2 | 3 | 4 | 8 |
|---|---|---|---|---|---|
| Mhash/s per copy | 2.72 | 2.62 | 2.45 | 1.86 | 1.43 |
| relative | 1.00 | 0.96 | 0.90 | 0.68 | 0.53 |

The reported clock (`scaling_cur_freq`) only fell from 3.45 to 3.2 GHz
between one and four busy cores, so most of the slowdown is not visible
as a frequency change. The likely cause is the package power limit; it is
not yet confirmed (RAPL counters were not read).

## What it affected

E11 and E12 capped "fast" nodes with `scaling_max_freq = 3.8 GHz`, which is
the turbo maximum, so it capped nothing. Calibration ran one benchmark per
node, so fast nodes were planned at their single-core rate, while the
experiments ran up to four tasks per fast node. Measured over all E12 runs
(Wayline and Ray alike), compute on fast nodes against plan:

| tasks at once on a fast node | 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| actual / planned | 1.03 | 1.17 | 1.32 | 1.40 |

This was about 5.5 s of the 8.7 s gap between the estimate and a warm run
of SAGA HEFT (E12, 2026-09-27). Medium (1.9 GHz) and slow (0.9 GHz) nodes
were unaffected: their ceilings are below every loaded clock.

## What fixes it

A lock (floor = ceiling) at or below the clock the chip holds under the
planned load. On anrg-1:

| lock | 1 copy | 4 copies | 8 copies |
|---|---|---|---|
| 3.0 GHz | 2.21 | 2.19 | 1.40 |
| 2.0 GHz | 1.47 | 1.37 | 1.42 |

3.0 GHz is flat up to four copies (the E11/E12 slot count); with all eight
cores busy only about 2.0 GHz holds.

Done: `eval/experiments/E11/e11.py` now locks floor and ceiling together
(fast 3.0, medium 1.5, slow 0.8 GHz) and restores 0.8 to 3.8 GHz afterward.
Experiments before 2026-09-27 used ceilings only.

## Future work

- **Calibrate under the planned load.** Benchmark each node with as many
  concurrent copies as it has slots, so the cost model matches execution
  even without a lock. On a production cluster clocks cannot be locked.
- **Model speed as a function of co-located load.** Slot-aware scheduling
  assumes a slot's speed is independent of its neighbours; on these nodes
  it is not. A scheduler could use rate(node, busy slots) instead of a
  single speed.
- **Profiler.** Wayline's profiler keeps one smoothed runtime per (task,
  node). It would average over whatever co-location occurred, which moves
  as placements change, so it cannot represent this effect on its own.
- **Confirm the mechanism** with RAPL power counters and per-core
  APERF/MPERF, which report effective clock under throttling.
