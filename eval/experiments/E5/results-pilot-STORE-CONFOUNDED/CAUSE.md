# Retained: store arms ran a different placement than the direct arms

All 24 runs passed their own validity checks, and the 18 direct-arm runs
in this dataset are sound: they match the frozen schedule hash 3/3 and
are carried forward unchanged.

The six store-arm runs (iso-heft-store, iso-maxtp-store) are NOT
comparable to the direct arms here. `e5_pilot.py` applied the checked-in
`e5-store-*.yml`, which had been generated at 08:26 from the frozen
schedule that predated the PYTHONHASHSEED fix. Re-generated templates
applied by hand at 10:01 were overwritten by that step at campaign
start.

Effect: direct arms ran `a -> anrg-8, c/j1/sink -> anrg-7` while the
store arms ran `a -> anrg-7, c/j1/sink -> anrg-8`. The two are
isomorphic under the anrg-7 <-> anrg-8 relabeling and those nodes are
symmetric peers (identical runtimeProfile, symmetric bandwidth), so the
1.61x / 1.59x realization ratios are probably unaffected -- but
"probably" is not "measured", and the whole point of the store arm is to
hold placement fixed while changing only realization.

Caught by the analyzer's "placement identical across realizations"
check, not by the per-run validity checks, which had no reason to
compare arms against each other.

Fix: the harness now regenerates the store templates from the frozen
refs in use at launch instead of applying a checked-in yaml.
