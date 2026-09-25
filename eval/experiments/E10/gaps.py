"""Per-task launch gap: from the last parent's close to this task's SDK start."""
import json, sys
PARENTS = {"source": [], "a": ["source"], "b": ["source"], "c": ["source"],
           "j1": ["a", "b", "c"], "j2": ["b", "c"], "sink": ["j1", "j2"]}
r = [x for x in json.load(open(sys.argv[1])) if x["arm"] == "wayline"][-1]
T, t0 = r["timings"], r["submitted"]
print(f"{'task':6} {'node':7} {'ready':>6} {'start':>6} {'gap':>5} {'compute':>7} {'close':>6}")
for t in sorted(T, key=lambda k: T[k]["taskStartUnix"]):
    ready = max([T[p]["closeUnix"] for p in PARENTS[t]], default=t0)
    x = T[t]
    print(f"{t:6} {r['placement'][t]:7} {ready-t0:6.2f} {x['taskStartUnix']-t0:6.2f} "
          f"{x['taskStartUnix']-ready:5.2f} {x['computeSeconds']:7.2f} {x['closeUnix']-t0:6.2f}")
