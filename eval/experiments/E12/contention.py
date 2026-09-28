"""CPU actually demanded per node, and compute stretch, for Wayline runs.

For each Wayline arm: from each task's real compute interval (SDK timings)
and CPU request, the peak total CPU demanded on any node at any instant,
against the node's 8 cores and the 7.4 the scheduler plans with; and each
task's compute time over its planned runtime.

Usage: python3 contention.py <results dir>
"""
import json, sys
d = sys.argv[1]
dag = json.load(open(f"{d}/dag.json"))["tasks"]; rates = json.load(open(f"{d}/rates.json"))
ref = max(rates.values())
for r in json.load(open(f"{d}/results.json")):
    if not r["arm"].startswith("wayline") or r["phase"] != "Succeeded":
        continue
    T, pl = r["timings"], r["placement"]
    iv = {t: (x["computeStartUnix"], x["computeEndUnix"]) for t, x in T.items()}
    peak, peak_node = 0, None
    for t, (s, _) in iv.items():
        used = sum(dag[u]["cpu"] for u, (a, b) in iv.items() if pl[u] == pl[t] and a <= s < b)
        if used > peak:
            peak, peak_node = used, pl[t]
    stretch = [(iv[t][1] - iv[t][0]) / (int(dag[t]["work"] * ref * 1e6) / (rates[pl[t]] * 1e6)) for t in iv]
    print(f"{r['arm']:28} makespan {r['makespan']:6.1f}s  peak CPU on a node {peak} ({peak_node}; 8 cores, plan 7.4)  "
          f"compute/plan mean {sum(stretch)/len(stretch):.2f} max {max(stretch):.2f}")
