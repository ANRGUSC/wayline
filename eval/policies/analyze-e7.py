#!/usr/bin/env python3
"""E7 numbers, regenerated from results-e7/.

The ODAG JSON dumps are pruned by the template's retention.maxRuns=10,
so per-run completion times come from the repeat logs; the surviving
JSONs supply per-task states (CacheHit) and cachedFrom chains.
"""
import json, re, statistics as st, pathlib

R = pathlib.Path(__file__).parent / "results-e7"

def log_makespans(arm):
    out = []
    for line in (R / f"{arm}.log").read_text().splitlines():
        m = re.match(r"-> \S+\s+phase=(\S+)\s+makespan=(\d+)s", line)
        if m:
            assert m.group(1) == "Succeeded", line
            out.append(int(m.group(2)))
    return out

nc = log_makespans("e7-nocache")
ca = log_makespans("e7-cache")
print(f"nocache n={len(nc)} median={st.median(nc)}s range=[{min(nc)},{max(nc)}]")
print(f"cache   cold(run1)={ca[0]}s")
warm = ca[1:]
print(f"        warm n={len(warm)} median={st.median(warm)}s range=[{min(warm)},{max(warm)}]")
print(f"        ratio nocache-median/warm-median = {st.median(nc)/st.median(warm):.2f}x")

d = json.loads((R / "e7-cache.json").read_text())
runs = sorted((i for i in d["items"] if i["status"].get("phase") == "Succeeded"),
              key=lambda i: i["metadata"]["creationTimestamp"])
hits = chain = 0
for r in runs:
    ts = {t["name"]: t for t in r["status"]["tasks"]}
    if ts["prep"].get("state") == "CacheHit":
        hits += 1
        if ts["prep"].get("cachedFrom", "").startswith("e7-cache-run-"):
            chain += 1
print(f"surviving JSON runs={len(runs)} CacheHit={hits} chained-to-prior-cache-run={chain}")
