#!/usr/bin/env python3
"""
Generate a seeded random edge-like network: per worker-pair bandwidth
(log-uniform 30 Mbit..1 Gbit) + delay (5-80 ms) + jitter (~10% of delay).
Symmetric (same both directions) for clean interpretation. Loss = 0 by default.

  gen-tc-random.py <seed>            -> prints "SRC DST RATE_mbit DELAY_ms JITTER_ms"
                                        for every ordered worker pair, and saves
                                        the matrix JSON to results/random-nets/seed-<n>.json

Edges to/from the control plane and self are left unshaped (handled by setup script).
"""
import sys, json, math, random, os

WORKERS = ["anrg-1","anrg-3","anrg-4","anrg-5","anrg-6","anrg-7","anrg-8","anrg-9"]
IP = {"anrg-1":"192.168.1.189","anrg-3":"192.168.1.164","anrg-4":"192.168.1.156",
      "anrg-5":"192.168.1.154","anrg-6":"192.168.1.208","anrg-7":"192.168.1.193",
      "anrg-8":"192.168.1.168","anrg-9":"192.168.1.166"}

def main():
    seed = int(sys.argv[1])
    rng = random.Random(seed)
    # symmetric per-pair draws
    pair = {}
    nodes = sorted(WORKERS)
    for i in range(len(nodes)):
        for j in range(i+1, len(nodes)):
            rate = round(math.exp(rng.uniform(math.log(30), math.log(1000))))  # Mbit, log-uniform
            delay = round(rng.uniform(5, 80))                                  # ms
            jitter = max(1, round(delay*0.1))                                  # ms (1-8)
            pair[(nodes[i],nodes[j])] = (rate, delay, jitter)
    # emit ordered pairs (symmetric)
    lines, matrix = [], {}
    for a in WORKERS:
        for b in WORKERS:
            if a == b: continue
            k = (min(a,b), max(a,b))
            rate, delay, jitter = pair[k]
            lines.append(f"{a} {b} {rate} {delay} {jitter}")
            matrix.setdefault(a,{})[b] = {"rate_mbit":rate,"delay_ms":delay,"jitter_ms":jitter,"dst_ip":IP[b]}
    here = os.path.dirname(os.path.abspath(__file__))
    outdir = os.path.join(here, "results", "random-nets"); os.makedirs(outdir, exist_ok=True)
    json.dump({"seed":seed,"workers":WORKERS,"ip":IP,"matrix":matrix},
              open(os.path.join(outdir, f"seed-{seed}.json"),"w"), indent=2)
    sys.stdout.write("\n".join(lines)+"\n")

if __name__ == "__main__":
    main()
