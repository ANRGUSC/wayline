#!/usr/bin/env python3
"""
Generate the heterogeneity-benchmark network matrix for the 20-node set.

A tiered, deliberately heterogeneous topology:
  intel <-> intel : 1000 Mbps   (fast compute fabric)
  pi    <-> pi    :  300 Mbps   (edge switch)
  intel <-> pi    :  100 Mbps   (edge uplink — where sensor->heavy flows ride)
  BOTTLENECK pis  :   50 Mbps   to/from the intel tier (slow far-edge sensors)

The bottleneck Pis are the slowest-clock ones, so they are doubly bad
(slow compute AND slow uplink) — a clean target for HEFT to learn to avoid.
An optional --seed applies +/-20% log-normal jitter to every link for
robustness-across-topologies runs (seed 0 = the clean tiered matrix).

Outputs (pick with the mode arg):
  rules       SRC DST RATE_mbit DELAY_ms JITTER_ms  (one ordered pair per line)
  configmap   the wl-network-profile ConfigMap YAML (bytes/sec) for HEFT
"""
import sys
import math
import random

INTEL = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
PI = ["rpi11", "rpi14", "rpi15", "rpi17", "rpi26", "rpi27",
      "rpi28", "rpi39", "rpi44", "rpi47", "rpi50", "rpi52"]
NODES = INTEL + PI
# Slowest-clock Pis (see clocks.sh) -> also bottlenecked uplinks.
BOTTLENECK_PI = {"rpi14", "rpi44", "rpi27"}

IP = {}
for n in INTEL:
    pass
IP = {
    "anrg-1": "192.168.1.189", "anrg-3": "192.168.1.164", "anrg-4": "192.168.1.156",
    "anrg-5": "192.168.1.154", "anrg-6": "192.168.1.208", "anrg-7": "192.168.1.193",
    "anrg-8": "192.168.1.168", "anrg-9": "192.168.1.166",
}
for n in PI:
    IP[n] = f"192.168.1.{100 + int(n[3:])}"


def tier(n):
    return "intel" if n in INTEL else "pi"


def base_link(a, b):
    """(rate_mbit, delay_ms) for the clean tiered matrix."""
    ta, tb = tier(a), tier(b)
    if ta == "intel" and tb == "intel":
        return 1000, 1
    if ta == "pi" and tb == "pi":
        return 300, 2
    # cross-tier
    if a in BOTTLENECK_PI or b in BOTTLENECK_PI:
        return 50, 25
    return 100, 10


def link(a, b, rng):
    rate, delay = base_link(a, b)
    if rng is not None:
        # symmetric jitter keyed on the unordered pair
        r = random.Random(hash((min(a, b), max(a, b))) ^ rng)
        f = math.exp(r.uniform(math.log(0.8), math.log(1.2)))
        rate = max(20, round(rate * f))
        delay = max(1, round(delay * f))
    jitter = max(1, round(delay * 0.1))
    return rate, delay, jitter


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "rules"
    seed = None
    if len(sys.argv) > 2 and sys.argv[2] != "0":
        seed = int(sys.argv[2])

    if mode == "rules":
        out = []
        for a in NODES:
            for b in NODES:
                if a == b:
                    continue
                rate, delay, jitter = link(a, b, seed)
                out.append(f"{a} {b} {rate} {delay} {jitter}")
        sys.stdout.write("\n".join(out) + "\n")

    elif mode == "configmap-calibrated":
        # Bandwidth = MEASURED effective data-agent throughput at this workload's
        # payload sizes (from results/ccr1 warm flows), not the tc link rate.
        # intra-Intel 272 Mbps; cross-tier (Pi<->Intel) 46 Mbps; pi-pi ~150 Mbps.
        EFF = {("intel", "intel"): 34_000_000, ("pi", "pi"): 18_750_000,
               ("intel", "pi"): 5_750_000, ("pi", "intel"): 5_750_000}
        lines = ["apiVersion: v1", "kind: ConfigMap", "metadata:",
                 "  name: wl-network-profile", "  namespace: wl-system", "data:",
                 '  defaultBandwidth: "34000000"']
        for a in NODES:
            for b in NODES:
                if a == b:
                    continue
                lines.append(f'  {a}_to_{b}: "{int(EFF[(tier(a), tier(b))])}"')
        sys.stdout.write("\n".join(lines) + "\n")

    elif mode == "configmap":
        lines = []
        lines.append("apiVersion: v1")
        lines.append("kind: ConfigMap")
        lines.append("metadata:")
        lines.append("  name: wl-network-profile")
        lines.append("  namespace: wl-system")
        lines.append("data:")
        lines.append('  defaultBandwidth: "125000000"')  # 1 Gbps fallback
        for a in NODES:
            for b in NODES:
                if a == b:
                    continue
                rate, _, _ = link(a, b, seed)
                bps = int(rate * 1_000_000 / 8)  # Mbit/s -> bytes/s
                lines.append(f'  {a}_to_{b}: "{bps}"')
        sys.stdout.write("\n".join(lines) + "\n")

    elif mode == "ipmap":
        # emit "name ip" for the applier
        for n in NODES:
            sys.stdout.write(f"{n} {IP[n]}\n")
    else:
        sys.stderr.write("usage: gen_net.py {rules|configmap|ipmap} [seed]\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
