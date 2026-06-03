#!/usr/bin/env python3
"""
Generate the heterogeneity-benchmark ODAGTemplate.

A 27-task, 5-class DAG built to exercise CPU-clock heterogeneity + network
heterogeneity on the mixed Intel/Pi cluster, and to compare classical vs.
resource-aware HEFT.

Classes (compute:data profile):
  sensor    - Pi tier, light CPU, modest output   (edge data origin)
  heavy     - Intel tier, heavy CPU, medium output (compute hotspots)
  shuffle   - Intel tier, medium CPU, medium output (diamonds / cross-links)
  aggregate - Intel tier, light CPU, small output  (fan-in)
  report    - Intel tier, trivial, no output       (final sink)

The two tiers are FIXED candidate pools for both scheduling modes, so the only
difference between `--mode ra` (resource-aware) and `--mode classical` is the
exclusivePerNode flag plus per-tier CPU sizing. This keeps the comparison
unconfounded. With heterogeneous core counts (Intel=8, Pi=4) a single CPU
request cannot be exclusive on both tiers, so exclusivity is enforced per tier:
  classical: Pi tasks request 3500m (1/node), Intel tasks request 7500m (1/node)
  ra:        fractional requests so independent tasks co-run on a node

The synthetic task does iterations = runtime_hint * 1e6 SHA-256 hashes, so a
node's actual runtime scales inversely with its pinned clock. The profiler then
learns per-node runtimes; cross-tier edges (Pi sensor -> Intel heavy) make the
shaped network matter.

Usage:
  gen_dag.py --mode ra        --name het-ra        [--seed 7] -o out.yml
  gen_dag.py --mode classical --name het-classical [--seed 7] -o out.yml
"""
import argparse
import random
import sys

IMAGE = "192.168.1.163:5000/wl-hetero-compute-task:multi"

INTEL = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
PI = ["rpi11", "rpi14", "rpi15", "rpi17", "rpi26", "rpi27",
      "rpi28", "rpi39", "rpi44", "rpi47", "rpi50", "rpi52"]

# Per-class spec: count, tier, runtime hint (s on a fast node), output bytes.
# Output sizes are kept small enough for Pi RAM (payload is built in-memory).
MB = 1024 * 1024
CLASSES = {
    "sensor":    dict(count=8, tier="pi",    runtime=1, data=12 * MB),
    "heavy":     dict(count=8, tier="intel", runtime=6, data=10 * MB),
    "shuffle":   dict(count=6, tier="intel", runtime=3, data=15 * MB),
    "aggregate": dict(count=4, tier="intel", runtime=2, data=3 * MB),
    "report":    dict(count=1, tier="intel", runtime=1, data=0),
}

# Resource requests per mode. millicores / bytes-as-Mi string.
RA_RES = {
    "sensor":    ("800m", "128Mi"),
    "heavy":     ("2000m", "256Mi"),
    "shuffle":   ("1500m", "200Mi"),
    "aggregate": ("1000m", "200Mi"),
    "report":    ("500m", "128Mi"),
}
# Classical: one task per node, sized to fit alongside system DaemonSets with
# margin for per-node variation (some Intel nodes carry ~1100m of system pods,
# leaving 6900m). Intel 6000m fits everywhere & 2x>8000 stays exclusive; Pi
# 2500m fits (even with ~1500m overhead) & 2x>4000 stays exclusive.
CLASSICAL_CPU = {"pi": "2500m", "intel": "6000m"}
CLASSICAL_MEM = {
    "sensor": "128Mi", "heavy": "256Mi", "shuffle": "200Mi",
    "aggregate": "200Mi", "report": "128Mi",
}


def pool(tier):
    return PI if tier == "pi" else INTEL


def build_tasks(seed):
    rng = random.Random(seed)
    tasks = []

    # Layer instances.
    sensors = [f"sensor-{i}" for i in range(CLASSES["sensor"]["count"])]
    heavies = [f"heavy-{i}" for i in range(CLASSES["heavy"]["count"])]
    shuffles = [f"shuffle-{i}" for i in range(CLASSES["shuffle"]["count"])]
    aggs = [f"aggregate-{i}" for i in range(CLASSES["aggregate"]["count"])]

    def mk(name, cls, deps):
        c = CLASSES[cls]
        # Per-task output jitter (+/-25%) for data heterogeneity (sensors only
        # vary meaningfully; report stays 0).
        data = c["data"]
        if data > 0:
            jitter = 1.0 + rng.uniform(-0.25, 0.25)
            data = int(data * jitter)
        return dict(name=name, cls=cls, tier=c["tier"], runtime=c["runtime"],
                    data=data, deps=deps)

    def cover_assign(producers, consumers, extra_prob):
        """Assign producers to consumers so every producer is consumed at least
        once (round-robin over a shuffled producer list), then add a second
        distinct producer to a fraction of consumers for diamonds/cross-links.
        Returns {consumer: [producers...]}."""
        prod = producers[:]
        rng.shuffle(prod)
        deps = {c: [] for c in consumers}
        for idx, p in enumerate(prod):
            deps[consumers[idx % len(consumers)]].append(p)
        for c in consumers:
            if rng.random() < extra_prob:
                extra = rng.choice(producers)
                if extra not in deps[c]:
                    deps[c].append(extra)
        return deps

    for s in sensors:
        tasks.append(mk(s, "sensor", []))

    # heavy depends on sensors (full coverage -> guaranteed cross-tier flows).
    hd = cover_assign(sensors, heavies, extra_prob=0.5)
    for h in heavies:
        tasks.append(mk(h, "heavy", hd[h]))

    # shuffle depends on heavies (diamonds / cross-links; full coverage).
    sd = cover_assign(heavies, shuffles, extra_prob=1.0)
    for sh in shuffles:
        tasks.append(mk(sh, "shuffle", sd[sh]))

    # aggregate depends on shuffles (full coverage).
    ad = cover_assign(shuffles, aggs, extra_prob=1.0)
    for a in aggs:
        tasks.append(mk(a, "aggregate", ad[a]))

    # report depends on all aggregates (final fan-in).
    tasks.append(mk("report", "report", list(aggs)))
    return tasks


def to_yaml(tasks, name, mode):
    exclusive = (mode == "classical")
    lines = []
    a = lines.append
    a("apiVersion: wl.io/v1")
    a("kind: ODAGTemplate")
    a("metadata:")
    a(f"  name: {name}")
    a("  namespace: wl-system")
    a("spec:")
    a(f"  description: \"heterogeneity benchmark ({mode}): 27-task 5-class DAG, Intel+Pi tiers\"")
    a("  scheduler: heft")
    a("  schedulerConfig:")
    a("    spreadEpsilon: 0")
    a(f"    exclusivePerNode: {'true' if exclusive else 'false'}")
    a("  profiling:")
    a("    enabled: true")
    a("    warmupRuns: 0")
    a("    minSamples: 2")
    a("    emaAlpha: 0.4")
    a("    maxSamples: 100")
    a("    runtimeSource: profiler")
    a("    bandwidthSource: external")
    a("  defaults:")
    a("    runtime: 3")
    a("    dataSize: \"0\"")
    a("  retention:")
    a("    maxRuns: 80")
    a("    data:")
    a("      policy: keepLatest")
    a("      keepRuns: 2")
    a("      maxSizePerNode: \"2Gi\"")
    a("  tasks:")
    for t in tasks:
        cls = t["cls"]
        if exclusive:
            cpu = CLASSICAL_CPU[t["tier"]]
            mem = CLASSICAL_MEM[cls]
        else:
            cpu, mem = RA_RES[cls]
        nodes = pool(t["tier"])
        a(f"    - name: {t['name']}")
        a(f"      image: {IMAGE}")
        a("      command: [\"python\", \"task.py\"]")
        if t["deps"]:
            a(f"      dependencies: [{', '.join(t['deps'])}]")
        else:
            a("      dependencies: []")
        a(f"      runtime: {t['runtime']}")
        a(f"      dataSize: \"{t['data']}\"")
        a("      resources:")
        a(f"        cpu: \"{cpu}\"")
        a(f"        memory: \"{mem}\"")
        a("      constraints:")
        a(f"        nodeNames: [{', '.join(nodes)}]")
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["ra", "classical"], required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("-o", "--out", default="-")
    args = ap.parse_args()

    tasks = build_tasks(args.seed)
    y = to_yaml(tasks, args.name, args.mode)
    if args.out == "-":
        sys.stdout.write(y)
    else:
        with open(args.out, "w") as f:
            f.write(y)
        # Brief summary to stderr.
        ntier = {"pi": 0, "intel": 0}
        for t in tasks:
            ntier[t["tier"]] += 1
        sys.stderr.write(
            f"wrote {args.out}: {len(tasks)} tasks "
            f"(pi={ntier['pi']}, intel={ntier['intel']}), mode={args.mode}, seed={args.seed}\n")


if __name__ == "__main__":
    main()
