#!/usr/bin/env python3
"""
Generate the CCR-targeted classical-HEFT template for the "how close to the
theoretical makespan does profiling get us" experiment.

Same 5-class DAG as gen_dag.py, but:
  - compute is set per class via WL_ITERS (exact SHA-256 iteration count),
    decoupled from the integer runtime hint, so we can dial compute precisely;
  - the sensor (cross-tier) payload is auto-sized so the analytical
    communication-to-computation ratio (CCR) hits the target.

CCR (placement-independent, the standard list-scheduling definition):
    CCR = sum_edges( bytes_e / bw_avg(tier-pair_e) )
        / sum_tasks( iters_t / throughput_avg(tier_t) )
using measured per-tier average throughput and the shaped-matrix average
bandwidth per tier-pair. Communication is dominated by the slow cross-tier
sensor->heavy edges; non-sensor (Intel-Intel) payloads are small and fixed, so
we solve a single scalar (sensor payload) to land CCR at the target.

Only classical HEFT (exclusivePerNode=true); network shaping must be ON for the
CCR to be realized. EMA alpha configurable (default 0.7).

Usage: gen_ccr.py --name het-ccr1 --ccr 1.0 --alpha 0.7 [--seed 7] -o out.yml
"""
import argparse
import random
import sys

IMAGE = "192.168.1.163:5000/wl-hetero-compute-task:multi"
INTEL = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
PI = ["rpi11", "rpi14", "rpi15", "rpi17", "rpi26", "rpi27",
      "rpi28", "rpi39", "rpi44", "rpi47", "rpi50", "rpi52"]
MB = 1024 * 1024

# Measured SHA-256 throughput (hashes/sec) at pinned clocks -> per-tier average.
TPUT = {"intel": 834422.0, "pi": 113094.0}
# Average bandwidth (bytes/sec) per tier-pair under the shaped matrix.
#   intel-intel 1000 Mbps; pi-pi 300 Mbps; cross-tier ~87.5 Mbps avg
#   (9/12 Pis @100 + 3/12 bottleneck @50).
BW = {("intel", "intel"): 125e6, ("pi", "pi"): 37.5e6,
      ("intel", "pi"): 10.94e6, ("pi", "intel"): 10.94e6}

# Per-class compute (WL_ITERS) and tier. Compute kept modest (~1-4 s) so it does
# not swamp communication; the sensor payload is solved for the CCR target.
CLASSES = {
    "sensor":    dict(count=8, tier="pi",    iters=340_000),    # ~3.0 s on Pi avg
    "heavy":     dict(count=8, tier="intel", iters=3_300_000),  # ~4.0 s on Intel avg
    "shuffle":   dict(count=6, tier="intel", iters=2_000_000),  # ~2.4 s
    "aggregate": dict(count=4, tier="intel", iters=1_250_000),  # ~1.5 s
    "report":    dict(count=1, tier="intel", iters=800_000),    # ~1.0 s
}
# Fixed (small) output payloads in bytes for the fast Intel-Intel edges.
FIXED_BYTES = {"heavy": 12 * MB, "shuffle": 15 * MB, "aggregate": 4 * MB, "report": 0}
# Classical exclusive CPU per tier (fits alongside system pods, 2x > alloc).
CLASSICAL_CPU = {"pi": "2500m", "intel": "6000m"}
CLASSICAL_MEM = {"sensor": "256Mi", "heavy": "256Mi", "shuffle": "256Mi",
                 "aggregate": "200Mi", "report": "128Mi"}


def tier_of(cls):
    return CLASSES[cls]["tier"]


def cover_assign(producers, consumers, rng, extra_prob):
    prod = producers[:]
    rng.shuffle(prod)
    deps = {c: [] for c in consumers}
    for i, p in enumerate(prod):
        deps[consumers[i % len(consumers)]].append(p)
    for c in consumers:
        if rng.random() < extra_prob:
            e = rng.choice(producers)
            if e not in deps[c]:
                deps[c].append(e)
    return deps


def build(seed):
    rng = random.Random(seed)
    S = [f"sensor-{i}" for i in range(CLASSES["sensor"]["count"])]
    H = [f"heavy-{i}" for i in range(CLASSES["heavy"]["count"])]
    SH = [f"shuffle-{i}" for i in range(CLASSES["shuffle"]["count"])]
    AG = [f"aggregate-{i}" for i in range(CLASSES["aggregate"]["count"])]
    tasks = [dict(name=s, cls="sensor", deps=[]) for s in S]
    hd = cover_assign(S, H, rng, 0.5)
    tasks += [dict(name=h, cls="heavy", deps=hd[h]) for h in H]
    sd = cover_assign(H, SH, rng, 1.0)
    tasks += [dict(name=s, cls="shuffle", deps=sd[s]) for s in SH]
    ad = cover_assign(SH, AG, rng, 1.0)
    tasks += [dict(name=a, cls="aggregate", deps=ad[a]) for a in AG]
    tasks.append(dict(name="report", cls="report", deps=list(AG)))
    return tasks


def solve_ccr(tasks, target):
    """Total compute (s), fixed comm (s, all non-sensor edges), and sensor comm
    coefficient (s per byte of sensor payload). Solve sensor payload bytes."""
    by = {t["name"]: t for t in tasks}
    comp = sum(CLASSES[t["cls"]]["iters"] / TPUT[tier_of(t["cls"])] for t in tasks)
    fixed_comm = 0.0
    sensor_edges = 0
    for t in tasks:
        ct = tier_of(t["cls"])
        for d in t["deps"]:
            pt = tier_of(by[d]["cls"])
            bw = BW[(pt, ct)]
            if by[d]["cls"] == "sensor":
                sensor_edges += 1  # coefficient applied after solving payload
            else:
                fixed_comm += FIXED_BYTES[by[d]["cls"]] / bw
    # sensor edges are pi->intel; coefficient = sensor_edges / BW(pi,intel)
    coef = sensor_edges / BW[("pi", "intel")]
    # target = (fixed_comm + coef * P_s) / comp  ->  P_s
    p_s = (target * comp - fixed_comm) / coef
    return comp, fixed_comm, sensor_edges, p_s


def emit(tasks, name, alpha, sensor_bytes):
    out = []
    a = out.append
    a("apiVersion: wl.io/v1"); a("kind: ODAGTemplate")
    a("metadata:"); a(f"  name: {name}"); a("  namespace: wl-system")
    a("spec:")
    a(f'  description: "CCR=1 classical-HEFT convergence benchmark (Intel+Pi)"')
    a("  scheduler: heft")
    a("  schedulerConfig:")
    a("    spreadEpsilon: 0")
    a("    exclusivePerNode: true")
    a("  profiling:")
    a("    enabled: true"); a("    warmupRuns: 0"); a("    minSamples: 2")
    a(f"    emaAlpha: {alpha}")
    a("    maxSamples: 100")
    a("    runtimeSource: profiler"); a("    bandwidthSource: external")
    a("  defaults: {runtime: 3, dataSize: \"0\"}")
    a("  retention:")
    a("    maxRuns: 100")
    a("    data: {policy: keepLatest, keepRuns: 2, maxSizePerNode: \"3Gi\"}")
    a("  tasks:")
    for t in tasks:
        cls = t["cls"]
        iters = CLASSES[cls]["iters"]
        tier = tier_of(cls)
        data = int(sensor_bytes) if cls == "sensor" else FIXED_BYTES[cls]
        rt_hint = max(1, round(iters / TPUT[tier]))  # cold-start estimate (s)
        nodes = PI if tier == "pi" else INTEL
        a(f"    - name: {t['name']}")
        a(f"      image: {IMAGE}")
        a('      command: ["python", "task.py"]')
        a(f"      dependencies: [{', '.join(t['deps'])}]" if t["deps"] else "      dependencies: []")
        a(f"      runtime: {rt_hint}")
        a(f'      dataSize: "{data}"')
        a(f"      env: [{{name: WL_ITERS, value: \"{iters}\"}}]")
        a("      resources:")
        a(f'        cpu: "{CLASSICAL_CPU[tier]}"')
        a(f'        memory: "{CLASSICAL_MEM[cls]}"')
        a("      constraints:")
        a(f"        nodeNames: [{', '.join(nodes)}]")
    return "\n".join(out) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--ccr", type=float, default=1.0)
    ap.add_argument("--alpha", type=float, default=0.7)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("-o", "--out", default="-")
    args = ap.parse_args()

    tasks = build(args.seed)
    comp, fixed_comm, sedges, p_s = solve_ccr(tasks, args.ccr)
    realized_comm = fixed_comm + sedges * p_s / BW[("pi", "intel")]
    y = emit(tasks, args.name, args.alpha, p_s)
    if args.out == "-":
        sys.stdout.write(y)
    else:
        open(args.out, "w").write(y)
    sys.stderr.write(
        f"[gen_ccr] {args.name}: tasks={len(tasks)} target_CCR={args.ccr} alpha={args.alpha}\n"
        f"  total compute = {comp:.1f}s ; sensor edges = {sedges} ; fixed(Intel) comm = {fixed_comm:.1f}s\n"
        f"  -> sensor payload = {p_s/MB:.1f} MB ; realized comm = {realized_comm:.1f}s ; CCR = {realized_comm/comp:.2f}\n")


if __name__ == "__main__":
    main()
