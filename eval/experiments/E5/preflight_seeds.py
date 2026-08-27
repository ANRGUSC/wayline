#!/usr/bin/env python3
"""Are HEFT and MaxTP frozen-stable across policy seeds 0..N-1?

The E5 paper campaign sets one policy seed per block so OLB's
tie-breaking sensitivity is sampled fairly. That only works if the
frozen-replay policies are INSENSITIVE to the same seed: if HEFT moves
when the seed moves, every HEFT arm fails its frozen-hash check and the
campaign is a total loss.

HEFT is precisely the policy we caught tie-breaking through a randomized
set, so this must be measured, not assumed.

Reads the placement the scheduler chose as soon as it is assigned; does
not wait for the DAG to finish. Restores PYTHONHASHSEED=0 on exit.

Usage: preflight_seeds.py [n_seeds]
"""
import json
import subprocess
import sys

NS = "wl-system"
NSEEDS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
FROZEN = "/home/anrg/E5-frozen"
APP = ["source", "a", "b", "c", "j1", "j2", "sink"]


def sh(cmd, timeout=420):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def set_seed(seed):
    sh(f"kubectl -n {NS} set env deploy/odag-controller -c saga-sidecar "
       f"PYTHONHASHSEED={seed}")
    sh(f"kubectl -n {NS} rollout status deploy/odag-controller --timeout=300s")


def placement(template):
    r = sh(f"/home/anrg/wayline/bin/wayline run {template} -n {NS}")
    run = ""
    for tok in (r.stdout + r.stderr).split():
        if tok.startswith(f"{template}-run-"):
            run = tok.strip().strip('",')
            break
    if not run:
        return None
    got = {}
    for _ in range(90):
        out = sh(f"kubectl -n {NS} get odag {run} -o json").stdout
        try:
            d = json.loads(out)
        except json.JSONDecodeError:
            sh("sleep 2")
            continue
        got = {t["name"]: t.get("node")
               for t in d.get("status", {}).get("tasks", [])
               if t.get("node") and t["name"] in APP}
        if len(got) == len(APP):
            break
        sh("sleep 2")
    sh(f"kubectl -n {NS} delete odag {run} --wait=false >/dev/null 2>&1")
    return got


def main():
    want = {}
    for algo in ("heft", "maxtp"):
        want[algo] = json.load(
            open(f"{FROZEN}/frozen-{algo}.json"))["placement"]
    bad = []
    try:
        for seed in range(NSEEDS):
            set_seed(seed)
            line = [f"seed {seed}:"]
            for algo in ("heft", "maxtp"):
                got = placement(f"e5-{algo}")
                ok = (got == want[algo])
                line.append(f"{algo}={'OK' if ok else 'MOVED'}")
                if not ok:
                    bad.append((seed, algo, got))
            print("  " + "  ".join(line), flush=True)
    finally:
        print("\nrestoring PYTHONHASHSEED=0 ...", flush=True)
        set_seed(0)
        print("restored.", flush=True)

    print()
    if not bad:
        print(f"VERDICT: HEFT and MaxTP are frozen-stable across all "
              f"{NSEEDS} seeds. A per-block policy seed is safe: set it "
              f"once per block and run all arms under it.")
    else:
        print(f"VERDICT: frozen replay is NOT seed-stable "
              f"({len(bad)} mismatch(es)).")
        for seed, algo, got in bad[:6]:
            print(f"  seed {seed} {algo}: {json.dumps(got, sort_keys=True)}")
        print("  A per-block seed applied to ALL arms would invalidate "
              "every affected arm. Pin seed 0 for the frozen-replay arms "
              "and apply the block seed only to the OLB arms.")


if __name__ == "__main__":
    main()
