#!/usr/bin/env python3
"""E5 paper campaign: 20 blocks x 8 arms = 160 arm-runs.

Design (per the campaign spec):
  * 10 policy seeds, each used in exactly 2 of the 20 blocks.
  * One policy seed chosen per block, applied to both OLB arms so OLB's
    tie-breaking sensitivity is sampled fairly instead of frozen at the
    seed-0 point estimate.
  * The 20 seeded blocks are run in randomised order; arms are shuffled
    within each block.
  * Shaped network identical to the pilot.
  * OLB is direct-only. HEFT and MaxTP supply the direct-vs-store
    comparison at identical placement and order.

POLICY_SEED_SCOPE decides who runs under the block's policy seed:
  all  - every arm (valid only if frozen replay is seed-stable, which
         preflight_seeds.py must confirm)
  olb  - only the OLB arms; HEFT/MaxTP stay pinned at seed 0 so their
         frozen-hash checks remain meaningful

Mandatory checks added on top of every pilot check:
  * both OLB arms in a block must agree on placement hash
  * HEFT/MaxTP must match frozen placement and node order under
    whatever seed they ran at
  * policy_seed recorded separately from the campaign shuffle seed
  * PYTHONHASHSEED restored to 0 on teardown, unconditionally

Usage (on anrg-2):
  BLOCKS=20 SEED=... RES=... FROZEN=... POLICY_SEED_SCOPE=olb e5_paper.py
"""
import csv
import json
import os
import random
import statistics as st
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RES = os.environ.get("RES", os.path.expanduser("~/E5-paper"))
os.environ["RES"] = RES
os.makedirs(RES, exist_ok=True)

import e5_pilot as P  # noqa: E402
import gen_e5 as G  # noqa: E402

SCOPE = os.environ.get("POLICY_SEED_SCOPE", "olb")
NBLOCKS = int(os.environ.get("BLOCKS", "20"))
CAMPAIGN_SEED = int(os.environ.get("SEED", "20260828"))
NSEEDS = int(os.environ.get("POLICY_SEEDS", "10"))
REPS = NBLOCKS // NSEEDS

if "policy_seed" not in P.FIELDS:
    P.FIELDS = P.FIELDS + ["policy_seed"]
FIELDS = P.FIELDS
OLB_ARMS = {"iso-olb-direct", "batch-olb-direct"}


def set_policy_seed(seed):
    P.sh(f"kubectl -n {P.NS} set env deploy/odag-controller -c saga-sidecar "
         f"PYTHONHASHSEED={seed}")
    P.sh(f"kubectl -n {P.NS} rollout status deploy/odag-controller "
         f"--timeout=300s", timeout=400)


def build_row(regime, arm, tpl, realization, algo, fr, idx, block):
    """Run one arm and assemble its row. Mirrors the pilot exactly."""
    if regime == "iso":
        rec, reasons, extra = P.run_iso(arm, tpl, realization, algo, fr,
                                        idx, block)
        if rec is None:
            return None, None, ["submit failed"]
        mk = [int(rec["makespan"])] if rec.get("makespan") else []
        row = dict(runs=1, completed=int(rec["phase"] == "Succeeded"),
                   makespan_med=st.median(mk) if mk else "",
                   makespan_p95="", batch_seconds="", dags_per_min="",
                   latency_med="", latency_p95="", busy_cov="",
                   busy_by_node=json.dumps(rec["busy"]),
                   placement=json.dumps(rec["placement"]),
                   node_order=json.dumps(rec["order"]),
                   pairs=json.dumps(rec["pairs"]),
                   gw_in=rec["gw_in"], gw_out=rec["gw_out"])
        if rec["phase"] != "Succeeded":
            reasons.append(f"phase={rec['phase']}")
        return row, extra, reasons

    recs, secs, done, reasons = P.run_batch(arm, tpl, realization, algo, fr)
    extra = {}
    if recs:
        ctrl = P.ctrl_slice(recs[-1]["run"])
        _, more, extra = P.check_run(recs[-1], arm, realization, algo, fr,
                                     ctrl)
        reasons += more
    lat = sorted(r["latency"] for r in recs if "latency" in r)
    mks = sorted(int(r["makespan"]) for r in recs if r.get("makespan"))
    busy = {}
    for r in recs:
        for n, v in r["busy"].items():
            busy[n] = busy.get(n, 0) + v
    vals = [busy.get(n, 0) for n in G.EDGE + G.COMPUTE]
    cov = (st.pstdev(vals) / st.mean(vals)) if vals and st.mean(vals) else ""
    pairs, gin, gout = {}, 0, 0
    for r in recs:
        for k, v in r["pairs"].items():
            pairs[k] = pairs.get(k, 0) + v
        gin += r["gw_in"]
        gout += r["gw_out"]
    row = dict(runs=len(recs), completed=done,
               makespan_med=st.median(mks) if mks else "",
               makespan_p95=(mks[int(.95 * len(mks)) - 1] if mks else ""),
               batch_seconds=round(secs, 1),
               dags_per_min=round(done / (secs / 60), 2) if secs else "",
               latency_med=round(st.median(lat), 1) if lat else "",
               latency_p95=(round(lat[int(.95 * len(lat)) - 1], 1)
                            if lat else ""),
               busy_cov=round(cov, 3) if cov != "" else "",
               busy_by_node=json.dumps(busy),
               placement=json.dumps(recs[-1]["placement"] if recs else {}),
               node_order=json.dumps(recs[-1]["order"] if recs else {}),
               pairs=json.dumps(pairs), gw_in=gin, gw_out=gout)
    if done < P.BATCH_TARGET:
        reasons.append(f"only {done}/{P.BATCH_TARGET} completed")
    return row, extra, reasons


def main():
    P.AGENTS.update(P.agent_ips())
    P.fw_pods()
    frozen = {}
    for a in ("heft", "maxtp"):
        fp = os.path.join(P.FROZEN, f"frozen-{a}.json")
        if os.path.exists(fp):
            frozen[a] = json.load(open(fp))
    P.sh(f"kubectl apply -f {P.E5DIR}/e5-bandwidth.yml >/dev/null")
    for f in ("e5-heft.yml", "e5-maxtp.yml", "e5-olb.yml"):
        P.sh(f"kubectl apply -f {P.E5DIR}/{f} >/dev/null")
    for algo in ("heft", "maxtp"):
        y = f"{P.E5DIR}/e5-store-{algo}.yml"
        P.sh(f"python3 {P.E5DIR}/gen_e5.py store "
             f"{P.FROZEN}/frozen-{algo}.json > {y}")
        P.sh(f"kubectl apply -f {y} >/dev/null")
    print(P.net("apply"), flush=True)

    rng = random.Random(CAMPAIGN_SEED)
    # Each policy seed appears in exactly REPS blocks; block order shuffled.
    blocks = [s for s in range(NSEEDS) for _ in range(REPS)]
    rng.shuffle(blocks)

    img = P.sh("kubectl -n wl-system get deploy odag-controller -o "
               "jsonpath='{.spec.template.spec.containers[*].image}'").stdout
    dig = P.sh("kubectl -n wl-system get pods -l app=odag-controller -o "
               "jsonpath='{.items[0].status.containerStatuses[*].imageID}'"
               ).stdout
    with open(f"{RES}/PROVENANCE.txt", "w") as pf:
        pf.write(f"campaign_seed {CAMPAIGN_SEED}\nblocks {NBLOCKS}\n"
                 f"policy_seeds 0..{NSEEDS - 1}, each in {REPS} block(s)\n"
                 f"policy_seed_scope {SCOPE}\n"
                 f"block_policy_seeds {blocks}\n"
                 f"controller_image {img.strip()}\n"
                 f"controller_imageID {dig.strip()}\n"
                 f"frozen_dir {P.FROZEN}\n"
                 f"olb is direct-only by design; heft/maxtp carry the\n"
                 f"direct-vs-store comparison at identical placement+order\n")

    with open(f"{RES}/execution-order.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["block", "policy_seed"])
        for b, s in enumerate(blocks, 1):
            w.writerow([b, s])

    idx = 0
    try:
        with open(f"{RES}/runs.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            for block, pseed in enumerate(blocks, 1):
                # Disk telemetry per block. This campaign is ~7x the
                # pilot; printing the trend lets us see exhaustion coming
                # at block 3 instead of discovering it at block 18.
                fpb = P.free_pct()
                print(f"[e5] block={block}/{NBLOCKS} seed={pseed} "
                      f"disk_free={fpb}%", flush=True)
                if fpb is not None and fpb < 15:
                    raise SystemExit(
                        f"disk headroom {fpb}% below 15%: stopping before a "
                        f"shaper or agent eviction can corrupt later blocks")
                arms = [a for a in P.ARMS]
                rng.shuffle(arms)
                if SCOPE == "olb":
                    # Group by seed so each block costs 2 sidecar restarts
                    # rather than one per arm; randomise which group leads.
                    olb = [a for a in arms if a[0] in OLB_ARMS]
                    rest = [a for a in arms if a[0] not in OLB_ARMS]
                    groups = [(pseed, olb), (0, rest)]
                    if rng.random() < 0.5:
                        groups.reverse()
                else:
                    groups = [(pseed, arms)]

                block_rows, olb_hashes = [], {}
                for seed_for_group, group in groups:
                    if not group:
                        continue
                    set_policy_seed(seed_for_group)
                    for (arm, regime, tpl, realization, algo) in group:
                        idx += 1
                        used = seed_for_group
                        P.POLICY_SEED = used
                        fpct = P.free_pct()
                        if fpct is not None and fpct < 12:
                            print(f"[e5] #{idx} {arm}: ABORT free={fpct}%",
                                  flush=True)
                            raise SystemExit("insufficient disk headroom")
                        nv = P.net("verify")
                        if "verified" not in nv:
                            print(f"[e5] #{idx} {arm}: NET NOT VERIFIED",
                                  flush=True)
                            blank = dict.fromkeys(
                                ["runs", "completed", "makespan_med",
                                 "makespan_p95", "batch_seconds",
                                 "dags_per_min", "latency_med", "latency_p95",
                                 "busy_cov", "busy_by_node", "placement",
                                 "node_order", "pairs", "gw_in", "gw_out"], "")
                            block_rows.append(P.row_values(
                                idx, block, arm, regime, algo, realization,
                                blank, {}, ["net-not-verified"], False,
                                CAMPAIGN_SEED))
                            continue
                        print(f"[e5] #{idx} block={block} seed={used} {arm}",
                              flush=True)
                        row, extra, reasons = build_row(
                            regime, arm, tpl, realization, algo,
                            frozen.get(algo), idx, block)
                        if row is None:
                            continue
                        if arm in OLB_ARMS:
                            olb_hashes[arm] = extra.get("hash", "")
                        valid = not reasons
                        block_rows.append((idx, block, arm, regime, algo,
                                           realization, row, extra, reasons,
                                           valid, used))
                        print(f"[e5] #{idx} {arm}: valid={valid} "
                              f"{'reasons=' + ';'.join(reasons) if reasons else ''} "
                              f"seed={used} makespan={row['makespan_med']} "
                              f"batch={row['batch_seconds']} "
                              f"gw_bytes={row['gw_in']}", flush=True)

                # Cross-arm check: both OLB arms in a block must have been
                # scheduled identically. They ran under the same policy
                # seed on the same cluster, so a mismatch means the seed
                # did not actually govern the placement and the block's
                # OLB pair cannot be aggregated.
                hs = [h for h in olb_hashes.values() if h]
                mismatch = len(set(hs)) > 1
                if mismatch:
                    print(f"[e5] block={block}: OLB placement hashes differ "
                          f"{olb_hashes}", flush=True)
                for item in block_rows:
                    if isinstance(item, list):
                        w.writerow(item)
                        continue
                    (i, b, arm, regime, algo, realization, row, extra,
                     reasons, valid, used) = item
                    if mismatch and arm in OLB_ARMS:
                        reasons = reasons + ["olb placement differs between "
                                             "iso and batch in this block"]
                        valid = False
                    P.POLICY_SEED = used
                    w.writerow(P.row_values(i, b, arm, regime, algo,
                                            realization, row, extra, reasons,
                                            valid, CAMPAIGN_SEED))
                f.flush()
    finally:
        print(P.net("clear"), flush=True)
        for node in G.NODES:
            P.kubectl(f"delete pod e5-fw-{node} --ignore-not-found "
                      f">/dev/null 2>&1")
        print("restoring PYTHONHASHSEED=0 ...", flush=True)
        set_policy_seed(0)
        print("restored.", flush=True)
    print("E5 PAPER DONE", flush=True)


if __name__ == "__main__":
    main()
