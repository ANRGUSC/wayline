#!/usr/bin/env python3
"""E17 analysis: the MILCOM 2026 schedule-repair study, testbed against simulation.

  analyze.py --scenarios DIR --results DIR [DIR ...] --out DIR

Every testbed run is compared with the simulator's run of the same scenario
and policy (scenarios.py stored it). Two normalizations:

  makespan / M0     each side by its own undisrupted makespan (testbed M0 is
                    the lane's m0 run): the fidelity measure, free of any
                    oracle approximation.
  makespan / oracle the paper's metric. The testbed oracle is the best of the
                    candidates that ran there: the simulator oracle's winning
                    fixed placement (when it is one) and reactive global
                    repair (global@0), which is the other member of the
                    oracle's portfolio.

Outputs: runs.csv (one row per run), summary.md (fidelity and the paper's
aggregates side by side), and figures when matplotlib is available.
"""
import argparse
import csv
import glob
import json
import math
import os
import statistics
from collections import defaultdict

FAMILY = {"out_tree": "synthetic", "in_tree": "synthetic", "parallel_chains": "synthetic",
          "etl": "riotbench", "stats": "riotbench", "train": "riotbench", "predict": "riotbench",
          "epigenomics": "wfcommons", "montage": "wfcommons", "seismology": "wfcommons"}
BASE = ["wait", "reroute", "local@0", "global@0"]
DELTAS = ["0", "0.02", "0.1", "0.5"]


def gmean(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float("nan")


def platform(lane):
    """Tier 1 lanes are one hardware type; a Tier 2 lane mixes them."""
    kinds = {"up" if n.startswith("anrg") else ("pi4" if n.startswith("rpi4-") else "pi3") for n in lane}
    return kinds.pop() if len(kinds) == 1 else "mixed"


def load(scen_dir, result_dirs):
    scen = {}
    for f in glob.glob(os.path.join(scen_dir, "*.json")):
        d = json.load(open(f))
        scen[d["id"]] = d
    runs = []   # one dict per (scenario, repeat, lane) with every label
    groups = defaultdict(dict)
    for rd in result_dirs:
        for f in glob.glob(os.path.join(rd, "*", "r*-*.json")):
            r = json.load(open(f))
            key = (r["scenario"], r.get("repeat", 0), r["lane"][0], rd)
            groups[key][r["label"]] = r
    for (sid, rep, lane0, rd), recs in groups.items():
        if sid not in scen or "m0" not in recs:
            continue
        runs.append({"sid": sid, "repeat": rep, "lane": recs["m0"]["lane"], "dir": rd, "recs": recs})
    return scen, runs


def tb_ms(rec):
    if rec is None or rec.get("status") != "Succeeded":
        return None
    return (rec.get("final") or {}).get("makespan")


ABL = {}


def sim_variant(d, skey, variant):
    """The simulated makespan of `skey` under a simulator variant:
    "paper"     as published (scenarios.py);
    "fixed"     the duplicate-transfer bug fixed (affects multi-hop routing,
                i.e. reroute);
    "aligned"   fixed, and the data-plane modeling choices set to what the
                testbed does: in-flight transfers follow route changes
                (reroute); a move does not abort inputs already on their way
                to the old node, and copies that landed are reused when the
                task returns (local/global)."""
    base = d["sim"].get(skey, {}).get("makespan")
    if variant == "paper" or base is None:
        return base
    rec = ABL.get(d["id"], {})
    if skey.startswith("reroute"):
        k = f"{skey}|fix-dup" if variant == "fixed" else f"{skey}|fix-dup+ip-reroute"
        return rec.get(k, {}).get("makespan", base if not rec else None)
    if variant == "aligned" and skey.split("@")[0] in ("local", "global"):
        return rec.get(f"{skey}|keep", {}).get("makespan")
    return base


def rows_for(scen, run):
    d = scen[run["sid"]]
    recs = run["recs"]
    m0_tb = tb_ms(recs["m0"])
    if not m0_tb:
        return []
    sim_or = d["oracle"]["makespan"]
    g0 = tb_ms(recs.get("global@0"))
    orc = tb_ms(recs.get("oracle"))
    if orc is None and d["oracle"]["placement"] is not None and d["oracle"]["placement"] == d["plan"]:
        orc = tb_ms(recs.get("wait"))      # the oracle's placement is the initial plan: that run is wait
    cands = [x for x in (g0, orc) if x]
    tb_or = min(cands) if cands else None
    out = []
    for label, rec in recs.items():
        if label == "m0":
            continue
        skey = label if "@" in label else f"{label}@0"
        sim_ms = sim_or if label == "oracle" else d["sim"].get(skey, {}).get("makespan")
        ms = tb_ms(rec)
        fx = sim_or if label == "oracle" else sim_variant(d, skey, "fixed")
        al = sim_or if label == "oracle" else sim_variant(d, skey, "aligned")
        out.append({
            "sim_fixed_norm": fx / d["m0"] if fx else None,
            "sim_fixed_ratio": fx / sim_or if fx else None,
            "sim_aligned_norm": al / d["m0"] if al else None,
            "sim_aligned_ratio": al / sim_or if al else None,
            "scenario": d["id"], "structure": d["structure"], "family": FAMILY[d["structure"]],
            "ccr": d["ccr"], "generator": d["generator"], "label": label,
            "policy": label.split("@")[0], "delta": label.split("@")[1] if "@" in label else "",
            "repeat": run["repeat"], "lane": run["lane"][0], "platform": platform(run["lane"]),
            "status": rec.get("status"), "m0_sim": d["m0"], "m0_tb": m0_tb,
            "m0_target": rec.get("T"), "inflation": m0_tb / rec["T"] - 1 if rec.get("T") else None,
            "sim_norm": sim_ms / d["m0"] if sim_ms else None,
            "tb_norm": ms / m0_tb if ms else None,
            "sim_ratio": sim_ms / sim_or if sim_ms else None,
            "tb_ratio": ms / tb_or if ms and tb_or else None,
            "sim_oracle_label": d["oracle"]["label"],
            "events": len(rec.get("events", [])),
            "late_events": sum(1 for e in rec.get("events", []) if e["applied"] - e["due"] > 0.5),
            "plans": len(rec.get("plans", [])),
            "plans_applied": sum(1 for p in rec.get("plans", []) if "applied" in p),
            "plan_compute_max": max([p.get("compute", 0) for p in rec.get("plans", [])] or [0]),
        })
    return out


def winners(rows, labels, key):
    """Per scenario/repeat, the best label by `key` (ties within 1% share it)."""
    by = defaultdict(dict)
    for r in rows:
        if r["label"] in labels and r[key]:
            by[(r["scenario"], r["repeat"], r["lane"])][r["label"]] = r[key]
    out = {}
    for k, v in by.items():
        if len(v) == len(labels):
            best = min(v.values())
            out[k] = {l for l, x in v.items() if x <= best * 1.01}
    return out


def fmt(x, p=3):
    return "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.{p}f}"


def summary(rows, out_dir):
    ok = [r for r in rows if r["sim_norm"] and r["tb_norm"]]
    L = []
    L.append("# E17: MILCOM 2026 schedule repair, testbed vs simulation\n")
    scen = sorted({r["scenario"] for r in rows})
    lanes = sorted({r["lane"] for r in rows})
    L.append(f"{len(scen)} scenarios, {len(ok)} paired runs ({len(rows) - len(ok)} unpaired: "
             f"{sum(1 for r in rows if r['status'] != 'Succeeded')} not succeeded), {len(lanes)} lanes.\n")
    m0 = {}
    for r in rows:
        m0[(r["scenario"], r["repeat"], r["lane"])] = (r["inflation"], r["platform"])
    for plat in ("pi3", "up", "pi4", "mixed"):
        xs = [v[0] for v in m0.values() if v[1] == plat and v[0] is not None]
        if xs:
            L.append(f"- M0 overhead on {plat}: median {statistics.median(xs) * 100:.1f}%, "
                     f"p90 {sorted(xs)[int(0.9 * (len(xs) - 1))] * 100:.1f}%, max {max(xs) * 100:.1f}% (n={len(xs)})")
    L.append("")
    # Fidelity of makespan / M0.
    L.append("## Fidelity: testbed makespan/M0 vs simulated makespan/M0\n")
    L.append("Relative error e = (tb/M0_tb) / (sim/M0_sim) - 1.\n")
    L.append("| slice | n | median e | median abs e | p90 abs e | within 5% | within 10% |")
    L.append("|---|---|---|---|---|---|---|")

    def fid(name, rs, key="sim_norm"):
        es = [r["tb_norm"] / r[key] - 1 for r in rs if r.get(key)]
        if not es:
            return
        ae = sorted(abs(e) for e in es)
        L.append(f"| {name} | {len(es)} | {statistics.median(es) * 100:+.1f}% | {statistics.median(ae) * 100:.1f}% | "
                 f"{ae[int(0.9 * (len(ae) - 1))] * 100:.1f}% | {sum(1 for e in ae if e <= .05) / len(ae) * 100:.0f}% | "
                 f"{sum(1 for e in ae if e <= .10) / len(ae) * 100:.0f}% |")
    fid("all", ok)
    for lab in BASE + ["oracle"]:
        fid(lab, [r for r in ok if r["label"] == lab])
    for g in ("single", "flapping", "mobility", "jamming", "episode"):
        fid(f"gen={g}", [r for r in ok if r["generator"] == g])
    for c in sorted({r["ccr"] for r in ok}):
        fid(f"ccr={c:g}", [r for r in ok if r["ccr"] == c])
    for fam in ("synthetic", "riotbench", "wfcommons"):
        fid(f"family={fam}", [r for r in ok if r["family"] == fam])
    for plat in ("pi3", "up", "mixed"):
        fid(f"platform={plat}", [r for r in ok if r["platform"] == plat])
    L.append("")
    if ABL:
        L.append("### The same, against the corrected and the aligned simulator\n")
        L.append("fixed: ncsim's duplicate-transfer bug fixed (multi-hop routing only). "
                 "aligned: fixed, plus in-flight transfers follow route changes, and a move keeps "
                 "inputs in flight to the old node and reuses landed copies (what the testbed's "
                 "data plane does).\n")
        L.append("| slice | n | median e | median abs e | p90 abs e | within 5% | within 10% |")
        L.append("|---|---|---|---|---|---|---|")
        for key, nm in (("sim_fixed_norm", "fixed"), ("sim_aligned_norm", "aligned")):
            fid(f"{nm}: all", ok, key)
            for lab in ("reroute", "local@0", "global@0"):
                fid(f"{nm}: {lab}", [r for r in ok if r["label"] == lab], key)
            fid(f"{nm}: gen=flapping", [r for r in ok if r["generator"] == "flapping"], key)
            fid(f"{nm}: family=wfcommons", [r for r in ok if r["family"] == "wfcommons"], key)
        L.append("")
    # Worst disagreements.
    L.append("### Largest disagreements (makespan/M0)\n")
    L.append("| scenario | label | sim | testbed | e |")
    L.append("|---|---|---|---|---|")
    for r in sorted(ok, key=lambda r: -abs(math.log(r["tb_norm"] / r["sim_norm"])))[:15]:
        L.append(f"| {r['scenario']} | {r['label']} | {fmt(r['sim_norm'])} | {fmt(r['tb_norm'])} | "
                 f"{(r['tb_norm'] / r['sim_norm'] - 1) * 100:+.0f}% |")
    L.append("")
    # Winner agreement.
    L.append("## Decision agreement at zero repair cost\n")
    for key, nm in (("sim_norm", "sim"), ("tb_norm", "tb")):
        pass
    ws = winners(ok, BASE, "sim_norm")
    wt = winners(ok, BASE, "tb_norm")
    both = [k for k in ws if k in wt]
    agree = sum(1 for k in both if ws[k] & wt[k])
    L.append(f"Best policy among {', '.join(BASE)} per scenario (ties within 1% count as shared): "
             f"testbed and simulation agree in {agree}/{len(both)} ({agree / max(1, len(both)) * 100:.0f}%).\n")
    by_gen = defaultdict(lambda: [0, 0])
    for k in both:
        g = next(r["generator"] for r in ok if (r["scenario"], r["repeat"], r["lane"]) == k)
        by_gen[g][1] += 1
        by_gen[g][0] += bool(ws[k] & wt[k])
    L.append(" | ".join(f"{g}: {a}/{n}" for g, (a, n) in sorted(by_gen.items())) + "\n")
    # The paper's aggregates.
    L.append("## The paper's aggregates on this scenario set (geometric mean of makespan/oracle, delta = 0)\n")

    keys = [("sim_ratio", "sim")] + ([("sim_fixed_ratio", "fixed")] if ABL else []) + [("tb_ratio", "tb")]

    def table(title, slicer, values):
        L.append(f"### {title}\n")
        head = " / ".join(n for _, n in keys)
        L.append("| " + " | ".join(["slice"] + [f"{p} {head}" for p in BASE]) + f" | best {head} |")
        L.append("|" + "---|" * (len(BASE) + 2))
        for v in values:
            rs = [r for r in ok if slicer(r) == v]
            if not rs:
                continue
            cells, bests = [], {k: {} for k, _ in keys}
            for p in BASE:
                vals = []
                for k, _ in keys:
                    g = gmean([r[k] for r in rs if r["label"] == p])
                    bests[k][p] = g
                    vals.append(fmt(g))
                cells.append(" / ".join(vals))
            best = lambda m: min((x for x in m.items() if not math.isnan(x[1])), key=lambda x: x[1], default=("n/a", 0))[0]
            L.append(f"| {v} | " + " | ".join(cells) + " | " + " / ".join(best(bests[k]) for k, _ in keys) + " |")
        L.append("")
    table("By disruption process", lambda r: r["generator"], ["single", "flapping", "mobility", "jamming"])
    for g in ("single", "flapping", "mobility", "jamming"):
        table(f"{g}, by CCR", lambda r, g=g: (r["ccr"] if r["generator"] == g else None),
              sorted({r["ccr"] for r in ok}))
    for g in ("flapping", "jamming", "mobility"):
        table(f"{g}, by family", lambda r, g=g: (r["family"] if r["generator"] == g else None),
              ["synthetic", "riotbench", "wfcommons"])
    # Cost sweep.
    L.append("## Repair cost sweep (geometric mean of makespan/oracle)\n")
    L.append("| process | policy | " + " | ".join(f"delta={d} sim / tb" for d in DELTAS) + " |")
    L.append("|---|---|" + "---|" * len(DELTAS))
    for g in ("single", "flapping", "mobility", "jamming"):
        for p in ("local", "global"):
            cells = []
            for dl in DELTAS:
                rs = [r for r in ok if r["generator"] == g and r["label"] == f"{p}@{dl}"]
                cells.append(f"{fmt(gmean([r['sim_ratio'] for r in rs]))} / {fmt(gmean([r['tb_ratio'] for r in rs]))}")
            L.append(f"| {g} | {p} | " + " | ".join(cells) + " |")
        for p in ("wait", "reroute"):
            rs = [r for r in ok if r["generator"] == g and r["label"] == p]
            v = f"{fmt(gmean([r['sim_ratio'] for r in rs]))} / {fmt(gmean([r['tb_ratio'] for r in rs]))}"
            L.append(f"| {g} | {p} | " + " | ".join([v] * len(DELTAS)) + " |")
    L.append("")
    open(os.path.join(out_dir, "summary.md"), "w").write("\n".join(L))
    return "\n".join(L)


def figures(rows, out_dir):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    ok = [r for r in rows if r["sim_norm"] and r["tb_norm"]]
    colors = {"wait": "#7a7a7a", "reroute": "#2f7ed8", "local": "#e5a00d", "global": "#c0392b", "oracle": "#2e8b57"}
    fig, ax = plt.subplots(figsize=(5.2, 5))
    for p, c in colors.items():
        rs = [r for r in ok if r["policy"] == p]
        ax.scatter([r["sim_norm"] for r in rs], [r["tb_norm"] for r in rs], s=10, c=c, label=p, alpha=.7)
    hi = max(max(r["sim_norm"] for r in ok), max(r["tb_norm"] for r in ok)) * 1.05
    ax.plot([0.8, hi], [0.8, hi], "k-", lw=.8)
    ax.plot([0.8, hi], [0.88, hi * 1.1], "k:", lw=.6)
    ax.plot([0.8, hi], [0.72, hi * .9], "k:", lw=.6)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("simulated makespan / M0"); ax.set_ylabel("testbed makespan / M0")
    ax.legend(fontsize=8); ax.set_title("E17: every run, testbed vs simulation")
    fig.tight_layout(); fig.savefig(os.path.join(out_dir, "fidelity_scatter.png"), dpi=150); plt.close(fig)
    gens = ["single", "flapping", "mobility", "jamming"]
    fig, axs = plt.subplots(1, 4, figsize=(12, 3.2))
    for ax, g in zip(axs, gens):
        xs = range(len(BASE))
        s = [gmean([r["sim_ratio"] for r in ok if r["generator"] == g and r["label"] == p]) for p in BASE]
        t = [gmean([r["tb_ratio"] for r in ok if r["generator"] == g and r["label"] == p]) for p in BASE]
        ax.bar([x - .2 for x in xs], s, .4, color="#9db4c0", label="simulation")
        ax.bar([x + .2 for x in xs], t, .4, color="#2c4a52", label="testbed")
        ax.set_xticks(list(xs)); ax.set_xticklabels([p.split("@")[0] for p in BASE], fontsize=8)
        ax.axhline(1, color="k", lw=.6); ax.set_title(g)
        ax.set_ylim(min(0.9, min(s + t) * .97), max(s + t) * 1.05)
    axs[0].set_ylabel("makespan / oracle (geo. mean)"); axs[0].legend(fontsize=7)
    fig.tight_layout(); fig.savefig(os.path.join(out_dir, "ladder.png"), dpi=150); plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", required=True)
    ap.add_argument("--results", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ablation", nargs="*", default=[], help="ablation.py outputs (corrected/aligned simulator)")
    ap.add_argument("--exclude", nargs="*", default=[], help="scenario ids left out (reported separately)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    for path in a.ablation:
        if os.path.exists(path):
            for sid, rec in json.load(open(path)).items():
                ABL.setdefault(sid, {}).update(rec)
    scen, runs = load(a.scenarios, a.results)
    rows = [row for run in runs if run["sid"] not in a.exclude for row in rows_for(scen, run)]
    if rows:
        with open(os.path.join(a.out, "runs.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    print(summary(rows, a.out))
    figures(rows, a.out)


if __name__ == "__main__":
    main()
