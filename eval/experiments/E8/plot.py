#!/usr/bin/env python3
"""E8 figures. Main: Part B (patch->convergence + copy-ops/s vs total copy
actions, groupings by marker) and Part D (controller CPU + drain throughput vs
concurrent runs). Appendix: Part C active-transfer resource curve.

  figures/e8-main.{png,pdf}   figures/e8-partC.{png,pdf}
"""
import json, glob, statistics as st
from collections import defaultdict
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RES = Path(__file__).parent / "results-campaign"
OUT = Path(__file__).parent / "figures"; OUT.mkdir(exist_ok=True)
med = lambda xs: st.median([x for x in xs if x is not None])

def load(part):
    d = defaultdict(list)
    for f in glob.glob(str(RES / f"{part}-b*.json")):
        for r in json.load(open(f)):
            key = (r["O"], r["C"]) if part == "B" else (r.get("A") or r.get("R"))
            d[key].append(r)
    return d

Bd, Cd, Dd = load("B"), load("C"), load("D")

# ---- MAIN FIGURE: Part B (left) + Part D (right) ----
fig, (axB, axD) = plt.subplots(1, 2, figsize=(7.0, 3.1))

# Part B: x = total copy actions; markers encode grouping
groups = [((1,1),"o"),((8,1),"o"),((32,1),"o"),((128,1),"o"),
          ((64,2),"s"),((32,4),"^"),((16,7),"D")]
labelled = set()
xs_conv=[]; ys_conv=[]
for (O,C),mk in groups:
    rs=Bd[(O,C)]; tot=O*C
    conv=med([r["patch_to_conv_s"] for r in rs])
    ops =med([r["copy_ops_per_s"] for r in rs])
    lab = f"C={C}" if C not in labelled else None; labelled.add(C)
    axB.plot(tot, conv, mk, color="#1f4e79", ms=7, label=lab)
    xs_conv.append(tot); ys_conv.append(conv)
axB.set_xscale("log", base=2); axB.set_xlabel("total desired copy actions (O·C)")
axB.set_ylabel("patch→convergence (s)", color="#1f4e79")
axB.tick_params(axis="y", labelcolor="#1f4e79")
axB.set_title("(a) Part B — copy reconciliation", fontsize=10)
axB.grid(True, alpha=0.25, which="both")
axB.legend(title="marker=grouping", fontsize=7, title_fontsize=7, loc="upper left")
axB2 = axB.twinx()
for (O,C),mk in groups:
    rs=Bd[(O,C)]; tot=O*C
    axB2.plot(tot, med([r["copy_ops_per_s"] for r in rs]), mk, color="#c55a11",
              ms=6, mfc="none")
axB2.set_ylabel("copy actions / s", color="#c55a11")
axB2.tick_params(axis="y", labelcolor="#c55a11")

# Part D: x = R; controller CPU (p95) + drain throughput
Rs=[1,4,8,16,32,64]
cpu=[med([r["ctrl_cpu_mc_p95"] for r in Dd[R]]) for R in Rs]
thr=[med([r["runs_per_min"] for r in Dd[R]]) for R in Rs]
axD.plot(Rs, cpu, "o-", color="#1f4e79", label="controller CPU p95")
axD.set_xscale("log", base=2); axD.set_xlabel("concurrent runs R")
axD.set_ylabel("controller CPU p95 (mc)", color="#1f4e79")
axD.tick_params(axis="y", labelcolor="#1f4e79")
axD.set_xticks(Rs); axD.set_xticklabels(Rs)
axD.set_title("(b) Part D — concurrent runs", fontsize=10)
axD.grid(True, alpha=0.25)
axD.axhline(1000, ls=":", color="gray", lw=0.8)
axD.text(1.1, 1020, "1 core", fontsize=6, color="gray")
axD2=axD.twinx()
axD2.plot(Rs, thr, "s--", color="#c55a11", mfc="none", label="drain throughput")
axD2.set_ylabel("throughput (runs/min)", color="#c55a11")
axD2.tick_params(axis="y", labelcolor="#c55a11")
axD2.set_ylim(0, 90)
fig.tight_layout()
for ext in ("png","pdf"): fig.savefig(OUT/f"e8-main.{ext}", dpi=150, bbox_inches="tight")

# ---- APPENDIX: Part C ----
figC, axC = plt.subplots(1, 2, figsize=(7.0, 3.0))
As=[1,4,8,16,32]
gp=[med([r["agg_goodput_mbps"] for r in Cd[A]]) for A in As]
axC[0].plot(As, gp, "o-", color="#2e7d32")
axC[0].plot(As, [a*59 for a in As], ":", color="gray", lw=0.9, label="A×59 Mbit ceiling")
axC[0].set_xlabel("concurrent transfers A"); axC[0].set_ylabel("aggregate goodput (Mbps)")
axC[0].set_title("(a) Part C — goodput", fontsize=10); axC[0].grid(True, alpha=0.25)
axC[0].legend(fontsize=7)
ccpu=[med([r["ctrl_cpu_core_s"] for r in Cd[A]]) for A in As]
acpu=[med([r["agent_cpu_core_s"] for r in Cd[A]]) for A in As]
axC[1].plot(As, acpu, "s-", color="#c55a11", label="agent CPU (aggregate)")
axC[1].plot(As, ccpu, "o-", color="#1f4e79", label="controller CPU")
axC[1].set_xlabel("concurrent transfers A"); axC[1].set_ylabel("CPU (core-seconds)")
axC[1].set_title("(b) Part C — CPU cost", fontsize=10); axC[1].grid(True, alpha=0.25)
axC[1].legend(fontsize=7)
figC.tight_layout()
for ext in ("png","pdf"): figC.savefig(OUT/f"e8-partC.{ext}", dpi=150, bbox_inches="tight")
print("wrote", *(str(p) for p in OUT.glob("e8-*")))
