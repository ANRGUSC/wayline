#!/usr/bin/env python3
"""Plots for the fair (equal-CPU, matched-placement, perf-governor) MCMT results.
  fig:aicity-fair  -- no-tc vs fixed-tc, 4 cells, Wayline vs Argo makespan (2 panels)
  fig:aicity-random -- random-network speedup distribution, 10 seeds, d30/d120-png dots
Reads fair-results.csv + random-results.csv; writes PDFs to OUT (default /tmp)."""
import csv, statistics, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT = os.environ.get("OUT", "/tmp")
FAIR = "/home/anrg/wayline/eval/mcmt/results/fair-2x2/fair-results.csv"
RAND = "/home/anrg/wayline/eval/synthetic-dags/scheduler/results/random-sweep/random-results.csv"
WL, AR = "#2c7fb8", "#de2d26"   # Wayline blue, Argo red
CELLS = ["d30-jpg", "d60-jpg", "d120-jpg", "d120-png"]

def mean_ms(rows, net, cell, sysn):
    v = [float(r["makespan_s"]) for r in rows if r["net"]==net and r["cell"]==cell
         and r["system"]==sysn and r["phase"]=="Succeeded" and r["makespan_s"] not in ("","?")]
    return statistics.mean(v) if v else None

# ---------- Figure 1: no-tc vs fixed-tc grouped bars ----------
fr = list(csv.DictReader(open(FAIR)))
fig, axes = plt.subplots(1, 2, figsize=(8.0, 3.1), sharey=True)
import numpy as np
x = np.arange(len(CELLS)); w = 0.38
for ax, net, title in [(axes[0],"notc","Unshaped 1\\,GbE (no tc)"), (axes[1],"tc","Fixed edge tc matrix")]:
    wl = [mean_ms(fr,net,c,"wayline") for c in CELLS]
    ar = [mean_ms(fr,net,c,"argo") for c in CELLS]
    ax.bar(x-w/2, wl, w, label="Wayline", color=WL)
    ax.bar(x+w/2, ar, w, label="Argo+MinIO", color=AR)
    for i,(a,b) in enumerate(zip(wl,ar)):
        ax.text(i, max(a,b)+4, f"{b/a:.2f}x", ha="center", fontsize=8)
    ax.set_xticks(x); ax.set_xticklabels(CELLS, rotation=20, fontsize=8)
    ax.set_title(title.replace("\\,"," "), fontsize=9)
axes[0].set_ylabel("makespan (s)"); axes[0].legend(fontsize=8, loc="upper left")
fig.tight_layout(); fig.savefig(f"{OUT}/aicity-fair.pdf"); fig.savefig(f"{OUT}/aicity-fair.png", dpi=140)
print(f"wrote {OUT}/aicity-fair.pdf")

# ---------- Figure 2: random-network speedup dots ----------
rr = [r for r in csv.DictReader(open(RAND)) if r["phase"]=="Succeeded" and r["makespan_s"] not in ("","?")]
def speedups(cell):
    out=[]
    for sd in sorted(set(r["seed"] for r in rr), key=int):
        w=[float(r["makespan_s"]) for r in rr if r["seed"]==sd and r["cell"]==cell and r["system"]=="wayline"]
        a=[float(r["makespan_s"]) for r in rr if r["seed"]==sd and r["cell"]==cell and r["system"]=="argo"]
        if w and a: out.append(statistics.mean(a)/statistics.mean(w))
    return out
fig2, ax = plt.subplots(figsize=(4.6, 3.3))
rng = np.random.default_rng(0)
for i,(cell,col) in enumerate([("d30-jpg","#1b9e77"),("d120-png","#7570b3")]):
    sp = speedups(cell)
    xs = i + (rng.random(len(sp))-0.5)*0.18
    ax.scatter(xs, sp, s=42, color=col, alpha=0.85, edgecolor="k", linewidth=0.4, zorder=3)
    med = statistics.median(sp)
    ax.hlines(med, i-0.22, i+0.22, color="k", linewidth=2, zorder=4)
    ax.text(i, max(sp)+0.08, f"median {med:.2f}x\n{sum(1 for v in sp if v>1)}/{len(sp)} wins", ha="center", fontsize=8)
ax.axhline(1.0, color="grey", ls="--", lw=1)
ax.set_xticks([0,1]); ax.set_xticklabels(["d30-jpg","d120-png"])
ax.set_ylabel("speedup  (Argo / Wayline makespan)"); ax.set_ylim(0.9, 3.6)
ax.set_title("10 randomized edge networks", fontsize=9)
fig2.tight_layout(); fig2.savefig(f"{OUT}/aicity-random.pdf"); fig2.savefig(f"{OUT}/aicity-random.png", dpi=140)
print(f"wrote {OUT}/aicity-random.pdf")
