#!/usr/bin/env python3
"""
Build the paper figure for the mcmt curve.

Reads each cell's results/<cell>-pilot/summary.csv and emits:
  - curve.csv  — per-cell aggregated stats (means/stds, paired deltas)
  - curve.pdf  — figure: x=intermediate bytes moved (Argo's MinIO transfer
                 proxy = Wayline bytes_out_total on the data plane), y=makespan,
                 both Wayline and Argo with error bars, paired-rep dots overlaid.

Argo doesn't expose a "bytes moved" counter (it goes through MinIO via the
artifact controller), so the x-axis uses Wayline's measured bytes_out_total as
a stand-in for the per-cell payload scale. That's a known interpretation —
the figure tells a per-cell story, not a continuous-x regression.
"""
from __future__ import annotations
import argparse, csv, glob, json, statistics
from pathlib import Path


def parse_bytes_field(v: str) -> int | None:
    if v in ("", "NA", "?"):
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _cell_sort_key(cell_dir: Path) -> tuple:
    """Sort cells by camera count, then by clip duration, then by fmt (jpg
    before png) so the curve reads D=30, D=60, D=120-jpg, D=120-png."""
    name = cell_dir.name.replace("-pilot", "")
    # name like n4-d30-jpg or n4-d120-png
    parts = name.split("-")
    n = int(parts[0][1:]) if parts[0].startswith("n") else 0
    d = int(parts[1][1:]) if len(parts) > 1 and parts[1].startswith("d") else 0
    fmt = parts[2] if len(parts) > 2 else ""
    fmt_order = 0 if fmt == "jpg" else 1
    return (n, d, fmt_order)


def aggregate(results_dir: Path) -> list[dict]:
    rows_out = []
    for cell_dir in sorted(results_dir.glob("n*-d*-*-pilot"), key=_cell_sort_key):
        summary = cell_dir / "summary.csv"
        if not summary.is_file():
            continue
        cell = cell_dir.name.replace("-pilot", "")
        rs = list(csv.DictReader(summary.open()))
        wl_succ  = [r for r in rs if r['system']=='wayline'  and r['phase']=='Succeeded']
        argo_succ = [r for r in rs if r['system']=='argo' and r['phase']=='Succeeded']
        wl_ms  = [int(r['makespan_s']) for r in wl_succ  if r['makespan_s'] not in ('','?')]
        argo_ms = [int(r['makespan_s']) for r in argo_succ if r['makespan_s'] not in ('','?')]
        if not (wl_ms and argo_ms):
            continue
        # bytes_out_total is cumulative on the data-agents (lifetime counter).
        # Per-rep delta gives bytes pushed during that rep.
        bo_series = [parse_bytes_field(r['bytes_out_total']) for r in wl_succ]
        bo_per_rep = []
        prev = 0
        for v in bo_series:
            if v is None: continue
            bo_per_rep.append(v - prev); prev = v
        bo_per_rep_pos = [b for b in bo_per_rep if b > 0]

        n = min(len(wl_ms), len(argo_ms))
        deltas = [argo_ms[i] - wl_ms[i] for i in range(n)]
        rows_out.append({
            'cell':         cell,
            'n_reps':       n,
            'wl_mean':     round(statistics.mean(wl_ms), 1),
            'wl_std':      round(statistics.pstdev(wl_ms), 1) if len(wl_ms) > 1 else 0.0,
            'argo_mean':    round(statistics.mean(argo_ms), 1),
            'argo_std':     round(statistics.pstdev(argo_ms), 1) if len(argo_ms) > 1 else 0.0,
            'mean_delta_s': round(statistics.mean(deltas), 1),
            'speedup_pct':  round(statistics.mean(deltas) / statistics.mean(argo_ms) * 100, 1),
            'wl_bytes_per_rep_mean': int(statistics.mean(bo_per_rep_pos)) if bo_per_rep_pos else None,
            'wl_ms_series':  wl_ms,
            'argo_ms_series': argo_ms,
        })
    return rows_out


def write_csv(rows: list[dict], out_path: Path) -> None:
    if not rows:
        print(f"no cells found"); return
    cols = ['cell','n_reps','wl_mean','wl_std','argo_mean','argo_std',
            'mean_delta_s','speedup_pct','wl_bytes_per_rep_mean']
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {out_path}")


def write_figure(rows: list[dict], out_path: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not available — skipping figure"); return
    cells = [r['cell'] for r in rows]
    x = list(range(len(cells)))
    fig, ax = plt.subplots(figsize=(6.5, 3.2))
    ax.errorbar(x, [r['wl_mean']  for r in rows],
                yerr=[r['wl_std']  for r in rows],
                marker='o', label='Wayline', capsize=4, linewidth=2)
    ax.errorbar(x, [r['argo_mean'] for r in rows],
                yerr=[r['argo_std'] for r in rows],
                marker='s', label='Argo Workflows', capsize=4, linewidth=2)
    for i, r in enumerate(rows):
        for ms in r['wl_ms_series']:
            ax.plot(i, ms, 'o', color='C0', alpha=0.25, markersize=4)
        for ms in r['argo_ms_series']:
            ax.plot(i, ms, 's', color='C1', alpha=0.25, markersize=4)
    ax.set_xticks(x); ax.set_xticklabels(cells, rotation=15, ha='right')
    ax.set_ylabel("Makespan (s)")
    ax.set_xlabel("Cell (n cameras × clip duration × intermediate format)")
    ax.legend(loc='best', frameon=False)
    ax.grid(True, axis='y', alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path); fig.savefig(out_path.with_suffix(".png"), dpi=160)
    print(f"wrote {out_path} (+ .png)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="eval/mcmt/results")
    ap.add_argument("--csv-out",      default="eval/mcmt/results/curve.csv")
    ap.add_argument("--fig-out",      default="eval/mcmt/results/curve.pdf")
    args = ap.parse_args()
    rows = aggregate(Path(args.results_dir))
    print(json.dumps([{k:v for k,v in r.items() if k not in ('wl_ms_series','argo_ms_series')} for r in rows], indent=2))
    write_csv(rows, Path(args.csv_out))
    write_figure(rows, Path(args.fig_out))


if __name__ == "__main__":
    main()
