#!/usr/bin/env python3
"""
Block 6 — analyze the poll-agents.sh output.

Reads overhead.csv and emits a per-agent table:

  - mean / peak CPU (millicores)
  - mean / peak memory (MB resident)
  - bytes_in/out delta over the captured window
  - peak push_inflight (a check on the bounded-fanout invariant)
  - put_total, put_ok, put_idempotent, put_conflict, put_checksum_mismatch
    delta over the window

  scripts/analyze.py [csv_path]   default: results/overhead.csv

Writes overhead-summary.md alongside the input CSV.
"""
from __future__ import annotations

import csv
import statistics
import sys
from pathlib import Path


def main() -> int:
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "results/overhead.csv")
    if not src.is_file():
        print(f"no such file: {src}", file=sys.stderr); return 1

    rows = list(csv.DictReader(src.open()))
    if not rows:
        print("empty CSV"); return 1

    # Bucket rows by agent_pod.
    by_pod: dict[str, list[dict]] = {}
    for r in rows:
        by_pod.setdefault(r["agent_pod"], []).append(r)

    summary_rows = []
    for pod, agent_rows in sorted(by_pod.items()):
        agent_rows.sort(key=lambda r: int(r["ts_s"]))
        node = agent_rows[0]["node"]
        cpus = [int(r["cpu_m"]) for r in agent_rows]
        mems = [int(r["mem_mb"]) for r in agent_rows]
        inflights = [int(r["push_inflight"]) for r in agent_rows]
        bi_start = int(agent_rows[0]["bytes_in"]);  bi_end = int(agent_rows[-1]["bytes_in"])
        bo_start = int(agent_rows[0]["bytes_out"]); bo_end = int(agent_rows[-1]["bytes_out"])
        pt_start = int(agent_rows[0]["put_total"]); pt_end = int(agent_rows[-1]["put_total"])
        pok_start = int(agent_rows[0]["put_ok"]);   pok_end = int(agent_rows[-1]["put_ok"])

        summary_rows.append({
            "node": node, "pod": pod,
            "cpu_mean_m": statistics.mean(cpus),
            "cpu_peak_m": max(cpus),
            "mem_mean_mb": statistics.mean(mems),
            "mem_peak_mb": max(mems),
            "bytes_in_delta_mb": (bi_end - bi_start) / 1e6,
            "bytes_out_delta_mb": (bo_end - bo_start) / 1e6,
            "put_total_delta": pt_end - pt_start,
            "put_ok_delta":    pok_end - pok_start,
            "peak_inflight":   max(inflights),
            "samples": len(agent_rows),
        })

    # ------------- print to stdout --------------------------------------
    print(f"{'node':<8} {'cpu_mean':>8} {'cpu_peak':>8} {'mem_mean':>8} {'mem_peak':>8} {'bytes_in':>10} {'bytes_out':>10} {'put':>5} {'inflt':>5}")
    print(f"{'':<8} {'(m)':>8} {'(m)':>8} {'(MB)':>8} {'(MB)':>8} {'(MB)':>10} {'(MB)':>10} {'':>5} {'peak':>5}")
    print("-" * 75)
    for s in summary_rows:
        print(f"{s['node']:<8} {s['cpu_mean_m']:>8.1f} {s['cpu_peak_m']:>8} "
              f"{s['mem_mean_mb']:>8.1f} {s['mem_peak_mb']:>8} "
              f"{s['bytes_in_delta_mb']:>10.1f} {s['bytes_out_delta_mb']:>10.1f} "
              f"{s['put_total_delta']:>5} {s['peak_inflight']:>5}")

    # Aggregate
    total_cpu_peak = sum(s["cpu_peak_m"] for s in summary_rows)
    total_mem_peak = sum(s["mem_peak_mb"] for s in summary_rows)
    total_bi = sum(s["bytes_in_delta_mb"] for s in summary_rows)
    total_bo = sum(s["bytes_out_delta_mb"] for s in summary_rows)
    print("-" * 75)
    print(f"{'TOTAL':<8} {'':>8} {total_cpu_peak:>8} {'':>8} {total_mem_peak:>8} "
          f"{total_bi:>10.1f} {total_bo:>10.1f}")
    print()
    print(f"Across {len(summary_rows)} agents, peak data-plane cost during the run:")
    print(f"  • CPU peak (sum across agents): {total_cpu_peak} m  ({total_cpu_peak/1000:.2f} cores)")
    print(f"  • RSS peak (sum across agents): {total_mem_peak} MB")
    print(f"  • Bytes moved: {total_bi:.0f} MB in, {total_bo:.0f} MB out across {len(summary_rows)} agents")
    print(f"  • Max push_inflight seen at any agent: {max(s['peak_inflight'] for s in summary_rows)}")
    print(f"  • Time window: {int(rows[-1]['ts_s']) - int(rows[0]['ts_s'])}s, {len(rows)} samples")

    # ------------- markdown report --------------------------------------
    md_path = src.with_suffix("").parent / (src.stem + "-summary.md")
    with md_path.open("w") as f:
        f.write(f"# Data-agent resource overhead — `{src.name}`\n\n")
        f.write(f"Time window {int(rows[-1]['ts_s']) - int(rows[0]['ts_s'])}s, {len(rows)} samples across {len(summary_rows)} agents.\n\n")
        f.write("| node | CPU mean | CPU peak | RSS mean | RSS peak | bytes in (MB) | bytes out (MB) | PUT count | peak inflight |\n")
        f.write("|------|---------:|---------:|---------:|---------:|--------------:|---------------:|----------:|--------------:|\n")
        for s in summary_rows:
            f.write(f"| {s['node']} | {s['cpu_mean_m']:.1f} m | {s['cpu_peak_m']} m | "
                    f"{s['mem_mean_mb']:.1f} MB | {s['mem_peak_mb']} MB | "
                    f"{s['bytes_in_delta_mb']:.1f} | {s['bytes_out_delta_mb']:.1f} | "
                    f"{s['put_total_delta']} | {s['peak_inflight']} |\n")
        f.write("\n")
        f.write(f"Aggregate peak across {len(summary_rows)} agents: "
                f"CPU {total_cpu_peak} m ({total_cpu_peak/1000:.2f} cores), "
                f"RSS {total_mem_peak} MB. Bounded-fanout invariant: max inflight at any agent "
                f"{max(s['peak_inflight'] for s in summary_rows)} (config max = 4).\n")
    print(f"\nwrote {md_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
