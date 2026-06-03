#!/usr/bin/env python3
"""
E0 unified-CSV harvester.

Reads per-run JSON files written by wayline/run.sh and minio/run.sh and
emits a single CSV with columns:

  system, colocation, payload_label, bytes, run_name,
  producer_pod, consumer_pod, producer_node, consumer_node,
  t0, t1, t1p, t2, t3, t4,
  e2e, compute, send_or_upload, producer_hold, consumer_wait, transfer_visible

Where:
  e2e               = t4 - t0
  compute           = t1 - t0
  send_or_upload    = (t1p - t1) for MinIO; not applicable for Wayline.
  producer_hold     = t2 - t1
  consumer_wait     = t3 - t2  (gap between producer-freed and consumer-can-start)
  transfer_visible  = t4 - t3  (consumer's blocking recv / get duration)

Pod-API timestamps (RFC3339 second-resolution) are used for t2 and (if
the consumer's in-pod t3 is missing) t3. In-pod wall clocks are used
otherwise for sub-second precision.

Usage:
  python harvest.py results/                  # writes results/all.csv
  python harvest.py results/ --out file.csv
"""

import argparse
import csv
import json
import pathlib
import sys
from datetime import datetime, timezone


def parse_rfc3339(s):
    if not s:
        return None
    # Python 3.11 handles 'Z' via fromisoformat.
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def harvest_one(path: pathlib.Path, system: str, colocation: str,
                payload_label: str, bytes_: int):
    try:
        rec = json.loads(path.read_text())
    except Exception as e:
        print(f"[skip] {path}: bad JSON ({e})", file=sys.stderr)
        return None

    p = rec.get("producer_log") or {}
    c = rec.get("consumer_log") or {}
    api = rec.get("pod_api") or {}

    t0 = p.get("t0_wall")
    t1 = p.get("t1_wall")
    t1p = p.get("t1p_wall")             # MinIO only
    t3 = c.get("t3_wall")
    t_found = c.get("t_found_wall")     # MinIO only — when HEAD first succeeded
    t4 = c.get("t4_wall")

    # Pod-API timestamps (RFC3339, second-resolution). t2 is the
    # canonical "producer pod terminated" event; the in-pod clock has no
    # equivalent. For t3 we prefer the in-pod wall (sub-second) and fall
    # back to the API.
    t2_api = parse_rfc3339(api.get("producer_finished"))
    t3_api = parse_rfc3339(api.get("consumer_started"))

    t2 = t2_api
    if t3 is None:
        t3 = t3_api

    def delta(a, b):
        if a is None or b is None:
            return None
        return float(b) - float(a)

    e2e               = delta(t0, t4)
    compute           = delta(t0, t1)
    send_or_upload    = delta(t1, t1p) if t1p is not None else None
    # producer_hold has a 1-second floor from K8s API second-resolution;
    # clamp small negatives to 0 so the decomposition doesn't show
    # impossible negative bars.
    ph = delta(t1, t2)
    producer_hold     = max(0.0, ph) if ph is not None else None
    consumer_wait     = delta(t2, t3)
    poll_wait         = delta(t3, t_found) if t_found is not None else None
    # download_time captures the actual on-wire GET (MinIO) — distinct
    # from the poll-wait the consumer paid before the object appeared.
    download_time     = delta(t_found, t4) if t_found is not None else None
    # transfer_visible is the legacy metric (t4 - t3); kept for Wayline where
    # there is no HEAD-poll loop and the recv_raw call blocks atomically.
    transfer_visible  = delta(t3, t4)

    return {
        "system": system,
        "colocation": colocation,
        "payload_label": payload_label,
        "bytes": bytes_,
        "run_name": rec.get("run_name", path.stem),
        "producer_pod": rec.get("producer_pod"),
        "consumer_pod": rec.get("consumer_pod"),
        "producer_node": p.get("node"),
        "consumer_node": c.get("node"),
        "t0": t0,
        "t1": t1,
        "t1p": t1p,
        "t2": t2,
        "t3": t3,
        "t_found": t_found,
        "t4": t4,
        "e2e": e2e,
        "compute": compute,
        "send_or_upload": send_or_upload,
        "producer_hold": producer_hold,
        "consumer_wait": consumer_wait,
        "poll_wait": poll_wait,
        "download_time": download_time,
        "transfer_visible": transfer_visible,
    }


def parse_cell_tag(tag: str):
    # E.g. "same-100mb" -> ("same", "100MB", 104857600)
    coloc, _, pay = tag.partition("-")
    label = pay.upper()
    # Convert label to bytes.
    mult = {"KB": 1024, "MB": 1024 * 1024, "GB": 1024 * 1024 * 1024}
    n = int("".join(ch for ch in label if ch.isdigit()))
    unit = "".join(ch for ch in label if ch.isalpha())
    return coloc, label, n * mult.get(unit, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results_dir", type=pathlib.Path)
    ap.add_argument("--out", type=pathlib.Path, default=None)
    args = ap.parse_args()

    out = args.out or (args.results_dir / "all.csv")

    rows = []
    for system_dir in sorted(args.results_dir.iterdir()):
        if not system_dir.is_dir() or system_dir.name not in ("wayline", "wayline", "minio", "nfs"):
            continue
        for cell_dir in sorted(system_dir.iterdir()):
            if not cell_dir.is_dir():
                continue
            try:
                coloc, label, bytes_ = parse_cell_tag(cell_dir.name)
            except Exception:
                print(f"[warn] cannot parse cell tag from {cell_dir.name}", file=sys.stderr)
                continue
            for run_json in sorted(cell_dir.glob("*.json")):
                row = harvest_one(run_json, system_dir.name, coloc, label, bytes_)
                if row is not None:
                    rows.append(row)

    if not rows:
        print(f"[harvest] no runs found under {args.results_dir}", file=sys.stderr)
        sys.exit(1)

    cols = list(rows[0].keys())
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[harvest] wrote {len(rows)} rows to {out}")


if __name__ == "__main__":
    main()
