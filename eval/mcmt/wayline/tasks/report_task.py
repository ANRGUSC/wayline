#!/usr/bin/env python3
"""Wayline wrapper for the terminal report stage.

Reads global_tracks.json from upstream, writes report.json to a local
volume so the harvest path can read it. No send needed — leaf task.
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/app")

from wl import WlTask                          # noqa: E402
from lib.payload import unpack_to_dir                # noqa: E402
from lib.report import generate_report               # noqa: E402


def main() -> None:
    task = WlTask()
    blob = task.recv_raw()

    # Final report lives at /reports/<odag>/report.json on the report-tier
    # node so the harvest script can collect it after the run.
    odag = os.environ.get("WL_ODAG_NAME", "unknown")
    report_root = Path(os.environ.get("VEMCMT_REPORT_ROOT", "/reports"))
    out_dir = report_root / odag
    out_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="vemcmt-rpt-in-") as in_dir:
        unpack_to_dir(blob, in_dir)
        meta = generate_report(in_dir, str(out_dir))
        print(
            f"[{task.name}] report tracks={meta['tracks']} classes={meta['unique_classes']} "
            f"wrote {out_dir}/report.json",
            flush=True,
        )
    task.close()


if __name__ == "__main__":
    main()
