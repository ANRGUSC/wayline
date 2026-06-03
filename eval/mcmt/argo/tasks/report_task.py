#!/usr/bin/env python3
"""Argo wrapper for the report stage.

Reads global_tracks tarball from upstream artifact, writes report.json
to a hostPath at /reports/<workflow>/report.json so the harvest script
can collect it.
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/app")

from lib.payload import unpack_file_to_dir   # noqa: E402
from lib.report import generate_report       # noqa: E402


def main() -> None:
    in_path = os.environ.get("VEMCMT_IN", "/in/cross_camera_match/output")
    report_root = Path(os.environ.get("VEMCMT_REPORT_ROOT", "/reports"))
    workflow = os.environ.get("ARGO_WORKFLOW_NAME", "unknown")
    out_dir = report_root / workflow
    out_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="vemcmt-rpt-in-") as in_dir:
        unpack_file_to_dir(in_path, in_dir)
        meta = generate_report(in_dir, str(out_dir))
        print(
            f"[argo report] tracks={meta['tracks']} classes={meta['unique_classes']} "
            f"wrote {out_dir}/report.json",
            flush=True,
        )


if __name__ == "__main__":
    main()
