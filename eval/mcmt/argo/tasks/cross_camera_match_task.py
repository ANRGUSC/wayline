#!/usr/bin/env python3
"""Argo wrapper for the cross-camera fan-in stage.

Argo passes one named artifact per camera. We expect them mounted at
/in/track-1/output, /in/track-2/output, etc. The wrapper unpacks each
into a subdir named cam-N (matching the lib.match convention).
"""
import os
import re
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/app")

from lib.match import cross_camera_match                       # noqa: E402
from lib.payload import pack_dir_to_file, unpack_file_to_dir   # noqa: E402

_TRACK_DIR_RE = re.compile(r"^track-(\d+)$")


def main() -> None:
    in_root = Path(os.environ.get("VEMCMT_IN_ROOT", "/in"))
    out_path = os.environ.get("VEMCMT_OUT", "/out/output")
    sim_thresh = float(os.environ.get("VEMCMT_SIM_THRESH", "0.55"))

    with tempfile.TemporaryDirectory(prefix="vemcmt-match-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-match-out-") as out_dir:
        n_cams = 0
        for sub in sorted(in_root.iterdir()):
            m = _TRACK_DIR_RE.match(sub.name)
            if not m:
                continue
            blob_path = sub / "output"
            if not blob_path.is_file():
                continue
            cam_dir = Path(in_dir) / f"cam-{m.group(1)}"
            unpack_file_to_dir(blob_path, cam_dir)
            n_cams += 1
        if n_cams == 0:
            raise RuntimeError(f"argo cross_camera_match: no track-N subdirs under {in_root}")

        meta = cross_camera_match(in_dir, out_dir, sim_thresh=sim_thresh)
        n = pack_dir_to_file(out_dir, out_path)
        print(
            f"[argo match] cameras={meta['cameras']} input_tracklets={meta['input_tracklets']} "
            f"global_tracks={meta['global_tracks']} wall={meta['wall_s']:.2f}s wrote={n}B",
            flush=True,
        )


if __name__ == "__main__":
    main()
