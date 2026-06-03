#!/usr/bin/env python3
"""Argo wrapper for the within-camera tracking stage."""
import os
import sys
import tempfile

sys.path.insert(0, "/app")

from lib.payload import pack_dir_to_file, unpack_file_to_dir  # noqa: E402
from lib.track import track_within_camera                     # noqa: E402


def main() -> None:
    in_path = os.environ.get("VEMCMT_IN", "/in/detect_embed/output")
    out_path = os.environ.get("VEMCMT_OUT", "/out/output")

    with tempfile.TemporaryDirectory(prefix="vemcmt-trk-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-trk-out-") as out_dir:
        unpack_file_to_dir(in_path, in_dir)
        meta = track_within_camera(in_dir, out_dir)
        n = pack_dir_to_file(out_dir, out_path)
        print(
            f"[argo track] frames={meta['frames']} tracklets={meta['tracklets']} "
            f"wall={meta['wall_s']:.2f}s wrote={n}B",
            flush=True,
        )


if __name__ == "__main__":
    main()
