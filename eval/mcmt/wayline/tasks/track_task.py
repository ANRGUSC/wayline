#!/usr/bin/env python3
"""Wayline wrapper for the within-camera tracking stage."""
import sys
import tempfile

sys.path.insert(0, "/app")

from wl import WlTask                          # noqa: E402
from lib.payload import pack_dir, unpack_to_dir      # noqa: E402
from lib.track import track_within_camera            # noqa: E402


def main() -> None:
    task = WlTask()
    blob = task.recv_raw()

    with tempfile.TemporaryDirectory(prefix="vemcmt-trk-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-trk-out-") as out_dir:
        unpack_to_dir(blob, in_dir)
        meta = track_within_camera(in_dir, out_dir)
        print(
            f"[{task.name}] track frames={meta['frames']} "
            f"tracklets={meta['tracklets']} wall={meta['wall_s']:.2f}s",
            flush=True,
        )
        out_blob = pack_dir(out_dir)

    print(f"[{task.name}] sending {len(out_blob)} bytes", flush=True)
    task.send_raw(out_blob)
    task.close()


if __name__ == "__main__":
    main()
