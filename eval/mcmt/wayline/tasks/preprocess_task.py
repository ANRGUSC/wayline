#!/usr/bin/env python3
"""Wayline wrapper for the preprocess stage."""
import os
import sys
import tempfile

sys.path.insert(0, "/app")

from wl import WlTask                          # noqa: E402
from lib.payload import pack_dir, unpack_to_dir      # noqa: E402
from lib.preprocess import preprocess_frames         # noqa: E402


def main() -> None:
    task = WlTask()
    target = int(os.environ.get("VEMCMT_TARGET_SIZE", "640"))
    fmt = os.environ.get("VEMCMT_FMT", "png")           # png | jpg
    quality = int(os.environ.get("VEMCMT_JPEG_QUALITY", "88"))

    blob = task.recv_raw()
    with tempfile.TemporaryDirectory(prefix="vemcmt-pre-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-pre-out-") as out_dir:
        unpack_to_dir(blob, in_dir)
        meta = preprocess_frames(in_dir, out_dir, target_size=(target, target),
                                 quality=quality, fmt=fmt)
        print(
            f"[{task.name}] preprocess frames={meta['frames']} "
            f"target={meta['target']} wall={meta['wall_s']:.2f}s",
            flush=True,
        )
        out_blob = pack_dir(out_dir)

    print(f"[{task.name}] sending {len(out_blob)} bytes", flush=True)
    task.send_raw(out_blob)
    task.close()


if __name__ == "__main__":
    main()
