#!/usr/bin/env python3
"""Argo wrapper for the preprocess stage."""
import os
import sys
import tempfile

sys.path.insert(0, "/app")

from lib.payload import pack_dir_to_file, unpack_file_to_dir  # noqa: E402
from lib.preprocess import preprocess_frames                  # noqa: E402


def main() -> None:
    in_path = os.environ.get("VEMCMT_IN", "/in/decode/output")
    out_path = os.environ.get("VEMCMT_OUT", "/out/output")
    target = int(os.environ.get("VEMCMT_TARGET_SIZE", "640"))
    fmt = os.environ.get("VEMCMT_FMT", "png")
    quality = int(os.environ.get("VEMCMT_JPEG_QUALITY", "88"))

    with tempfile.TemporaryDirectory(prefix="vemcmt-pre-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-pre-out-") as out_dir:
        unpack_file_to_dir(in_path, in_dir)
        meta = preprocess_frames(in_dir, out_dir, target_size=(target, target),
                                 quality=quality, fmt=fmt)
        n = pack_dir_to_file(out_dir, out_path)
        print(
            f"[argo preprocess] frames={meta['frames']} target={meta['target']} "
            f"wall={meta['wall_s']:.2f}s wrote={n}B",
            flush=True,
        )


if __name__ == "__main__":
    main()
