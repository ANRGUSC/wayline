#!/usr/bin/env python3
"""Argo wrapper for the decode stage.

I/O contract is files: clip is on a hostPath mount at VEMCMT_CLIP_PATH;
output tarball is written to /out/output for Argo's artifact uploader.
"""
import os
import sys
import tempfile

sys.path.insert(0, "/app")

from lib.decode import decode_clip      # noqa: E402
from lib.payload import pack_dir_to_file  # noqa: E402


def main() -> None:
    camera = os.environ.get("VEMCMT_CAMERA", "?")
    clip = os.environ["VEMCMT_CLIP_PATH"]
    fps = int(os.environ.get("VEMCMT_FPS", "5"))
    out_path = os.environ.get("VEMCMT_OUT", "/out/output")

    with tempfile.TemporaryDirectory(prefix="vemcmt-decode-") as work:
        meta = decode_clip(clip, work, fps=fps)
        n = pack_dir_to_file(work, out_path)
        print(
            f"[argo decode] camera={camera} decoder={meta['decoder']} "
            f"frames={meta['frames']} wall={meta['wall_s']:.2f}s wrote={n}B",
            flush=True,
        )


if __name__ == "__main__":
    main()
