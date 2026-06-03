#!/usr/bin/env python3
"""
Wayline wrapper for the decode stage.

Reads the clip from a hostPath mount (/dataset/<camera>/clip.mp4) staged
by dataset/stage-on-nodes.sh. Calls lib.decode.decode_clip, packs the
frame directory, sends the tar.gz blob to all successors.

Environment:
    WL_TASK_NAME       — e.g. "decode-1"
    VEMCMT_CAMERA       — camera label (also the clip key); typically "cam-1"
    VEMCMT_CLIP_PATH    — absolute path to the input mp4 inside the pod
    VEMCMT_FPS          — sampling fps; defaults to 5
"""
import os
import sys
import tempfile

# /app/lib is mounted via the task image; add to path for lib.* imports.
sys.path.insert(0, "/app")

from wl import WlTask                     # noqa: E402
from lib.decode import decode_clip              # noqa: E402
from lib.payload import pack_dir                # noqa: E402


def main() -> None:
    task = WlTask()
    camera = os.environ.get("VEMCMT_CAMERA", task.name)
    clip_path = os.environ["VEMCMT_CLIP_PATH"]
    fps = int(os.environ.get("VEMCMT_FPS", "5"))

    with tempfile.TemporaryDirectory(prefix="vemcmt-decode-") as work:
        meta = decode_clip(clip_path, work, fps=fps)
        print(
            f"[{task.name}] decode camera={camera} clip={clip_path} "
            f"decoder={meta['decoder']} frames={meta['frames']} wall={meta['wall_s']:.2f}s",
            flush=True,
        )
        blob = pack_dir(work)

    print(f"[{task.name}] sending {len(blob)} bytes of frame tarball", flush=True)
    task.send_raw(blob)
    task.close()


if __name__ == "__main__":
    main()
