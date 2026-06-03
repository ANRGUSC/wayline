#!/usr/bin/env python3
"""Wayline wrapper for the detect+embed stage.

Models (YOLOv8n + OSNet-x0_25) live in /models inside the image, in
OpenVINO IR form. VEMCMT_DEVICE selects the OpenVINO device target:
"GPU" → Intel iGPU (Xe-LP on i3-N305 via /dev/dri); "CPU" → fallback.
"""
import os
import sys
import tempfile

sys.path.insert(0, "/app")

from wl import WlTask                              # noqa: E402
from lib.detect_embed import detect_and_embed            # noqa: E402
from lib.payload import pack_dir, unpack_to_dir          # noqa: E402


def main() -> None:
    task = WlTask()
    device = os.environ.get("VEMCMT_DEVICE", "GPU")
    det_model = os.environ.get("VEMCMT_DET_MODEL", "/models/yolov8n.xml")
    reid_model = os.environ.get("VEMCMT_REID_MODEL", "/models/osnet_x0_25.xml")

    blob = task.recv_raw()
    with tempfile.TemporaryDirectory(prefix="vemcmt-det-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-det-out-") as out_dir:
        unpack_to_dir(blob, in_dir)
        meta = detect_and_embed(
            in_dir, out_dir,
            det_model=det_model, reid_model=reid_model, device=device,
        )
        print(
            f"[{task.name}] detect+embed device={meta['device']} "
            f"frames={meta['frames']} detections={meta['detections']} "
            f"wall={meta['wall_s']:.2f}s",
            flush=True,
        )
        out_blob = pack_dir(out_dir)

    print(f"[{task.name}] sending {len(out_blob)} bytes", flush=True)
    task.send_raw(out_blob)
    task.close()


if __name__ == "__main__":
    main()
