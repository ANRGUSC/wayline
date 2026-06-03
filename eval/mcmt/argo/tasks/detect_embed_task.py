#!/usr/bin/env python3
"""Argo wrapper for the detect+embed stage."""
import os
import sys
import tempfile

sys.path.insert(0, "/app")

from lib.detect_embed import detect_and_embed              # noqa: E402
from lib.payload import pack_dir_to_file, unpack_file_to_dir  # noqa: E402


def main() -> None:
    in_path = os.environ.get("VEMCMT_IN", "/in/preprocess/output")
    out_path = os.environ.get("VEMCMT_OUT", "/out/output")
    device = os.environ.get("VEMCMT_DEVICE", "GPU")
    det_model = os.environ.get("VEMCMT_DET_MODEL", "/models/yolov8n.xml")
    reid_model = os.environ.get("VEMCMT_REID_MODEL", "/models/osnet_x0_25.xml")

    with tempfile.TemporaryDirectory(prefix="vemcmt-det-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-det-out-") as out_dir:
        unpack_file_to_dir(in_path, in_dir)
        meta = detect_and_embed(in_dir, out_dir, det_model, reid_model, device=device)
        n = pack_dir_to_file(out_dir, out_path)
        print(
            f"[argo detect+embed] device={meta['device']} frames={meta['frames']} "
            f"detections={meta['detections']} wall={meta['wall_s']:.2f}s wrote={n}B",
            flush=True,
        )


if __name__ == "__main__":
    main()
