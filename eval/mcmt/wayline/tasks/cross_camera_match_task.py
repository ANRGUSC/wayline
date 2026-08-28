#!/usr/bin/env python3
"""Wayline wrapper for the cross-camera fan-in stage.

Receives one tar.gz blob per camera, one declared named input per
upstream track task. Unpacks each into a subdir named after the camera,
then calls lib.match.cross_camera_match which scans the parent for
camera subdirs and produces global_tracks.json.
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/app")

from wl import WlTask                          # noqa: E402
from lib.match import cross_camera_match             # noqa: E402
from lib.wlobj import recv_named, send_named, producer_of  # noqa: E402
from lib.payload import pack_dir, unpack_to_dir      # noqa: E402


def _camera_name_for(dep_name: str) -> str:
    """Map an upstream task name (e.g. 'track-1') to a camera label
    ('cam-1'). The trailing token is the camera index; we keep the same
    label as the dataset uses."""
    suffix = dep_name.rsplit("-", 1)[-1]
    return f"cam-{suffix}"


def main() -> None:
    task = WlTask()
    sim_thresh = float(os.environ.get("VEMCMT_SIM_THRESH", "0.55"))

    inputs = recv_named(task)  # [(canonical_key, bytes)] in declared order
    if not inputs:
        raise RuntimeError("cross_camera_match: no declared inputs")

    with tempfile.TemporaryDirectory(prefix="vemcmt-match-in-") as in_dir, \
         tempfile.TemporaryDirectory(prefix="vemcmt-match-out-") as out_dir:
        for key, blob in inputs:
            # Map from the canonical object key, not the peer: under store
            # lowering the bytes arrive from a vertex whose name carries
            # no camera index.
            cam_dir = Path(in_dir) / _camera_name_for(producer_of(key))
            unpack_to_dir(blob, cam_dir)

        meta = cross_camera_match(in_dir, out_dir, sim_thresh=sim_thresh)
        print(
            f"[{task.name}] match cameras={meta['cameras']} "
            f"input_tracklets={meta['input_tracklets']} "
            f"global_tracks={meta['global_tracks']} wall={meta['wall_s']:.2f}s",
            flush=True,
        )
        out_blob = pack_dir(out_dir)

    print(f"[{task.name}] sending {len(out_blob)} bytes", flush=True)
    send_named(task, out_blob)
    task.close()


if __name__ == "__main__":
    main()
