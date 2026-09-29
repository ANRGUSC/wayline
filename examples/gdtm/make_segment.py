#!/usr/bin/env python3
"""Cut a segment of GDTM's sample test split (single view 3, good lighting)
into small HDF5 files with the dataset's own layout, keeping only what the
early-fusion model reads: ZED left camera, RealSense depth, mmWave
range-Doppler and the ReSpeaker waveform per node, plus OptiTrack ground truth.

The sample data (dataset_singleview3.zip, 6.9 GB) is linked from
github.com/nesl/GDTM-tracking. Usage:

  make_segment.py <mcp-sample-dataset/test> <out-dir> [--start 600] [--frames 300]
"""
import argparse
import os

import h5py

FILES = {  # file -> datasets the model uses
    "zed.hdf5": ["zed_camera_left"],
    "realsense.hdf5": ["realsense_camera_depth"],
    "mmwave.hdf5": ["range_doppler"],
    "respeaker.hdf5": ["mic_waveform"],
}
NODES = ["node_1", "node_2", "node_3"]


def cut(src, dst, start, frames):
    with h5py.File(f"{src}/mocap.hdf5") as h:
        keys = sorted(h.keys(), key=int)[start:start + frames]
    os.makedirs(dst, exist_ok=True)
    with h5py.File(f"{src}/mocap.hdf5") as h, h5py.File(f"{dst}/mocap.hdf5", "w") as o:
        for k in keys:
            o.create_group(k).create_dataset("mocap", data=h[k]["mocap"][()])
    for node in NODES:
        os.makedirs(f"{dst}/{node}", exist_ok=True)
        for fname, mods in FILES.items():
            with h5py.File(f"{src}/{node}/{fname}") as h, h5py.File(f"{dst}/{node}/{fname}", "w") as o:
                for k in keys:
                    g = o.create_group(k).create_group(node)
                    for m in mods:
                        g.create_dataset(m, data=h[k][node][m][:])
    return keys


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--start", type=int, default=600)
    ap.add_argument("--frames", type=int, default=300)
    a = ap.parse_args()
    keys = cut(a.src, a.dst, a.start, a.frames)
    print(f"{len(keys)} frames {keys[0]}..{keys[-1]} -> {a.dst}")
