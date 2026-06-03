"""
Stage 1: decode a clip into JPEG frames at the target FPS.

Uses FFmpeg via subprocess. When /dev/dri/renderD128 is present (the
Intel iGPU on UP 7000 nodes), VAAPI hardware-accelerated H.264/H.265
decode is used; otherwise it falls back to libavcodec software decode.
The choice is reported in the stage metadata so the eval can show
"VAAPI on N producer nodes, software on M" if it matters.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Union


def _vaapi_available() -> bool:
    """True iff /dev/dri/renderD128 exists AND ffmpeg has the vaapi hwaccel
    compiled in. The first is per-node; the second is per-image."""
    if not Path("/dev/dri/renderD128").exists():
        return False
    try:
        out = subprocess.run(
            ["ffmpeg", "-hide_banner", "-hwaccels"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return "vaapi" in out.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def decode_clip(
    clip_path: Union[str, Path],
    out_dir: Union[str, Path],
    fps: int = 5,
    use_vaapi: Union[bool, None] = None,
) -> dict:
    """
    Decode clip_path to JPEG frames in out_dir at the target fps.

    Returns metadata: { "clip": str, "fps": int, "frames": int,
                        "duration_s": float, "decoder": "vaapi"|"software",
                        "wall_s": float }

    Frames are named frame_NNNNNN.jpg starting at 1 (FFmpeg's default
    one-indexed scheme). The caller's wrapper handles tarring the output
    directory.
    """
    clip = Path(clip_path)
    if not clip.is_file():
        raise FileNotFoundError(f"decode_clip: clip not found: {clip}")
    out = Path(out_dir)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    if use_vaapi is None:
        use_vaapi = _vaapi_available()

    pattern = str(out / "frame_%06d.jpg")

    # FFmpeg invocation. VAAPI hwaccel decodes on the iGPU; we read frames
    # back to system memory with `hwdownload` then encode to MJPEG. Quality
    # is -q:v 5 (mid-range JPEG; smaller files than the default of 2-3).
    if use_vaapi:
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
            "-hwaccel", "vaapi",
            "-hwaccel_device", "/dev/dri/renderD128",
            "-hwaccel_output_format", "vaapi",
            "-i", str(clip),
            "-vf", f"fps={fps},hwdownload,format=nv12,format=yuvj420p",
            "-q:v", "5",
            pattern,
        ]
        decoder = "vaapi"
    else:
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
            "-i", str(clip),
            "-vf", f"fps={fps}",
            "-q:v", "5",
            pattern,
        ]
        decoder = "software"

    # Force the Intel iHD driver when VAAPI is requested. Some base images
    # ship libva but don't auto-detect i965 vs iHD; iHD is the right one
    # for Xe-LP (i3-N305 here).
    env = os.environ.copy()
    if use_vaapi:
        env.setdefault("LIBVA_DRIVER_NAME", "iHD")
    t0 = time.perf_counter()
    res = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    wall = time.perf_counter() - t0

    if res.returncode != 0:
        # VAAPI runtime can fail even if /dev/dri exists (missing driver,
        # SELinux, etc.). Fall back to software decode rather than failing
        # the whole pipeline — the data plane is what we're testing here.
        if use_vaapi:
            print(f"[decode_clip] VAAPI failed (code={res.returncode}), retrying with software decode", flush=True)
            cmd_sw = [
                "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
                "-i", str(clip),
                "-vf", f"fps={fps}",
                "-q:v", "5",
                pattern,
            ]
            t0 = time.perf_counter()
            res = subprocess.run(cmd_sw, capture_output=True, text=True, check=False)
            wall = time.perf_counter() - t0
            decoder = "software"
            if res.returncode != 0:
                raise RuntimeError(
                    f"decode_clip: ffmpeg failed on both VAAPI and software paths (code={res.returncode}):\n"
                    f"stderr: {res.stderr[-2000:]}"
                )
        else:
            raise RuntimeError(
                f"decode_clip: ffmpeg failed (decoder={decoder}, code={res.returncode}):\n"
                f"stderr: {res.stderr[-2000:]}"
            )

    frames = sorted(out.glob("frame_*.jpg"))
    meta = {
        "clip": str(clip),
        "fps": int(fps),
        "frames": len(frames),
        "duration_s": len(frames) / fps if frames else 0.0,
        "decoder": decoder,
        "wall_s": wall,
    }
    (out / "decode_meta.json").write_text(json.dumps(meta))
    return meta


if __name__ == "__main__":
    # Tiny CLI shim for ad-hoc testing: `python -m lib.decode <clip> <out>`.
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("clip")
    p.add_argument("out_dir")
    p.add_argument("--fps", type=int, default=5)
    args = p.parse_args()
    print(json.dumps(decode_clip(args.clip, args.out_dir, fps=args.fps), indent=2))
