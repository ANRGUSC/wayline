"""
Stage 2: resize + letterbox extracted frames to a fixed input size for
the downstream detector. Pure OpenCV, no model dependency.

Letterbox keeps aspect ratio: scale to fit the target box, pad with gray
(114) on the short side. Detection coordinates from the resized image
can be mapped back to original-image coordinates via the recorded
(scale, pad_x, pad_y) per frame.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Tuple, Union

import cv2  # type: ignore[import-not-found]


def _letterbox(img, target: Tuple[int, int] = (640, 640), color=(114, 114, 114)):
    """Resize img preserving aspect ratio, pad to exactly `target`.

    Returns the resized image plus (scale, pad_x, pad_y) so the detector's
    output bounding boxes can be unmapped to original-image coords later.
    """
    h, w = img.shape[:2]
    tw, th = target
    scale = min(tw / w, th / h)
    new_w, new_h = int(round(w * scale)), int(round(h * scale))
    resized = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
    pad_x = (tw - new_w) // 2
    pad_y = (th - new_h) // 2
    canvas = cv2.copyMakeBorder(
        resized,
        pad_y, th - new_h - pad_y,
        pad_x, tw - new_w - pad_x,
        cv2.BORDER_CONSTANT, value=color,
    )
    return canvas, scale, pad_x, pad_y


def preprocess_frames(
    in_dir: Union[str, Path],
    out_dir: Union[str, Path],
    target_size: Tuple[int, int] = (640, 640),
    quality: int = 88,
    fmt: str = "png",
) -> dict:
    """
    Resize every frame_*.jpg in in_dir to target_size and write to out_dir.

    Also writes preprocess_meta.json with per-frame (scale, pad_x, pad_y)
    for inverse-mapping the detector outputs.

    Returns top-level metadata: { "frames": N, "target": [W,H], "wall_s": float }.
    """
    src = Path(in_dir)
    dst = Path(out_dir)
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)

    frames = sorted(src.glob("frame_*.jpg"))
    per_frame = []
    t0 = time.perf_counter()
    for f in frames:
        img = cv2.imread(str(f), cv2.IMREAD_COLOR)
        if img is None:
            raise RuntimeError(f"preprocess: failed to read {f}")
        out, scale, pad_x, pad_y = _letterbox(img, target=target_size)
        if fmt == "png":
            out_path = dst / (f.stem + ".png")
            ok = cv2.imwrite(str(out_path), out)
        else:
            out_path = dst / f.name
            ok = cv2.imwrite(str(out_path), out, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
        if not ok:
            raise RuntimeError(f"preprocess: failed to write {out_path}")
        per_frame.append({
            "frame": f.name,
            "scale": float(scale),
            "pad_x": int(pad_x),
            "pad_y": int(pad_y),
            "orig_w": int(img.shape[1]),
            "orig_h": int(img.shape[0]),
        })

    wall = time.perf_counter() - t0
    meta = {
        "frames": len(per_frame),
        "target": list(target_size),
        "quality": quality,
        "wall_s": wall,
        "per_frame": per_frame,
    }
    (dst / "preprocess_meta.json").write_text(json.dumps(meta))
    return {"frames": len(per_frame), "target": list(target_size), "wall_s": wall}


if __name__ == "__main__":
    import argparse, json as _json
    p = argparse.ArgumentParser()
    p.add_argument("in_dir")
    p.add_argument("out_dir")
    p.add_argument("--size", type=int, default=640)
    args = p.parse_args()
    print(_json.dumps(preprocess_frames(args.in_dir, args.out_dir,
                                        target_size=(args.size, args.size)), indent=2))
