"""
Stage 3: detection (YOLOv8n) + ReID embedding (OSNet) on each preprocessed
frame. OpenVINO runtime; targets the Intel iGPU when `device="GPU"` is
selected and /dev/dri is mounted in the pod (fix #1 unblocked this).

Writes:
    detections.json   — per-frame bboxes, classes, scores (mapped back to
                        original-image coords using preprocess_meta.json)
    embeddings.npy    — (N_detections, 512) float32; row i corresponds to
                        detections.json["detections"][i]
"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path
from typing import Union

import cv2  # type: ignore[import-not-found]
import numpy as np
import openvino as ov  # type: ignore[import-not-found]


# COCO classes we keep (everything else is dropped at filter time). Vehicle
# detection for AI City is dominated by car/truck/bus/motorcycle.
_VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}


def _yolo_postprocess(
    output: np.ndarray,
    scale: float, pad_x: int, pad_y: int,
    conf_thresh: float = 0.35,
    iou_thresh: float = 0.5,
) -> list:
    """
    Decode YOLOv8 output tensor (1, 84, 8400) into a list of detections
    in ORIGINAL-image coordinates (undoing letterbox).
    """
    # YOLOv8 output is (1, 4+nc, 8400): [cx, cy, w, h, c0..c79] per anchor.
    out = output[0]                          # (84, 8400)
    boxes = out[:4, :].T                     # (8400, 4)
    cls_scores = out[4:, :]                  # (80, 8400)
    cls_ids = np.argmax(cls_scores, axis=0)  # (8400,)
    confs = cls_scores[cls_ids, np.arange(cls_scores.shape[1])]  # (8400,)

    keep = (confs >= conf_thresh) & np.isin(cls_ids, list(_VEHICLE_CLASSES.keys()))
    if not np.any(keep):
        return []
    boxes = boxes[keep]
    confs = confs[keep]
    cls_ids = cls_ids[keep]

    # cx,cy,w,h → x1,y1,x2,y2 in 640x640 coords.
    xy = np.empty_like(boxes)
    xy[:, 0] = boxes[:, 0] - boxes[:, 2] / 2
    xy[:, 1] = boxes[:, 1] - boxes[:, 3] / 2
    xy[:, 2] = boxes[:, 0] + boxes[:, 2] / 2
    xy[:, 3] = boxes[:, 1] + boxes[:, 3] / 2

    # NMS in 640-space.
    indices = cv2.dnn.NMSBoxes(
        xy.tolist(), confs.tolist(),
        score_threshold=conf_thresh, nms_threshold=iou_thresh,
    )
    if isinstance(indices, (list, tuple)):
        idx_list = [int(i) for i in indices]
    else:
        idx_list = indices.flatten().tolist() if len(indices) else []
    if not idx_list:
        return []

    # Undo letterbox: subtract pad, divide by scale.
    dets = []
    for i in idx_list:
        x1 = (xy[i, 0] - pad_x) / scale
        y1 = (xy[i, 1] - pad_y) / scale
        x2 = (xy[i, 2] - pad_x) / scale
        y2 = (xy[i, 3] - pad_y) / scale
        dets.append({
            "bbox": [float(x1), float(y1), float(x2), float(y2)],
            "score": float(confs[i]),
            "class_id": int(cls_ids[i]),
            "class": _VEHICLE_CLASSES.get(int(cls_ids[i]), "unknown"),
        })
    return dets


def _osnet_crop(img: np.ndarray, bbox, target=(128, 256)) -> np.ndarray:
    """Crop a vehicle from img, resize to OSNet's 128x256 input, normalize.

    OSNet expects NCHW float32, [0,1] normalized with ImageNet mean/std.
    """
    x1, y1, x2, y2 = [max(0, int(round(v))) for v in bbox]
    crop = img[y1:y2, x1:x2]
    if crop.size == 0:
        # Degenerate box (out-of-frame after letterbox unmap). Return zeros.
        return np.zeros((1, 3, target[1], target[0]), dtype=np.float32)
    crop = cv2.resize(crop, target, interpolation=cv2.INTER_LINEAR)
    crop = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB).astype(np.float32) / 255.0
    mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
    std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
    crop = (crop - mean) / std
    crop = np.transpose(crop, (2, 0, 1))[None, ...]  # (1, 3, 256, 128)
    return crop.astype(np.float32)


def detect_and_embed(
    in_dir: Union[str, Path],
    out_dir: Union[str, Path],
    det_model: Union[str, Path],
    reid_model: Union[str, Path],
    device: str = "GPU",
) -> dict:
    """
    Run YOLOv8n on each preprocessed frame, then OSNet on each detected
    crop. Writes detections.json + embeddings.npy + crops_meta.json into
    out_dir.

    `device` is passed to OpenVINO.Core.compile_model — "GPU" targets the
    Intel iGPU (Xe-LP on the i3-N305 nodes); "CPU" is the fallback.

    Returns: { "frames": F, "detections": D, "wall_s": float, "device": str }.
    """
    src = Path(in_dir)
    dst = Path(out_dir)
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)

    pre_meta = json.loads((src / "preprocess_meta.json").read_text())
    per_frame = {f["frame"]: f for f in pre_meta["per_frame"]}

    core = ov.Core()
    # Try the requested device first (typically GPU = Intel iGPU via
    # /dev/dri). If that errors (missing user-space OpenCL stack, no GPU
    # context, etc.), fall back to CPU and keep going. The data plane is
    # what we want to measure; the algorithm runs correctly either way.
    def _compile(model_path: str):
        try:
            return core.compile_model(model=model_path, device_name=device), device
        except Exception as e:
            if device != "CPU":
                print(f"[detect_embed] {device} compile failed ({e!s}); falling back to CPU", flush=True)
                return core.compile_model(model=model_path, device_name="CPU"), "CPU"
            raise
    _t_compile0 = time.perf_counter()
    det, det_device = _compile(str(det_model))
    reid, reid_device = _compile(str(reid_model))
    compile_s = time.perf_counter() - _t_compile0
    effective_device = det_device if det_device == reid_device else f"{det_device}/{reid_device}"
    # Use positional input/output access ([tensor] / result[0]) so we work
    # regardless of whether the source model named its tensors. The
    # Ultralytics YOLOv8 export and the OSNet stub both emit unnamed
    # outputs; this avoids tripping on `.any_name` lookups.

    all_dets = []   # parallel to embeddings rows
    embeddings = []
    t0 = time.perf_counter()
    n_frames = 0

    # Match both JPEG (legacy) and PNG (lossless preprocess for paper-grade
    # data-plane comparison). Falls back gracefully if no frames in a
    # particular extension.
    candidates = sorted(list(src.glob("frame_*.png")) + list(src.glob("frame_*.jpg")))
    for frame_path in candidates:
        # preprocess_meta.json keys by the ORIGINAL frame name (.jpg from
        # decode); when preprocess wrote .png we need to map back.
        meta_name = frame_path.stem + ".jpg"
        info = per_frame.get(meta_name) or per_frame.get(frame_path.name)
        if info is None:
            continue
        n_frames += 1

        # Detection — read preprocessed (already 640x640), convert to NCHW float32 [0,1].
        img = cv2.imread(str(frame_path), cv2.IMREAD_COLOR)
        if img is None:
            continue
        det_input = cv2.cvtColor(img, cv2.COLOR_BGR2RGB).astype(np.float32) / 255.0
        det_input = np.transpose(det_input, (2, 0, 1))[None, ...]
        det_out = det([det_input])[0]

        dets = _yolo_postprocess(
            det_out,
            scale=info["scale"], pad_x=info["pad_x"], pad_y=info["pad_y"],
        )
        if not dets:
            continue

        # ReID embedding — we need the ORIGINAL-image crop, not the
        # letterboxed one. Reload the original by following the source clip
        # frame: detection bbox is already in original-image coords thanks
        # to _yolo_postprocess unmapping. But the preprocessed frame IS
        # the letterboxed 640x640, not the original. We approximate by
        # cropping from the letterboxed image at the letterboxed coords
        # (re-apply letterbox transform). This keeps the embedding stable
        # and only depends on the preprocessed frame — no need to ship
        # the original frames through to this stage.
        for d in dets:
            # Re-map the bbox back into letterbox space for cropping.
            x1, y1, x2, y2 = d["bbox"]
            lx1 = x1 * info["scale"] + info["pad_x"]
            ly1 = y1 * info["scale"] + info["pad_y"]
            lx2 = x2 * info["scale"] + info["pad_x"]
            ly2 = y2 * info["scale"] + info["pad_y"]
            crop_input = _osnet_crop(img, [lx1, ly1, lx2, ly2])
            reid_out = reid([crop_input])[0]
            emb = reid_out.flatten().astype(np.float32)
            # L2-normalize for cosine similarity downstream.
            n = np.linalg.norm(emb)
            if n > 0:
                emb = emb / n
            d["frame"] = frame_path.name
            d["det_index"] = len(embeddings)
            embeddings.append(emb)
            all_dets.append(d)

    wall = time.perf_counter() - t0
    print(f"[detect_embed timing] compile={compile_s:.2f}s inference={wall:.2f}s "
          f"frames={n_frames} dets={len(all_dets)} dev={effective_device}", flush=True)
    emb_arr = np.stack(embeddings) if embeddings else np.zeros((0, 512), dtype=np.float32)
    np.save(str(dst / "embeddings.npy"), emb_arr)
    (dst / "detections.json").write_text(json.dumps({
        "device": effective_device,
        "frames": n_frames,
        "detections": all_dets,
    }))
    return {
        "frames": n_frames,
        "detections": len(all_dets),
        "wall_s": wall,
        "device": effective_device,
    }
