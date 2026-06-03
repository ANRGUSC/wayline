#!/usr/bin/env bash
#
# Fetch YOLOv8n + OSNet-x0_25 and convert to OpenVINO IR (FP16).
#
# Requirements: a Python venv with the packages in models/requirements.txt
# (install with `--index-url https://download.pytorch.org/whl/cpu` to keep
# torch CPU-only and avoid the multi-GB CUDA wheels — the Intel iGPU is
# the runtime target, not CUDA).
#
# Output (relative to this script's directory):
#   yolov8n.xml + yolov8n.bin       — OpenVINO IR FP16
#   osnet_x0_25.xml + .bin          — OpenVINO IR FP16
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

OSNET_ONNX_URL="https://github.com/KaiyangZhou/deep-person-reid/releases/download/v1.4.4/osnet_x0_25_msmt17.onnx"

echo "=== Step 1/3: pull YOLOv8n .pt and export to OpenVINO IR ==="
if [[ ! -f yolov8n.xml ]]; then
    python3 - <<'PY'
from ultralytics import YOLO
# Ultralytics downloads yolov8n.pt (~6 MB) into the working dir, then
# exports to OpenVINO IR FP16.
m = YOLO("yolov8n.pt")
m.export(format="openvino", half=True, imgsz=640, opset=13)
PY
    # Ultralytics writes to ./yolov8n_openvino_model/yolov8n.{xml,bin}.
    cp yolov8n_openvino_model/yolov8n.xml yolov8n.xml
    cp yolov8n_openvino_model/yolov8n.bin yolov8n.bin
fi
ls -lh yolov8n.xml yolov8n.bin

echo
echo "=== Step 2/3: OSNet-x0_25 ReID model ==="
# Primary path: try the public OSNet ONNX export.
# Fallback: build a small reproducible stub embedder (deterministic
# linear projection) so the data plane can be smoke-tested even when
# the OSNet release is offline. For paper results, replace the stub
# with the real export before re-building the detect_embed image.
if [[ ! -f osnet_x0_25.xml ]]; then
    if [[ ! -f osnet_x0_25.onnx ]]; then
        if curl -fLs --max-time 30 "$OSNET_ONNX_URL" -o osnet_x0_25.onnx; then
            echo "  OK: downloaded real OSNet-x0_25 from upstream"
        else
            echo "  WARN: upstream OSNet ONNX not reachable; building stub embedder for smoke"
            rm -f osnet_x0_25.onnx
            python3 - <<'PY'
"""Stub embedder: NCHW (1,3,256,128) → (1,512). Deterministic linear
projection seeded from the input checksum. Useful only for smoke testing
the mcmt data plane — NOT for paper-quality ReID matching."""
import numpy as np
import torch
import torch.nn as nn

torch.manual_seed(0)

class StubReID(nn.Module):
    def __init__(self):
        super().__init__()
        self.flat = nn.AdaptiveAvgPool2d((8, 4))     # 1x3x8x4 = 96
        self.proj = nn.Linear(3 * 8 * 4, 512)
    def forward(self, x):
        y = self.flat(x).flatten(1)
        z = self.proj(y)
        # L2-normalize so cosine similarity downstream is well-behaved.
        return torch.nn.functional.normalize(z, p=2, dim=1)

m = StubReID().eval()
dummy = torch.zeros(1, 3, 256, 128)
torch.onnx.export(
    m, dummy, "osnet_x0_25.onnx",
    input_names=["input"], output_names=["output"],
    dynamic_axes={"input": {0: "batch"}, "output": {0: "batch"}},
    opset_version=13,
)
print("  wrote stub osnet_x0_25.onnx")
PY
        fi
    fi
    if command -v ovc >/dev/null 2>&1; then
        ovc osnet_x0_25.onnx --output_model osnet_x0_25 --compress_to_fp16
    else
        mo --input_model osnet_x0_25.onnx --output_dir . --model_name osnet_x0_25 --data_type FP16
    fi
fi
ls -lh osnet_x0_25.xml osnet_x0_25.bin

echo
echo "=== Step 3/3: sanity — both IRs load on CPU ==="
python3 - <<'PY'
import openvino as ov
c = ov.Core()
for name in ("yolov8n", "osnet_x0_25"):
    m = c.read_model(f"{name}.xml")
    cm = c.compile_model(m, "CPU")
    in0 = cm.input(0); out0 = cm.output(0)
    print(f"  {name}: input={in0.any_name} shape={list(in0.shape)} output={out0.any_name} shape={list(out0.shape)}")
PY

echo
echo "Done. Files ready for image build:"
ls -lh *.xml *.bin
