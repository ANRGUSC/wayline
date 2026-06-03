# vemcmt-decode: FFmpeg with VAAPI hwaccel for iGPU H.264/H.265 decode on
# the i3-N305 Xe-LP integrated GPU. Falls back to software decode if
# /dev/dri/renderD128 isn't mounted (handled inside lib.decode).
#
# Single image, both wrappers baked: the template selects which entrypoint
# runs via its `command:` field (wl_decode_task.py or argo_decode_task.py).
#
# Build from repo root:
#   docker build -f eval/mcmt/images/decode/Dockerfile \
#       -t 192.168.1.163:5000/vemcmt-decode:latest .

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        libva2 libva-drm2 libva-x11-2 \
        intel-media-va-driver-non-free \
        python3 python3-numpy \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY sdk/python/wl ./wl
COPY eval/mcmt/lib ./lib
COPY eval/mcmt/wayline/tasks/decode_task.py   ./wl_decode_task.py
COPY eval/mcmt/argo/tasks/decode_task.py  ./argo_decode_task.py

# Default to Wayline entrypoint; templates override.
CMD ["python3", "wl_decode_task.py"]
