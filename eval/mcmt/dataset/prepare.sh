#!/usr/bin/env bash
#
# Slice the AI City Challenge Track 1 source videos into the {30,60,120}s
# clips this eval expects, one directory per camera.
#
# Input: $VEMCMT_AICITY_SOURCE — path to a directory containing
#   c001/vdo.avi  c002/vdo.avi  c003/vdo.avi  c004/vdo.avi  (...)
# (the AI City Track 1 release layout, scene S04 by default).
#
# Output (under ./clips/):
#   clips/cam-1/clip_30s.mp4   clip_60s.mp4   clip_120s.mp4
#   clips/cam-2/...
#   clips/cam-3/...
#   clips/cam-4/...
#
# Each clip is re-encoded with H.264 at the source resolution/bitrate so
# the data plane sees realistic frame intermediates. The mp4 container is
# faststart so VAAPI decode doesn't have to scan to find the moov box.
set -euo pipefail

SRC="${VEMCMT_AICITY_SOURCE:-./source}"
SCENE="${VEMCMT_SCENE:-S04}"
N_CAMS="${VEMCMT_N_CAMS:-4}"
START="${VEMCMT_START_SECONDS:-0}"
DURATIONS=(30 60 120)

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/clips"
mkdir -p "$OUT"

# AI City Track 1 camera IDs for scene S04 are c016..c040; the four we use
# for the default cell come from one intersection. The env var
# VEMCMT_CAM_IDS lets the caller override.
CAM_IDS_DEFAULT="c016 c017 c018 c019"
CAM_IDS="${VEMCMT_CAM_IDS:-$CAM_IDS_DEFAULT}"
read -r -a CAM_IDS_ARR <<< "$CAM_IDS"

if (( ${#CAM_IDS_ARR[@]} < N_CAMS )); then
    echo "ERROR: need at least $N_CAMS camera IDs, have ${#CAM_IDS_ARR[@]}" >&2
    exit 2
fi

echo "Source root: $SRC"
echo "Scene: $SCENE"
echo "Cameras: ${CAM_IDS_ARR[*]:0:$N_CAMS}"
echo "Durations: ${DURATIONS[*]}"
echo

for i in $(seq 1 "$N_CAMS"); do
    cam_id="${CAM_IDS_ARR[$((i - 1))]}"
    src_video="$SRC/$SCENE/$cam_id/vdo.avi"
    if [[ ! -f "$src_video" ]]; then
        echo "ERROR: missing source video: $src_video" >&2
        exit 3
    fi
    out_dir="$OUT/cam-$i"
    mkdir -p "$out_dir"
    for d in "${DURATIONS[@]}"; do
        out_clip="$out_dir/clip_${d}s.mp4"
        if [[ -f "$out_clip" ]]; then
            echo "  skip (exists): $out_clip"
            continue
        fi
        echo "  encoding cam-$i × ${d}s from $cam_id"
        ffmpeg -hide_banner -loglevel error -y \
            -ss "$START" -t "$d" -i "$src_video" \
            -c:v libx264 -preset veryfast -crf 23 \
            -movflags +faststart -an \
            "$out_clip"
    done
done

echo
echo "Done. Tree:"
find "$OUT" -type f -name '*.mp4' | sort | xargs -I{} ls -la {}
