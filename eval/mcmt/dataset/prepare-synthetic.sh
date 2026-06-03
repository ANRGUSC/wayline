#!/usr/bin/env bash
#
# Generate synthetic test clips with FFmpeg's testsrc + drawtext filters
# for smoke testing the mcmt pipeline WITHOUT the AI City
# Challenge dataset.
#
# The clips contain moving color bars and a frame counter — no real
# vehicles. The detector will return zero detections per frame, the
# pipeline will run end-to-end, and report.json will have zero global
# tracks. This is the right behavior to validate:
#   - DAG topology renders + applies correctly
#   - Per-task containers launch + read+write tarball intermediates
#   - The data plane carries bytes between stages
#   - The correctness diff matches between Wayline and Argo (both will
#     produce the same empty report)
#
# It does NOT validate the algorithm — for that you need real video.
#
# Output (same layout as prepare.sh):
#   clips/cam-{1..N}/clip_{30,60,120}s.mp4
set -euo pipefail

N_CAMS="${VEMCMT_N_CAMS:-4}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/clips"
mkdir -p "$OUT"

for i in $(seq 1 "$N_CAMS"); do
    out_dir="$OUT/cam-$i"
    mkdir -p "$out_dir"
    for d in 30 60 120; do
        out_clip="$out_dir/clip_${d}s.mp4"
        if [[ -f "$out_clip" ]]; then
            echo "  skip (exists): $out_clip"
            continue
        fi
        # 1280x720 @ 30fps testsrc with a per-camera color tint + frame
        # counter overlay. encode H.264 baseline so VAAPI decode works.
        echo "  generating cam-$i × ${d}s synthetic clip"
        ffmpeg -hide_banner -loglevel error -y \
            -f lavfi -i "testsrc=duration=${d}:size=1280x720:rate=30" \
            -vf "drawtext=text='cam ${i} f%{n}':fontcolor=white:fontsize=42:x=20:y=20" \
            -c:v libx264 -preset veryfast -crf 23 \
            -movflags +faststart -an \
            "$out_clip"
    done
done

echo
echo "Done. Synthetic clips written. Run dataset/stage-on-nodes.sh to distribute."
find "$OUT" -type f -name '*.mp4' | sort | xargs -I{} ls -la {}
