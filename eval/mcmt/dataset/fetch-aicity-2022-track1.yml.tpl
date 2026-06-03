# K8s Job: pull AI City 2022 Track 1 source from Google Drive, extract the
# four cameras we use (scene S04, c016..c019), slice each to {30,60,120}s
# clips, and write them under hostPath /var/lib/wl-workloads/aicity-source.
#
# Runs on anrg-9 (most disk headroom in our cluster). After it finishes,
# stage-aicity-on-nodes.sh (or a paired Job per sensor node) copies the
# sliced clips out to the sensor nodes' hostPath.
#
# Template fields filled by fetch-aicity-2022-track1.sh:
#   {{NAME}}    e.g. vemcmt-aicity-fetch
#   {{NODE}}    e.g. anrg-9
#   {{GDRIVE_ID}}  Google Drive file id
#   {{SCENE}}   e.g. S04
#   {{CAMS}}    space-separated, e.g. "c016 c017 c018 c019"
apiVersion: batch/v1
kind: Job
metadata:
  name: {{NAME}}
  namespace: default
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 1800
  template:
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: {{NODE}}
      volumes:
        - name: source
          hostPath:
            path: /var/lib/wl-workloads/aicity-source
            type: DirectoryOrCreate
        - name: scratch
          hostPath:
            # hostPath instead of emptyDir so we get full node disk
            # (~30 GB on anrg-9) without kubelet's emptyDir sizeLimit.
            # Cleaned up at the end of the Job's main script.
            path: /var/lib/wl-workloads/aicity-scratch
            type: DirectoryOrCreate
      containers:
        - name: fetch
          image: python:3.12-slim
          workingDir: /scratch
          command: ["bash", "-c"]
          args:
            - |
              set -euo pipefail
              echo "Installing tools..."
              apt-get update -qq && apt-get install -y -qq --no-install-recommends \
                  ffmpeg curl ca-certificates unzip xxd >/dev/null
              pip install --no-cache-dir -q gdown

              GDRIVE_ID={{GDRIVE_ID}}
              SCENE={{SCENE}}
              CAMS="{{CAMS}}"
              SOURCE=/source

              # Idempotent download: skip if we already pulled it.
              if [ -f aic22-track1.zip ] && [ -s aic22-track1.zip ]; then
                  echo "Reusing existing aic22-track1.zip ($(du -h aic22-track1.zip | cut -f1))"
              else
                  echo "Downloading AI City tarball from gdrive id=$GDRIVE_ID ..."
                  gdown "https://drive.google.com/uc?id=$GDRIVE_ID" -O aic22-track1.zip
              fi
              ls -lh aic22-track1.zip

              # Detect archive format from magic bytes (the file extension lies).
              MAGIC=$(xxd -p -l 2 aic22-track1.zip)
              echo "magic bytes: $MAGIC"
              case "$MAGIC" in
                  504b) ARC=zip ;;
                  1f8b) ARC=tar.gz ;;
                  *)
                      echo "ERR: unknown archive format ($MAGIC)" >&2
                      exit 3
                      ;;
              esac
              echo "Archive format: $ARC"

              echo "Listing archive contents (first 30 entries)..."
              # Capture to a file then slice — avoid SIGPIPE from head closing
              # early while pipefail is on.
              case "$ARC" in
                  zip)    unzip -l aic22-track1.zip > /tmp/zip-list.txt ;;
                  tar.gz) tar tzf aic22-track1.zip > /tmp/zip-list.txt ;;
              esac
              head -30 /tmp/zip-list.txt

              echo "Extracting only the cameras we need..."
              mkdir -p extracted
              for cam in $CAMS; do
                  echo "  $SCENE/$cam"
                  case "$ARC" in
                      zip)
                          unzip -o -q aic22-track1.zip "*/$SCENE/$cam/vdo.*" -d extracted/ \
                              || unzip -o -q aic22-track1.zip "*/$SCENE/$cam/*" -d extracted/
                          ;;
                      tar.gz)
                          tar xzf aic22-track1.zip --wildcards "*/$SCENE/$cam/vdo.*" -C extracted/ \
                              || tar xzf aic22-track1.zip --wildcards "*/$SCENE/$cam/*" -C extracted/
                          ;;
                  esac
              done

              echo "Slicing clips into hostPath at $SOURCE ..."
              i=1
              for cam in $CAMS; do
                  # Find the source video (could be vdo.avi or vdo.mp4 etc).
                  src=$(find extracted -path "*/$SCENE/$cam/vdo.*" -type f | head -1)
                  if [ -z "$src" ]; then
                      echo "  WARN: no video found for $SCENE/$cam — skipping"
                      i=$((i + 1)); continue
                  fi
                  out_dir="$SOURCE/cam-$i"
                  mkdir -p "$out_dir"
                  for d in 30 60 120; do
                      out="$out_dir/clip_${d}s.mp4"
                      if [ -f "$out" ]; then
                          echo "  skip exists: $out"
                          continue
                      fi
                      echo "  encoding cam-$i (from $cam) × ${d}s from $src"
                      ffmpeg -hide_banner -loglevel error -y \
                          -ss 0 -t "$d" -i "$src" \
                          -c:v libx264 -preset veryfast -crf 23 \
                          -movflags +faststart -an \
                          "$out"
                  done
                  i=$((i + 1))
              done

              echo "Cleaning up scratch (zip + extracted source AVIs) ..."
              rm -rf /scratch/aic22-track1.zip /scratch/extracted

              echo "Done. Output tree:"
              find "$SOURCE" -type f -name '*.mp4' | sort | xargs -I{} ls -lh {}
          volumeMounts:
            - name: source
              mountPath: /source
            - name: scratch
              mountPath: /scratch
          resources:
            requests: { cpu: "2", memory: "2Gi" }
            limits:   { cpu: "4", memory: "4Gi" }
