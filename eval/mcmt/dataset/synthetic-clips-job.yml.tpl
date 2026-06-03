# One-shot K8s Job per sensor node that generates synthetic mcmt
# clips directly into /var/lib/wl-workloads/aicity/cam-<i>/clip_<d>s.mp4.
# Sidesteps the need for ffmpeg on the dev host AND ssh access to nodes:
# the kubelet schedules an ffmpeg container on the target node and writes
# clips to its hostPath.
#
# Template fields filled by dataset/prepare-synthetic-via-k8s.sh:
#   {{NAME}}    e.g. vemcmt-synth-cam-1
#   {{NODE}}    e.g. anrg-1
#   {{CAMERA}}  e.g. cam-1
apiVersion: batch/v1
kind: Job
metadata:
  name: {{NAME}}
  namespace: default
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: {{NODE}}
      volumes:
        - name: aicity
          hostPath:
            path: /var/lib/wl-workloads/aicity
            type: DirectoryOrCreate
      containers:
        - name: ffmpeg
          image: linuxserver/ffmpeg:7.1.1
          command: ["sh", "-c"]
          args:
            - |
              set -eu
              DEST=/data/{{CAMERA}}
              mkdir -p "$DEST"
              for d in 30 60 120; do
                out="$DEST/clip_${d}s.mp4"
                if [ -f "$out" ]; then
                  echo "skip (exists): $out"
                  continue
                fi
                echo "generating $out"
                /usr/local/bin/ffmpeg -hide_banner -loglevel error -y \
                  -f lavfi -i "testsrc=duration=${d}:size=1280x720:rate=30" \
                  -vf "drawtext=text='{{CAMERA}} f%{n}':fontcolor=white:fontsize=42:x=20:y=20" \
                  -c:v libx264 -preset veryfast -crf 23 \
                  -movflags +faststart -an \
                  "$out"
              done
              ls -lh "$DEST"
          volumeMounts:
            - name: aicity
              mountPath: /data
          resources:
            requests: { cpu: "500m", memory: "256Mi" }
            limits:   { cpu: "2",    memory: "1Gi" }
