# mcmt — Real-Workload ATC Evaluation

A **Multi-Camera Multi-Target tracking** (MCMT) workflow ported from VideoEdge
(Hung et al., SEC 2018), running against the AI City Challenge Track 1
dataset. Used as the real-workload evidence in the ATC submission, sitting
alongside the synthetic E1 head-to-head.

## What this is

The reference workflow asks: *for a fixed clip window, identify each
unique vehicle that traversed the monitored area and the sequence of
cameras it appeared in.* This is precisely the AI City Track 1 task and
the canonical VideoEdge cross-camera query.

The DAG shape comes from two sources:

- **Per-camera pipeline** mirrors the NVIDIA DeepStream reference
  pipeline (decode → preprocess → primary detector → tracker), the
  industry-standard topology for multi-stream video analytics.
- **Cross-camera structure** mirrors VideoEdge's hierarchical query
  graph: per-camera tracklets feed a fan-in stage that assigns global
  vehicle IDs via ReID embedding similarity + Hungarian assignment.

We did not design this DAG. We ported it. That is the point.

## DAG

For N cameras (default N=4):

```
camera-i  (i = 1..N)
─────────
decode_i  ───▶  preprocess_i  ───▶  detect_embed_i  ───▶  track_i ──┐
                                                                    │
                                                                    ▼
                                                      cross_camera_match
                                                                    │
                                                                    ▼
                                                                 report
```

Tasks per cell: **N×4 + 2** (=18 at N=4).

## Tier mapping

Mirroring VideoEdge's edge / cluster / aggregation hierarchy onto our 8-node
testbed:

| Tier | Nodes | Tasks pinned |
|---|---|---|
| Sensor (edge) | anrg-1, 3, 4, 5 (one per camera) | `decode_i`, `preprocess_i` |
| Compute (cluster) | anrg-6, 7, 8 (scheduler picks) | `detect_embed_i`, `track_i` |
| Aggregation (cloud) | anrg-9 | `cross_camera_match`, `report` |

Wayline and Argo use the **same** pin map so we're comparing data-plane
behavior under identical placement, not scheduling quality.

## Per-task contract

Every stage reads a tar.gz blob from each upstream dep (placed by Wayline or
Argo into `/in/<dep>/output`) and writes a single tar.gz to
`/out/output`. The lib functions are pure — they take an extracted input
directory, produce an extracted output directory. The thin Wayline and Argo
wrappers handle tar pack/unpack.

| Stage | Input | Operation | Output |
|---|---|---|---|
| `decode_i` | clip_i.mp4 (hostPath fixture on sensor node) | FFmpeg w/ VAAPI iGPU decode → JPEG frames at 5 fps | `frames/frame_NNNNNN.jpg` |
| `preprocess_i` | frames tar | OpenCV resize to 640×640, letterbox | `preprocessed/frame_NNNNNN.jpg` |
| `detect_embed_i` | preprocessed frames | OpenVINO YOLOv8n FP16 on iGPU + OSNet-x0_25 ReID embeddings on each crop | `detections.json` + `embeddings.npy` |
| `track_i` | detections + embeddings | ByteTrack-style within-camera tracking | `tracklets.json` + `tracklet_embeddings.npy` |
| `cross_camera_match` | tracklets from every camera | Pairwise cosine similarity + Hungarian assignment | `global_tracks.json` |
| `report` | global tracks | Aggregation (counts/classes/paths/dwell) | `report.json` |

Heavy data edges:
- `decode → preprocess` (~50–300 MB frame tars, intra-node)
- `preprocess → detect_embed` (~30–150 MB preprocessed tars, cross-tier)
- `track → cross_camera_match` (~5 MB tracklets + embeddings per camera at fan-in)

## Models

All ONNX, converted to OpenVINO IR at image-build time. Weights baked into
container images — no runtime download.

| Model | Role | Format | Size |
|---|---|---|---|
| YOLOv8n | Vehicle detection | IR FP16 | ~6 MB |
| OSNet-x0_25 | ReID embeddings | IR FP16 | ~2 MB |
| ByteTrack | Tracker | Pure Python | — |

iGPU access: each pod mounts `/dev/dri` via the ODAGTemplate
`volumes` / `volumeMounts` fields (fix #1) and joins gid 992 via
`securityContext.supplementalGroups`. `device="GPU"` in the OpenVINO
runtime targets the Xe-LP 32-EU iGPU on the i3-N305 nodes.

## Dataset

**Primary:** AI City Challenge Track 1 (CityFlow V2) — multi-camera
multi-target vehicle tracking. One scene's 4 cameras are pre-sliced to
{30, 60, 120}s clips and staged on the sensor nodes' hostPath at
`/var/lib/wl-workloads/aicity/`. Both Wayline and Argo read from the same
hostPath; no runtime download.

**Fallback:** VIRAT public release if AI City registration is a blocker.
The lib functions are dataset-agnostic; only `dataset/prepare.sh` and
the clip-mapping change.

## Sweep axes

| Axis | Levels | Role |
|---|---|---|
| Clip duration | 30s, 60s, 120s | Drives intra-camera intermediate data size |
| Number of cameras | 2, 4, 8 | Drives fan-in width |
| System | Wayline+HEFT, Wayline+random, Argo+MinIO, Argo+MinIO+gzip | Comparison matrix |

Default cell: 4 cameras × 60s × YOLOv8n.

## Baseline matrix

| System | Description |
|---|---|
| Wayline + HEFT | Primary |
| Wayline + random | Scheduler ablation |
| Argo + MinIO (default) | Standard Argo |
| Argo + MinIO + gzip | Tuned Argo (pre-empts "did you compress?" reviewer ask) |

Wayline compression (`--push-compress=gzip`) gives a symmetric "Wayline + gzip"
cell if needed for a four-way comparison.

## Correctness diff

After every paired run, `scripts/verify_reports.py` compares Wayline's
`report.json` against Argo's:
- **Hard match**: unique vehicle count per class.
- **Hard match**: camera-path sequence per global vehicle ID.
- **Tolerated**: floating-point fields (dwell times) within 1e-3.

Inference is seeded for determinism. Diff failures reject the run.

## Layout

```
eval/mcmt/
├── README.md                this file
├── lib/                     pure-Python computation (no Wayline/Argo coupling)
│   ├── decode.py
│   ├── preprocess.py
│   ├── detect_embed.py
│   ├── track.py
│   ├── match.py
│   ├── report.py
│   └── payload.py           tar.gz pack/unpack helpers
├── wayline/
│   ├── tasks/               6 thin wrappers (WlTask + send/recv tarballs)
│   ├── template.yml.tpl     ODAGTemplate with N_CAMERAS, CLIP_DURATION
│   ├── render.py            template renderer
│   └── submit.sh
├── argo/
│   ├── tasks/               6 thin wrappers (read /in/, write /out/)
│   ├── workflow.yml.tpl     Argo WorkflowTemplate
│   ├── render.py
│   └── submit.sh
├── images/                  6 stage Dockerfiles
│   ├── decode/Dockerfile    ffmpeg + libva (VAAPI iGPU)
│   ├── preprocess/Dockerfile
│   ├── detect_embed/Dockerfile   OpenVINO + iGPU device plugin
│   ├── track/Dockerfile
│   ├── cross_camera_match/Dockerfile
│   └── report/Dockerfile
├── models/
│   └── fetch.sh             downloads + converts to OpenVINO IR
├── dataset/
│   ├── prepare.sh           slices AI City clips
│   └── stage-on-nodes.sh    distributes to sensor-node hostPath
├── scripts/
│   ├── build-and-push.sh
│   ├── preflight-idle.sh    (symlink → ../two-hop/preflight-idle.sh)
│   ├── verify_reports.py    Wayline↔Argo report diff
│   ├── run.sh               single cell
│   ├── sweep.sh             full matrix
│   └── harvest.py           CSV-ifies per-run timestamps
├── results/                 gitignored
└── figures/                 plot outputs
```

## Citations

- Hung, C.-C., Ananthanarayanan, G., Bodik, P., Golubchik, L., Yu, M.,
  Bahl, P., Philipose, M. *VideoEdge: Processing Camera Streams using
  Hierarchical Clusters*. SEC 2018.
- NVIDIA DeepStream SDK Reference Pipeline.
  developer.nvidia.com/deepstream-sdk
- Naphade, M. et al. *The 5th AI City Challenge*. CVPR Workshops 2021
  (CityFlow V2 dataset).
- Zhang, Y. et al. *ByteTrack: Multi-Object Tracking by Associating
  Every Detection Box*. ECCV 2022.
- Zhou, K., Xiang, T. *Torchreid: A Library for Deep Learning Person
  Re-Identification in PyTorch* (OSNet). 2019.
