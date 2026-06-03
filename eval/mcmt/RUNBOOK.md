# mcmt — Runbook

End-to-end sequence from a clean checkout to figures in `figures/`. Each
step is independent and re-runnable. The data plane, controller, and
SDK fixes are assumed already deployed (see project memory's
`project_atc2026_data_plane_state_model`).

## 0. Cluster sanity

```sh
# Controllers + data-agent rolled with the data-plane fixes in place.
kubectl -n wl-system get deploy odag-controller -o jsonpath='{.spec.template.spec.containers[0].image}'
kubectl -n wl-system get ds   data-agent       -o jsonpath='{.spec.template.spec.containers[0].image}'
kubectl -n wl-system get pods -l app=data-agent -o jsonpath='{.items[*].status.phase}' ; echo
```

All `data-agent` pods should be `Running`. The cluster idle preflight is
called by `scripts/run.sh` automatically, but you can sanity-check it:

```sh
scripts/preflight-idle.sh && echo idle
```

## 1. Build-host venv + model IRs

```sh
python3 -m venv .venv-build
source .venv-build/bin/activate
pip install -r eval/mcmt/models/requirements.txt
eval/mcmt/models/fetch.sh
```

This produces `eval/mcmt/models/{yolov8n,osnet_x0_25}.{xml,bin}`,
which `images/detect_embed/Dockerfile` will COPY at build time.

## 2. Dataset

Pick one:

**Real (AI City Challenge Track 1, default).** Download the release into
`$VEMCMT_AICITY_SOURCE/$VEMCMT_SCENE/c0XX/vdo.avi`. Then:

```sh
VEMCMT_AICITY_SOURCE=/path/to/aicity \
VEMCMT_SCENE=S04 \
VEMCMT_CAM_IDS="c016 c017 c018 c019" \
eval/mcmt/dataset/prepare.sh
```

**Synthetic (smoke test only).** Generates FFmpeg `testsrc` clips with no
vehicles. Detector returns empty; the pipeline still runs end-to-end and
the correctness diff trivially passes. Use this to validate plumbing
before committing to the full build.

```sh
eval/mcmt/dataset/prepare-synthetic.sh
```

Either way, push clips to the sensor nodes' hostPath:

```sh
eval/mcmt/dataset/stage-on-nodes.sh
```

This rsyncs into `/var/lib/wl-workloads/aicity/cam-N/` on each
`anrg-{1,3,4,5}` (sensor-tier mapping).

## 3. Build + push images

```sh
eval/mcmt/scripts/build-and-push.sh
```

Six images at `192.168.1.163:5000/vemcmt-*:latest`. Re-run any time the
SDK, lib, or stage entrypoints change.

## 4. Smoke (one rep at default cell)

```sh
eval/mcmt/scripts/run.sh 4 60 1
```

Inspect `eval/mcmt/results/n4-d60/summary.csv` — expect one Wayline
row and one Argo row, both `Succeeded`, with `report_ok=true`. The
correctness diff log lives at `results/n4-d60/diff-rep1.log`.

If anything fails: the data-agent's `/metrics` endpoint per node and the
controller logs (`kubectl -n wl-system logs deploy/odag-controller`) are
the two best diagnostic surfaces.

## 5. Full sweep

```sh
REPS=10 eval/mcmt/scripts/sweep.sh
```

Default sweep is `{N=2,4,8} × {D=30,60,120}` = 9 cells × 10 reps × 2
systems = 180 paired runs. Override via env:

```sh
REPS=20 CAM_LIST="4" DUR_LIST="60" eval/mcmt/scripts/sweep.sh
```

## 6. Harvest + plot

```sh
cd eval/mcmt
python3 scripts/harvest.py
python3 scripts/plot.py
```

Outputs:

- `results/all.csv` — flat row-per-run CSV
- `figures/e1v-makespan-box.{png,pdf}` — Wayline vs Argo per cell
- `figures/e1v-makespan-vs-duration.{png,pdf}` — clip-duration sensitivity at N=4
- `figures/e1v-makespan-vs-cameras.{png,pdf}` — camera-count sensitivity at D=60
- `figures/e1v-speedup.{png,pdf}` — Wayline/Argo ratio per cell
- `figures/e1v-summary.md` — text table

## Sub-experiments worth running once the headline numbers are in

- **Wayline + gzip data plane:** roll the data-agent with
  `--push-compress=gzip` in the DaemonSet args, re-sweep, compare against
  the default (`none`) row. Symmetric to "Argo + compression" cell.
- **Bounded fan-out ablation:** sweep `--max-concurrent-pushes=1,2,4,8`
  for the 8-camera cells; bounded vs sequential vs unbounded.
- **Wayline + random scheduler:** add `--scheduler random` to
  `wayline/render.py` and a second cell. Isolates transport from scheduling.
- **Mechanism decomposition** (per the architecture review): run cells
  with one mechanism disabled at a time — direct-write SDK (no
  data-agent install), fixed timeout, MinIO restart mid-sweep — to
  attribute the speedup to each property of the data plane.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Decode pod logs `decoder=software` instead of `vaapi` | `/dev/dri` not mounted, or `intel-media-va-driver-non-free` missing | Verify `volumes`/`volumeMounts` in the rendered template; rebuild `vemcmt-decode` image |
| `detect_embed` falls back to CPU | iGPU device plugin or render-group access denied | Confirm `securityContext.supplementalGroups: [992]` is on the pod; check `ls -la /dev/dri/renderD128` inside the container |
| Correctness diff fails on real data | Detector seeding skipped, or randomness in OSNet ReID | Set `OV_DETERMINISTIC=1`; pin numpy/scipy seeds in the lib |
| ODAG stuck Running with one task pod Failed | Controller failed-state aggregation regressed | Check the fix #2 patch is in `cmd/odag-controller/main.go` `checkODAGCompletion` |
| `/push` 202s but transfer state never appears on disk | Stale data-agent image | Rebuild + roll DaemonSet; data-plane fix #1+#2 must be in effect |
