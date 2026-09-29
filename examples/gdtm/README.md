# GDTM: a mixed container/function tracking pipeline

Indoor tracking of an RC car from three distributed sensor nodes, each with a
ZED stereo camera, a RealSense depth camera, a TI mmWave radar and a ReSpeaker
microphone array: the all-modalities early-fusion baseline of GDTM
(Srivastava group, UCLA; github.com/nesl/GDTM-tracking, MIT), with its
released checkpoint and a recorded segment of its sample test data.

```
source-n1 --> prep-{zed,depth,radar,audio}-n1 --> embed-{...}-n1 --\
source-n2 --> prep-{...}-n2                  --> embed-{...}-n2 ----> fuse --> track
source-n3 --> prep-{...}-n3                  --> embed-{...}-n3 --/
```

## Why it is mixed

Each stage's kind follows what the pipeline needs in a real deployment:

| stage | kind | why |
|---|---|---|
| source (per node) | container | the capture drivers: ZED SDK (CUDA), librealsense, TI mmWave tooling, ALSA for the mic array. Here the container replays the node's recording. |
| prep (per node and modality) | function | JPEG decode, resize and normalize; range-Doppler scaling; audio spectrogram. Plain numpy, OpenCV and torch. |
| embed (per node and modality) | container | GDTM's backbones and adapters, loaded from the released checkpoint through the repo's own code. That code needs mmcv-full 1.x (C++ ops compiled per torch build), the repo's forks of mmdetection 2.x and mmclassification 0.x, and torch 1.12; it cannot share an image with a current-torch runner. |
| fuse | container | the same stack: cross-attention fusion and the output head. |
| track | function | covariance calibration and the repo's Kalman filter (plain torch). |

Two images: `Dockerfile` (current Python stack, the replay data, the Kalman
filter) for sources and function stages, and `Dockerfile.model` (the pinned
stack, the repo at 62860de, the checkpoint split per stage) for the model
stages.

## Files

| file | image | what |
|---|---|---|
| `make_segment.py` | local | cuts a segment of the sample test split (default frames 600-899, 20 s at 15 fps) into small HDF5 files in the dataset's layout |
| `gdtm_glue.py` | glue | replay, the repo's mmdet preprocessing re-expressed with the same OpenCV/torch calls, the tracker |
| `gdtm_model.py` | model | `embed` and `fuse`, built with the calls `EarlyFusion.__init__` makes |
| `split_ckpt.py` | model | checkpoint to per-stage weight files, at image build |
| `stage.py` | both | one stage, by `GDTM_ROLE` |
| `reference.py` | model | the unsplit model through the repo's own data path, test loop and evaluation, plus the split model stages on the same inputs |
| `check_glue.py` | glue | the function stages against `reference.py`'s outputs |
| `gen_gdtm.py` | | the ODAGTemplate: `--mode mixed` (default), `pods`, `warm` |
| `runner.yml` | | warm runners `gdtm` (glue image) and `gdtm-model` (model image, weights preloaded) |

## Build and run

```bash
python3 examples/gdtm/make_segment.py <mcp-sample-dataset/test> examples/gdtm/segment
docker build -f examples/gdtm/Dockerfile.model -t <registry>/wl-gdtm-model:latest .
docker build -f examples/gdtm/Dockerfile -t <registry>/wl-gdtm:latest .
kubectl apply -f examples/gdtm/runner.yml
python3 examples/gdtm/gen_gdtm.py gdtm-mixed --frames 30 | kubectl apply -f -
wayline run gdtm-mixed -n wl-system
```

## Checked against the unsplit model

`reference.py` runs the released model unsplit, through the repo's own data
path, test loop and evaluation (on CPU), and the split model stages on the
same inputs: all twelve embeddings, the detections and covariances are
bit-exact. `check_glue.py` runs the function stages on the current stack:
camera, depth and radar preprocessing are bit-exact with the repo's mmcv
path; the audio spectrogram differs by at most 2e-8 and the Kalman track by
2e-6 cm (float rounding between torch versions). A whole Wayline run
reproduces the reference track to 2e-5 cm, with the model's 5.3 cm mean
tracking error against OptiTrack ground truth over a 30-frame window.
Results and timings: `eval/experiments/E15/PLAN.md`.
