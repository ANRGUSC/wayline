# FOCAL on MOD: a mixed function/container pipeline

Vehicle classification from seismic (100 Hz) and acoustic (8 kHz) sensing
with FOCAL's SW_Transformer (Liu et al., github.com/tomoyoshki/focal, MIT),
split along the model's own forward pass into Wayline stages:

```
source-seismic -> fft-seismic -> encoder-seismic --\
                                                     fuse-head
source-audio   -> fft-audio   -> encoder-audio   --/
```

| stage | work | per 64 two-second windows | kind (mixed) |
|---|---|---|---|
| source-* | read a window batch (pinned to its sensor node) | 0.05 / 4.1 MB out | function |
| fft-* | FOCAL's `fft_preprocess` | ms | function |
| encoder-* | patch embed, Swin layers, projection | 0.5-0.8 s on an i3-N305 | container |
| fuse-head | modality fusion and classifier | ms | function |

`focal_stages.py` builds the model from FOCAL's MOD config and splits it; the
stages compose exactly to the unsplit model. Weights are fixed random (seed
0; no pretrained weights are published) and inputs are synthetic signals at
MOD's rates and shapes (the dataset is on Box): compute and data volume are
the real model's, predictions are meaningless.

```bash
docker build -f examples/focal-mod/Dockerfile -t <registry>/wl-focal:latest .   # from the repo root
kubectl apply -f examples/focal-mod/runner.yml                                 # warm runner for functions
python3 examples/focal-mod/gen_focal.py focal-mixed --mode mixed | kubectl apply -f -
wayline run focal-mixed -n wl-system
```

`--mode pods` runs every stage as a pod, `--mode warm` every stage as a function.
