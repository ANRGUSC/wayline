"""FOCAL (SW_Transformer on MOD) split into pipeline stages.

The model is FOCAL's own code (github.com/tomoyoshki/focal, commit f6a989e),
built from its MOD config. Stages follow its forward pass:

  source(mod)  a window batch of raw samples           [b, 1, 10, s] float32
  fft(mod)     fft_preprocess for one modality          [b, 2, 10, s]
  encode(mod)  patch embed + Swin layers + projection   [b, 1, 256]
  fuse_head    modality fusion + classifier             [b, num_classes]

MOD has one sensor location, so the model's location fusion is the identity.
Weights are fixed random (seed 0): no pretrained weights are published, and
compute cost and data sizes do not depend on the values. Inputs are synthetic
signals at MOD's rates and shapes (seismic 100 Hz, audio 8 kHz, 2 s windows
in 10 segments), since the dataset is only on Box.
"""
import os
import sys
import types
from argparse import Namespace

import numpy as np
import torch
import yaml

FOCAL_SRC = os.environ.get("FOCAL_SRC", "/opt/focal/src")
sys.path.insert(0, FOCAL_SRC)
# FusionModules imports an unused matplotlib symbol; stub it.
_mpl = types.ModuleType("matplotlib"); _plt = types.ModuleType("matplotlib.pyplot")
_plt.axis = None; _mpl.pyplot = _plt
sys.modules.setdefault("matplotlib", _mpl); sys.modules.setdefault("matplotlib.pyplot", _plt)

from models.SW_Transformer import SW_Transformer  # noqa: E402

LOC = "shake"
MODS = ("seismic", "audio")
TASK = "vehicle_classification"
torch.set_num_threads(int(os.environ.get("FOCAL_THREADS", "1")))


def dataset_config():
    with open(os.path.join(FOCAL_SRC, "data", "MOD.yaml")) as f:
        return yaml.safe_load(f)


_model = None


def model():
    """The FOCAL model, built once per process with fixed weights."""
    global _model
    if _model is None:
        torch.manual_seed(0)
        args = Namespace(dataset_config=dataset_config(), task=TASK, train_mode="supervised")
        _model = SW_Transformer(args).eval()
    return _model


def spectrum_len(mod):
    return dataset_config()["loc_mod_spectrum_len"][LOC][mod]


def source(mod, batch, seed):
    """A batch of 2 s windows: a vehicle-like tone sweep plus noise, in the
    time domain, shaped [b, 1, 10 segments, samples per segment]."""
    rng = np.random.default_rng(seed)
    s = spectrum_len(mod)
    rate = s * 5                              # 10 segments per 2 s window
    t = np.arange(10 * s) / rate
    f0 = rng.uniform(5, 40 if mod == "seismic" else 400, size=(batch, 1))
    x = np.sin(2 * np.pi * f0 * t * (1 + 0.1 * t)) + 0.3 * rng.standard_normal((batch, 10 * s))
    return x.reshape(batch, 1, 10, s).astype(np.float32)


def fft(x):
    """fft_preprocess (input_utils/time_input_utils.py) for one modality."""
    f = torch.view_as_real(torch.fft.fft(torch.from_numpy(x), dim=-1))
    f = f.permute(0, 1, 4, 2, 3)
    b, c1, c2, i, s = f.shape
    return f.reshape(b, c1 * c2, i, s).contiguous().numpy()


@torch.no_grad()
def encode(mod, freq):
    """Step 1 of forward_encoder for one modality: [b, 1, 256]."""
    m = model()
    freq_input, _ = m.pad_input({LOC: {mod: torch.from_numpy(freq)}}, LOC, mod)
    x = m.patch_embed[LOC][mod](freq_input)
    for layer in m.freq_interval_layers[LOC][mod]:
        x = layer(x)
    b = x.shape[0]
    return m.mod_in_layers[LOC][mod](x.reshape([b, -1])).reshape(b, 1, -1).numpy()


@torch.no_grad()
def fuse_head(feats):
    """Steps 2-3 of forward_encoder: modality fusion and the class layer."""
    m = model()
    mod_features = [torch.from_numpy(feats[mod]).flatten(start_dim=1) for mod in MODS]
    x = torch.stack(mod_features, dim=1).unsqueeze(dim=1)
    return m.class_layer(m.mod_fusion_layers(x).flatten(start_dim=1)).numpy()


@torch.no_grad()
def whole(freqs):
    """The unsplit model, for checking that the stages compose to it."""
    return model()({LOC: {mod: torch.from_numpy(freqs[mod]) for mod in MODS}}).numpy()


if os.environ.get("FOCAL_PRELOAD") == "1":
    model()   # warm runner: build before the zygote forks, so calls share it
