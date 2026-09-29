"""GDTM's early-fusion tracker (github.com/nesl/GDTM-tracking, master at
62860de, config early_fusion_zed_mmwave_audio_ucla), split along the model's
own forward pass. Runs in the model image: the repo's code on its pinned stack
(torch 1.12, mmcv-full 1.7, the repo's mmdetection and mmclassification).

  embed(mod, node, x)  one modality's backbone and that node's adapter
                       (EarlyFusion._forward_single, one key)
  fuse(embeds)         cross-attention over all twelve embeddings and the
                       output head (the rest of EarlyFusion.forward_track)

Every stage builds its modules with the calls EarlyFusion.__init__ uses and
loads its slice of the released checkpoint (split at image build time by
split_ckpt.py). reference.py checks the stages against the unsplit model.

GDTM_PRELOAD=1 (the warm runner) builds every module at import, before the
runner's zygote forks; each backbone is built once and shared by its nodes.
"""
import copy
import json
import os

import numpy as np
import torch

from mmcv import Config
from mmdet.models import build_backbone
from mmtrack.models import build_model

GDTM = os.environ.get("GDTM_SRC", "/opt/gdtm")
CFG = f"{GDTM}/configs/mocap/early_fusion_zed_mmwave_audio_ucla.py"
WEIGHTS = os.environ.get("GDTM_WEIGHTS", "/opt/gdtm-weights")
MODS = ["range_doppler", "realsense_camera_depth", "mic_waveform", "zed_camera_left"]
NODES = ["node_1", "node_2", "node_3"]
# The order EarlyFusion concatenates embeddings in: the dataset's key order
# (HDF5 files in config order, then datasets in file order). reference.py
# asserts it against the repo's own data path.
ORDER = [(m, n) for n in NODES for m in MODS]


def config():
    return Config.fromfile(CFG)


def full_state_dict():
    """The checkpoint's state_dict, reassembled from the stage files."""
    sd = {}
    for f in sorted(os.listdir(WEIGHTS)):
        if not f.endswith(".pth"):
            continue
        kind, name = f[:-4].split(".", 1) if "." in f[:-4] else (f[:-4], "")
        part = torch.load(f"{WEIGHTS}/{f}", map_location="cpu")
        prefix = {"backbone": f"backbones.{name}.", "adapter": f"models.{name}.", "fusion": ""}[kind]
        sd.update({prefix + k: v for k, v in part.items()})
    return sd


def calibration(res_json, metric="nll"):
    """Covariance scaling (a, b) per detector, chosen from the validation grid
    search exactly as HDF5Dataset.calibrate_outputs does."""
    with open(res_json) as f:
        data = json.load(f)
    best = {"det_result_%d" % (i + 1): (None, 1e20) for i in range(3)}
    for a_b, res1 in data.items():
        if a_b == "uncalibrated":
            continue
        for det, res2 in res1.items():
            if det == "track_result":
                continue
            vals = -np.array(res2["nll_vals"] if metric == "nll" else res2["grid_scores"])
            score = np.mean(vals)
            if best[det][1] > score:
                best[det] = (a_b, score)
    return {int(k.split("_")[-1]) - 1: tuple(float(x) for x in v[0].split("_"))
            for k, v in best.items() if v[0] is not None}


_BACKBONES, _ADAPTERS, _FUSION = {}, {}, []


def load_embedder(mod, node):
    m = config().model
    if mod not in _BACKBONES:
        backbone = build_backbone(m.backbone_cfgs[mod])
        backbone.load_state_dict(torch.load(f"{WEIGHTS}/backbone.{mod}.pth", map_location="cpu"))
        _BACKBONES[mod] = backbone.eval()
    if (mod, node) not in _ADAPTERS:
        adapter = build_model(m.model_cfgs[(mod, node)])
        adapter.load_state_dict(torch.load(f"{WEIGHTS}/adapter.{mod}_{node}.pth", map_location="cpu"))
        _ADAPTERS[(mod, node)] = adapter.eval()
    return _BACKBONES[mod], _ADAPTERS[(mod, node)]


def embed(mod, node, x, modules=None):
    """x: float32 [frames, C, H, W], the dataset pipeline's output for one
    (modality, node). Returns float32 [frames, L, 512], one frame at a time
    as the repo's test loop runs it."""
    backbone, adapter = modules or load_embedder(mod, node)
    out = []
    with torch.no_grad():
        for i in range(len(x)):
            img = torch.from_numpy(np.ascontiguousarray(x[i:i + 1]))
            try:
                feats = backbone(img)
            except Exception:
                feats = backbone([img])
            out.append(adapter(feats[0]))
    return torch.cat(out).numpy()


def load_fusion():
    """EarlyFusion without backbones or adapters: the repo's own constructor
    builds the cross-attention, positional query and output head."""
    if not _FUSION:
        m = copy.deepcopy(config().model)
        m.backbone_cfgs, m.model_cfgs = {}, {}
        model = build_model(m)
        sd = torch.load(f"{WEIGHTS}/fusion.pth", map_location="cpu")
        missing, unexpected = model.load_state_dict(sd, strict=False)
        if missing or unexpected:
            raise RuntimeError(f"fusion weights mismatch: missing {missing} unexpected {unexpected}")
        _FUSION.append(model.eval())
    return _FUSION[0]


def fuse(embeds, model=None):
    """embeds: {(mod, node): [frames, L, 512]}. Returns per-frame position
    means [frames, 2] and covariances [frames, 2, 2] (cm), the values
    forward_track returns as det_means / det_covs."""
    model = model or load_fusion()
    frames = len(next(iter(embeds.values())))
    means, covs = [], []
    with torch.no_grad():
        for i in range(frames):
            det_embeds = torch.cat([torch.from_numpy(embeds[k][i:i + 1]) for k in ORDER], dim=1)
            final_embed = model.global_pos_encoding.weight
            for layer in model.global_cross_attn:
                final_embed = layer(final_embed, det_embeds)
            dist = model.output_head(final_embed.unsqueeze(0))["dist"]
            means.append(dist.loc.squeeze(0).reshape(-1))
            covs.append(dist.covariance_matrix.squeeze(0).reshape(2, 2))
    return torch.stack(means).numpy(), torch.stack(covs).numpy()


if os.environ.get("GDTM_PRELOAD") == "1":
    for _k in ORDER:
        load_embedder(*_k)
    load_fusion()
