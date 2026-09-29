#!/usr/bin/env python3
"""Split GDTM's released checkpoint into one weight file per stage (optimizer
state dropped): backbone.<modality>.pth, adapter.<modality>_<node>.pth and
fusion.pth (cross-attention, positional query, output head, tracker buffers).

Usage: split_ckpt.py <epoch_40.pth> <out-dir>
"""
import os
import sys

import torch


def split(src, dst):
    sd = torch.load(src, map_location="cpu")["state_dict"]
    groups = {}
    for k, v in sd.items():
        head, rest = k.split(".", 1)
        if head == "backbones":
            mod, rest = rest.split(".", 1)
            groups.setdefault(f"backbone.{mod}", {})[rest] = v
        elif head == "models":
            name, rest = rest.split(".", 1)
            groups.setdefault(f"adapter.{name}", {})[rest] = v
        else:
            groups.setdefault("fusion", {})[k] = v
    os.makedirs(dst, exist_ok=True)
    for g, d in groups.items():
        torch.save(d, f"{dst}/{g}.pth")
    return {g: len(d) for g, d in sorted(groups.items())}


if __name__ == "__main__":
    print(split(sys.argv[1], sys.argv[2]))
