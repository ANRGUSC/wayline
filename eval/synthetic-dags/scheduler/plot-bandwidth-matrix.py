#!/usr/bin/env python3
"""
Render the cluster bandwidth matrix as a heatmap.

Reads `wl-network-profile`-style ConfigMap YAML (key `anrg-X_to_anrg-Y`
with bytes/sec values) and writes a PNG.

Usage:
    ./plot-bandwidth-matrix.py <configmap.yml> <out.png>
    ./plot-bandwidth-matrix.py bandwidth-configmap.yml figures/bandwidth-v2.png

Generates two annotations per cell: the rate in Mbps and the class band
(F/M/S) for quick visual comparison across matrix versions.
"""
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import yaml

NODES = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]


def load_matrix(path: Path):
    cm = yaml.safe_load(path.read_text())
    data = cm.get("data", {})
    default_bps = float(data.get("defaultBandwidth", 125_000_000))
    n = len(NODES)
    mat = np.full((n, n), default_bps)
    for i in range(n):
        mat[i, i] = np.nan  # diagonal
    for k, v in data.items():
        if "_to_" not in k:
            continue
        src, dst = k.split("_to_", 1)
        if src not in NODES or dst not in NODES:
            continue
        mat[NODES.index(src), NODES.index(dst)] = float(v)
    return mat, default_bps


def class_map_from_matrix(mat_mbps):
    """Derive F/M/S class boundaries from the rates present in this matrix.
    The largest unique rate is F, smallest is S, middle is M. Works for
    either matrix version without hardcoding thresholds."""
    rates = sorted({round(v, 2) for v in mat_mbps[~np.isnan(mat_mbps)]})
    labels = {}
    if len(rates) >= 3:
        labels[rates[0]] = "S"
        labels[rates[-1]] = "F"
        for r in rates[1:-1]:
            labels[r] = "M"
    elif len(rates) == 2:
        labels[rates[0]] = "M"
        labels[rates[-1]] = "F"
    elif rates:
        labels[rates[0]] = "F"
    return labels


def band_label(mbps: float, label_map: dict) -> str:
    if np.isnan(mbps):
        return ""
    return label_map.get(round(mbps, 2), "")


def render(matrix_path: Path, out_path: Path, title: str):
    mat_bps, default_bps = load_matrix(matrix_path)
    mat_mbps = mat_bps / 125_000.0  # bytes/sec → Mbps (125_000 B/s = 1 Mbps)
    label_map = class_map_from_matrix(mat_mbps)

    fig, ax = plt.subplots(figsize=(8, 6.5))
    # Use log scale so bottleneck differences are visible.
    masked = np.ma.masked_invalid(mat_mbps)
    im = ax.imshow(masked, aspect="auto", cmap="RdYlGn",
                   norm=plt.matplotlib.colors.LogNorm(vmin=40, vmax=1100))

    # Annotations: "X Mbps\nF/M/S"
    for i in range(len(NODES)):
        for j in range(len(NODES)):
            if i == j:
                ax.text(j, i, "—", ha="center", va="center", color="#555", fontsize=9)
                continue
            v = mat_mbps[i, j]
            lbl = band_label(v, label_map)
            rate_str = f"{v:.0f}"
            # Dark background → white text; light → black.
            text_color = "white" if v < 300 else "black"
            ax.text(j, i, f"{rate_str}\n{lbl}", ha="center", va="center",
                    color=text_color, fontsize=8, linespacing=1.0)

    ax.set_xticks(range(len(NODES)))
    ax.set_xticklabels(NODES, rotation=30, ha="right")
    ax.set_yticks(range(len(NODES)))
    ax.set_yticklabels(NODES)
    ax.set_xlabel("destination")
    ax.set_ylabel("source")
    ax.set_title(f"{title}\n(values in Mbps; F=same-tier, M=cross-tier, S=bottleneck)")

    cbar = fig.colorbar(im, ax=ax, shrink=0.85, pad=0.02)
    cbar.set_label("bandwidth (Mbps, log scale)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    if len(sys.argv) >= 3:
        render(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[2]).stem)
        return

    # Default: render both archived matrices plus the live one.
    here = Path(__file__).resolve().parent
    jobs = [
        (here / "results/archive-matrix-v1/bandwidth-configmap.yml",
         here / "results/archive-matrix-v1/figures/bandwidth-matrix.png",
         "Bandwidth matrix v1 — F=1G / M=300M / S=100M"),
        (here / "results/archive-matrix-v2/bandwidth-configmap.yml",
         here / "results/archive-matrix-v2/figures/bandwidth-matrix.png",
         "Bandwidth matrix v2 — F=1G / M=100M / S=50M"),
    ]
    for cfg, out, title in jobs:
        if cfg.exists():
            render(cfg, out, title)
        else:
            print(f"SKIP missing: {cfg}")


if __name__ == "__main__":
    main()
