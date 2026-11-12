"""Image tags E18 uses, by set; E18_IMAGES picks the set (default mx1).

  mx1  linux/amd64 + linux/arm64 builds of 2026-11-08 (build.sh light, ray,
       focal; dask mirrored) for every image that has one; the rest keep
       their E16 tags (GDTM's images and the Ray / Argo images built on GDTM's
       and FOCAL's, all amd64-only).
  e16  the E16-era tags, every one amd64-only (wl-iot-* of 2026-08-21, wl-e11
       of 2026-09-25, wl-ray-baseline:2.58.0, ...).

The mx1 amd64 variants are rebuilds from the same Dockerfiles and the
current sdk/python/wl, not E16's bytes: Python 3.12.16 / 3.11.18 (E16:
3.12.14 / 3.11.16); wl-focal pins E16's torch 2.14.0, torchvision 0.29.0,
timm 1.0.30, numpy 2.5.2. `E18_IMAGES=e16 ./run.sh ...` runs E16's images
(UP-board lanes only).
"""
import os

import kube

E16 = {
    "wl-e11": "latest", "wl-runner-e11": "latest", "wl-runner-iot": "latest",
    "wl-iot-capture": "latest", "wl-iot-preprocess": "latest", "wl-iot-infer": "latest",
    "wl-iot-fuse": "latest", "wl-iot-report": "latest",
    "wl-e16-task": "latest", "wl-ray-baseline": "2.58.0",
    "wl-gdtm": "latest", "wl-gdtm-model": "latest", "wl-focal": "latest",
    "wl-gdtm-ray": "latest", "wl-focal-ray": "latest",
    "wl-gdtm-argo": "latest", "wl-gdtm-model-argo": "latest", "wl-focal-argo": "latest",
    "rclone": "e16", "dask": "2026.8.0-py3.12",
    # The Dask arm's image (dask/image.sh): python:3.12-slim + dask[distributed], amd64 + arm64, in
    # both sets. Not "dask" (ghcr.io/dask/dask mirrored): its conda CPython/OpenSSL hash E11's loop
    # ~30% faster than the python:3.12-slim of the Ray, Argo and Wayline task images.
    "wl-dask-baseline": "2026.8.0",
}
MULTI_ARCH = ("wl-e11", "wl-runner-e11", "wl-runner-iot", "wl-iot-capture", "wl-iot-preprocess",
              "wl-iot-infer", "wl-iot-fuse", "wl-iot-report", "wl-e16-task", "wl-focal")
MX1 = dict(E16, **{r: "mx1" for r in MULTI_ARCH}, **{"wl-ray-baseline": "2.58.0-mx1"})
SETS = {"mx1": MX1, "e16": E16}

SET = os.environ.get("E18_IMAGES", "mx1")
if SET not in SETS:
    raise SystemExit(f"E18_IMAGES={SET!r}: one of {sorted(SETS)}")
TAGS = SETS[SET]


def repo_tag(repo):
    """'wl-e11:mx1': the repository with this set's tag."""
    return f"{repo}:{TAGS[repo]}"


def ref(repo):
    """'10.10.0.1:5000/wl-e11:mx1'."""
    return f"{kube.REG}/{repo_tag(repo)}"


def retag(image):
    """An image in our registry (any tag) with this set's tag for its
    repository; anything else unchanged (iot-base.json names :latest)."""
    if not image.startswith(kube.REG + "/"):
        return image
    repo = image[len(kube.REG) + 1:].rsplit(":", 1)[0]
    return ref(repo) if repo in TAGS else image
