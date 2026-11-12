#!/usr/bin/env bash
# The Dask baseline's image, wl-dask-baseline:<dask version>, for linux/amd64 and linux/arm64,
# assembled on eclipse without a build: python:3.12-slim plus one layer holding
# dask[distributed] and numpy, installed from manylinux wheels for each architecture.
#
#   eval/experiments/E18/dask/image.sh            # never overwrites an existing tag (FORCE=1 does)
#
# Why not the upstream ghcr.io/dask/dask image: it is conda's CPython and OpenSSL, which run
# E11's SHA-256 loop (the chain workloads' compute) about 30% faster than python:3.12-slim
# (3.28 against 2.52 to 2.61 Mhash/s on anrg-3). The Ray image, the Argo task image and
# Wayline's E11 tasks are all python:3.12-slim, so the Dask arm must be too, or it wins the
# chains on its interpreter build.
#
# Why no build: eclipse's Docker cannot run RUN steps (its default runtime is nvidia, whose
# binary is missing) and has no QEMU for arm64. pip fetches the target platform's wheels
# (every dependency of dask[distributed] publishes x86_64 and aarch64 manylinux wheels), and
# crane appends them as a layer to the matching python:3.12-slim and joins the two images.
# The image has no `dask` console script; run `python -m dask scheduler|worker`.
set -euo pipefail
REG=${REG:-10.10.0.1:5000}
DASK=${DASK:-2026.8.0}
BASE=${BASE:-docker.io/library/python:3.12-slim}
CRANE=${CRANE:-$HOME/testbed/bin/crane}
PIP=${PIP:-pip3}
WORK=${WORK:-$HOME/testbed/build/wl-dask-baseline}
REF=$REG/wl-dask-baseline:$DASK

if "$CRANE" digest --insecure "$REF" >/dev/null 2>&1 && [ -z "${FORCE:-}" ]; then
  echo "skip $REF (exists)"; exit 0
fi
base_digest=$("$CRANE" digest "$BASE")
rm -rf "$WORK"; mkdir -p "$WORK"
for pair in x86_64:amd64 aarch64:arm64; do
  pa=${pair%%:*}; ka=${pair##*:}
  root=$WORK/$ka
  site=$root/usr/local/lib/python3.12/site-packages
  mkdir -p "$site"
  "$PIP" install --quiet --no-compile --disable-pip-version-check --target "$site" \
    --implementation cp --python-version 3.12 --abi cp312 --only-binary=:all: \
    --platform "manylinux_2_28_$pa" --platform "manylinux_2_17_$pa" --platform "manylinux2014_$pa" \
    "dask[distributed]==$DASK" numpy
  rm -rf "$site/bin"   # console scripts with eclipse's interpreter in the shebang
  tar --owner=0 --group=0 --numeric-owner -C "$root" -czf "$WORK/$ka.tar.gz" usr
  "$CRANE" append --insecure --platform "linux/$ka" -b "${BASE%:*}@$base_digest" -f "$WORK/$ka.tar.gz" -t "$REF-$ka" >/dev/null
  echo "pushed $REF-$ka ($(du -h "$WORK/$ka.tar.gz" | cut -f1) layer on $BASE@$base_digest)"
done
"$CRANE" index append --insecure -m "$REF-amd64" -m "$REF-arm64" -t "$REF" >/dev/null
echo "built $REF (linux/amd64, linux/arm64)"
