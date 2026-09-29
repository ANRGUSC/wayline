# FOCAL on MOD as a Wayline pipeline: one image for container stages (pods)
# and function stages (warm runner calls). Build from the repo root:
#   docker build -f examples/focal-mod/Dockerfile -t <registry>/wl-focal:latest .
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
# torchvision from the same CPU index: timm needs it, and a default-index
# torchvision does not match the CPU torch build (torchvision::nms missing).
RUN pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir timm einops pyyaml numpy pyzmq
# FOCAL's model code (MIT), pinned.
RUN git clone -q https://github.com/tomoyoshki/focal.git /opt/focal && git -C /opt/focal checkout -q f6a989e
ENV FOCAL_SRC=/opt/focal/src PYTHONWARNINGS=ignore
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/focal-mod/focal_stages.py examples/focal-mod/stage.py ./
# Build the model once at image build, so a broken dependency fails here.
RUN python -c "import focal_stages as F; F.model(); print('focal model ok')"
CMD ["python", "stage.py"]
