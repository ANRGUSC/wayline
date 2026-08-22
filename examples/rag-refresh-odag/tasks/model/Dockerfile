# prepare-model task image: bakes the ONNX MiniLM-L6-v2 embedding model.
# The model reaches embed/eval tasks as a Wayline data object, not via
# their images — that is the point of the benchmark.
# Build from the repo root:
#   docker build -f examples/rag-refresh-odag/tasks/model/Dockerfile \
#     -t <REGISTRY>/wl-rag-model:latest .
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /model
RUN curl -fsSL -o model.onnx \
      https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx \
 && curl -fsSL -o tokenizer.json \
      https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main/tokenizer.json \
 && ls -la
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/rag-refresh-odag/tasks/model/task.py .
CMD ["python", "task.py"]
