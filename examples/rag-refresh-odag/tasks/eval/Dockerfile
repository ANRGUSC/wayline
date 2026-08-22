# Build from the repo root:
#   docker build -f examples/rag-refresh-odag/tasks/eval/Dockerfile \
#     -t <REGISTRY>/wl-rag-eval:latest .
FROM python:3.11-slim
RUN pip install --no-cache-dir onnxruntime tokenizers numpy
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/rag-refresh-odag/tasks/embed/mlembed.py .
COPY examples/rag-refresh-odag/tasks/eval/task.py .
CMD ["python", "task.py"]
