# Build from the repo root:
#   docker build -f examples/iobt-mission-snapshot-odag/tasks/preprocess/Dockerfile \
#     -t <REGISTRY>/wl-iobt-preprocess:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/iobt-mission-snapshot-odag/tasks/preprocess/task.py .
CMD ["python", "task.py"]
