# Build from the repo root:
#   docker build -f examples/iot-mission-snapshot-odag/tasks/fuse/Dockerfile \
#     -t <REGISTRY>/wl-iot-fuse:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/iot-mission-snapshot-odag/tasks/fuse/task.py .
CMD ["python", "task.py"]
