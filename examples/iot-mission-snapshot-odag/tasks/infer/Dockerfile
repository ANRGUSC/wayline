# Build from the repo root:
#   docker build -f examples/iot-mission-snapshot-odag/tasks/infer/Dockerfile \
#     -t <REGISTRY>/wl-iot-infer:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/iot-mission-snapshot-odag/tasks/infer/task.py .
CMD ["python", "task.py"]
