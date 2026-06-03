# Build from the repo root:
#   docker build -f examples/iobt-mission-snapshot-odag/tasks/report/Dockerfile \
#     -t <REGISTRY>/wl-iobt-report:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/iobt-mission-snapshot-odag/tasks/report/task.py .
CMD ["python", "task.py"]
