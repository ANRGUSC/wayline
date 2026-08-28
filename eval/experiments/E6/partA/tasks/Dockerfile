# Build from the repo root:
#   docker build -f eval/experiments/E6/partA/tasks/Dockerfile -t <REG>/wl-e6a:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY eval/experiments/E6/partA/tasks/task.py .
CMD ["python", "task.py"]
