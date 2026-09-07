# Build from repo root:
#   docker build -f eval/experiments/E7/tasks/Dockerfile -t <REG>/wl-e7:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY eval/experiments/E7/tasks/task.py .
CMD ["python", "task.py"]
