# Build from the repo root:
#   docker build -f eval/experiments/E5/tasks/Dockerfile -t <REG>/wl-e5:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY eval/experiments/E5/tasks/task.py .
CMD ["python", "task.py"]
