# Build from the repo root:
#   docker build -f eval/experiments/E4/tasks/Dockerfile -t <REG>/wl-e4:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY eval/experiments/E4/tasks/task.py .
CMD ["python", "task.py"]
