# Build from the repo root:
#   docker build -f examples/named-outputs/tasks/Dockerfile \
#     -t <REGISTRY>/wl-named-outputs:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/named-outputs/tasks/task.py .
CMD ["python", "task.py"]
