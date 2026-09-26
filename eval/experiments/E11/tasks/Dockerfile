# Build from the repo root:
#   docker build -f eval/experiments/E11/tasks/Dockerfile -t 192.168.1.163:5000/wl-e11:latest .
# Python 3.12, as in the Ray image, so both runtimes hash at the same rate.
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq
COPY sdk/python/wl ./wl
COPY eval/experiments/E11/tasks/task.py .
CMD ["python", "task.py"]
