FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq
COPY sdk/python/wl ./wl
COPY examples/heterogeneous-compute/tasks/task.py .
CMD ["python", "task.py"]
