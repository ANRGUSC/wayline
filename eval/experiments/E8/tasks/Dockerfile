FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY eval/experiments/E8/tasks/task.py .
CMD ["python", "task.py"]
