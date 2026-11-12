FROM python:3.12-slim
RUN pip install --no-cache-dir numpy
COPY e16_ray_body.py task.py /app/
WORKDIR /app
ENTRYPOINT ["python", "/app/task.py"]
