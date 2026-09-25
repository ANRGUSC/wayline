# Ray runtime for the E10 baseline: head and workers run this image as
# hostNetwork pods, one per node, so Ray uses the hosts' network like the
# Wayline data agents do.
FROM python:3.12-slim
RUN pip install --no-cache-dir "ray[default]==2.58.0" numpy
WORKDIR /e10
COPY e10_ray.py /e10/e10_ray.py
