# vemcmt-track: numpy + scipy for IoU-greedy + Hungarian. CPU.
FROM python:3.12-slim

RUN pip install --no-cache-dir numpy==2.1.* scipy==1.14.*

WORKDIR /app

COPY sdk/python/wl ./wl
COPY eval/mcmt/lib ./lib
COPY eval/mcmt/wayline/tasks/track_task.py   ./wl_track_task.py
COPY eval/mcmt/argo/tasks/track_task.py  ./argo_track_task.py

CMD ["python", "wl_track_task.py"]
