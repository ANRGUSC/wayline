# vemcmt-cross-camera-match: numpy + scipy for Hungarian + union-find. CPU.
FROM python:3.12-slim

RUN pip install --no-cache-dir numpy==2.1.* scipy==1.14.*

WORKDIR /app

COPY sdk/python/wl ./wl
COPY eval/mcmt/lib ./lib
COPY eval/mcmt/wayline/tasks/cross_camera_match_task.py   ./wl_cross_camera_match_task.py
COPY eval/mcmt/argo/tasks/cross_camera_match_task.py  ./argo_cross_camera_match_task.py

CMD ["python", "wl_cross_camera_match_task.py"]
