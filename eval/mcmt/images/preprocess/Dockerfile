# vemcmt-preprocess: OpenCV resize + letterbox. CPU only.
FROM python:3.12-slim

RUN pip install --no-cache-dir opencv-python-headless==4.10.* numpy==2.1.*

WORKDIR /app

COPY sdk/python/wl ./wl
COPY eval/mcmt/lib ./lib
COPY eval/mcmt/wayline/tasks/preprocess_task.py   ./wl_preprocess_task.py
COPY eval/mcmt/argo/tasks/preprocess_task.py  ./argo_preprocess_task.py

CMD ["python", "wl_preprocess_task.py"]
