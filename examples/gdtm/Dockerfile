# GDTM's glue stages (function stages, plus the sensor-replay containers): a
# current Python stack with numpy, OpenCV and torch, the repo's Kalman filter,
# and a recorded segment of GDTM's sample test split. Build from the repo root
# after make_segment.py has written examples/gdtm/segment/:
#   docker build -f examples/gdtm/Dockerfile -t <registry>/wl-gdtm:latest .
FROM python:3.12-slim
ENV PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu \
 && pip install numpy opencv-python-headless h5py scipy matplotlib pyzmq
# The repo's Kalman filter (src/resource_constrained_tracking, MIT) at the
# commit the model image uses.
ARG GDTM_REV=62860dea524a9526e6436a3ddb5a8776a84e22d4
RUN mkdir -p /opt/rct && for f in tracker.py pubsub.py simulator.py; do \
      curl -sfL -o /opt/rct/$f https://raw.githubusercontent.com/nesl/GDTM-tracking/${GDTM_REV}/src/resource_constrained_tracking/$f; done
ENV PYTHONPATH=/opt/rct GDTM_SEGMENT=/opt/gdtm-segment PYTHONWARNINGS=ignore MPLBACKEND=Agg
COPY examples/gdtm/segment /opt/gdtm-segment
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/gdtm/gdtm_glue.py examples/gdtm/stage.py examples/gdtm/check_glue.py ./
RUN python -c "import gdtm_glue, tracker, cv2, h5py; print('gdtm glue ok')"
CMD ["python", "stage.py"]
