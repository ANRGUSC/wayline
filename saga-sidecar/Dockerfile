# SAGA scheduler sidecar for the Wayline odag-controller.
# Build (from repo root, for the x86 UP-board testbed):
#   docker build -t <registry>/wl-saga-sidecar:latest --platform linux/amd64 saga-sidecar/
FROM python:3.12-slim

# anrg-saga pulls networkx/numpy/pydantic (+ z3 via its smt scheduler).
RUN pip install --no-cache-dir "anrg-saga>=2.1.0"

WORKDIR /app
COPY bridge.py server.py /app/

EXPOSE 8090
ENTRYPOINT ["python", "server.py", "--port", "8090"]
