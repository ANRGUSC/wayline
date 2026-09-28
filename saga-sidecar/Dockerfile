# SAGA scheduler sidecar for the Wayline odag-controller.
#
# The bridge needs SAGA with capacity, demand, exact per-node runtimes and
# placement constraints in its core model (branch feature/capacity of
# ANRGUSC/saga, not yet on PyPI). Build a wheel of that branch into
# saga-sidecar/wheels/ first:
#   (cd <saga checkout on feature/capacity> && uv build --wheel -o <wayline>/saga-sidecar/wheels)
# then, for the x86 UP-board testbed:
#   docker build -t <registry>/wl-saga-sidecar:latest --platform linux/amd64 saga-sidecar/
FROM python:3.12-slim

COPY wheels /wheels
# anrg-saga pulls networkx/numpy/pydantic (+ z3 via its smt scheduler).
RUN pip install --no-cache-dir /wheels/anrg_saga-*.whl && rm -rf /wheels

WORKDIR /app
COPY bridge.py server.py /app/

EXPOSE 8090
ENTRYPOINT ["python", "server.py", "--port", "8090"]
