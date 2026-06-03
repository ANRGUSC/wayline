# Build from the repo root:
#   docker build -f eval/synthetic-dags/e1/tasks/Dockerfile \
#     -t 192.168.1.163:5000/e1-argo-tasks:latest .
#
# Single image bundling all 6 Argo-side task scripts. The workflow
# picks which to run via its command (e.g. ["python", "/app/capture.py"]).
FROM python:3.11-slim
WORKDIR /app
COPY eval/synthetic-dags/e1/tasks/capture.py    .
COPY eval/synthetic-dags/e1/tasks/preprocess.py .
COPY eval/synthetic-dags/e1/tasks/infer.py      .
COPY eval/synthetic-dags/e1/tasks/fuse.py       .
COPY eval/synthetic-dags/e1/tasks/report.py     .
COPY eval/synthetic-dags/e1/tasks/generic.py    .
# No external deps; everything uses the standard library.
