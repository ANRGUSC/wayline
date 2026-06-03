# vemcmt-report: pure Python aggregation.
FROM python:3.12-slim

WORKDIR /app

COPY sdk/python/wl ./wl
COPY eval/mcmt/lib ./lib
COPY eval/mcmt/wayline/tasks/report_task.py   ./wl_report_task.py
COPY eval/mcmt/argo/tasks/report_task.py  ./argo_report_task.py

CMD ["python", "wl_report_task.py"]
