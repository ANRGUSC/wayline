#!/usr/bin/env python3
"""Named-outputs demo: one producer, three semantically distinct
objects with different consumers. Task role selected by WL_TASK_NAME."""
import hashlib
import os

from wl import WlTask

task = WlTask()
name = task.name

if name == "produce":
    alert = b"ALERT:threshold-exceeded:" + os.urandom(1000)
    features = os.urandom(50_000_000)
    print(f"[produce] emitting alert ({len(alert)}B) and "
          f"features ({len(features)}B)", flush=True)
    task.send_raw("alert", alert)
    task.send_raw("features", features)
elif name == "actuator":
    data = task.recv_raw(peer="produce.alert")
    print(f"[actuator] alert received: {len(data)}B "
          f"sha={hashlib.sha256(data).hexdigest()[:12]}", flush=True)
    task.send_raw(b"actuated")
elif name == "analyzer":
    data = task.recv_raw(peer="produce.features")
    print(f"[analyzer] features received: {len(data)}B "
          f"sha={hashlib.sha256(data).hexdigest()[:12]}", flush=True)
    task.send_raw(b"analyzed")
elif name == "report":
    a = task.recv_raw(peer="actuator")
    b = task.recv_raw(peer="analyzer")
    print(f"[report] {a.decode()} + {b.decode()}", flush=True)
    task.send_raw(b"done")
print(f"[{name}] done", flush=True)
task.close()
