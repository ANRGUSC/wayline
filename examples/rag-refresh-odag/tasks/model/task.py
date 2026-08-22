#!/usr/bin/env python3
"""
RAG KB Refresh — prepare-model task.

Materializes the embedding model (ONNX MiniLM-L6-v2 + tokenizer) as a
Wayline data object. The template declares a cacheKey for this task, so
after the first run the controller binds it to the previous run's
installed copy and the model is never re-materialized or re-sent from
scratch: warm runs alias the existing bytes and the scheduler places
consumers near them.
"""

import io
import os
import tarfile
import time

from wl import WlTask

task = WlTask()
print(f"[{task.name}] node={task.node}", flush=True)

t0 = time.perf_counter()
buf = io.BytesIO()
with tarfile.open(fileobj=buf, mode="w") as tar:
    for fname in ("model.onnx", "tokenizer.json"):
        path = os.path.join("/model", fname)
        tar.add(path, arcname=fname)
blob = buf.getvalue()
print(f"[{task.name}] packed model blob: {len(blob)} bytes "
      f"in {time.perf_counter() - t0:.2f}s", flush=True)

t1 = time.perf_counter()
task.send_raw(blob)
print(f"[{task.name}] send_raw() completed in {time.perf_counter() - t1:.2f}s", flush=True)
task.close()
