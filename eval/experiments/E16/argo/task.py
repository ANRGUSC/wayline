#!/usr/bin/env python3
"""E16 Argo task: one iot or chain stage with the same body as the Wayline
and Ray versions (../ray/e16_ray.py). Inputs are files Argo downloaded from
the artifact repository into /in; the output goes to /out/data for Argo to
upload.

  python task.py <name> <role> <runtime_s> <size_bytes>
"""
import glob
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, "/app")
from e16_ray_body import body  # noqa: E402

name, role, runtime, size = sys.argv[1], sys.argv[2], float(sys.argv[3]), int(sys.argv[4])
inputs = [np.fromfile(p, dtype=np.uint8) for p in sorted(glob.glob("/in/*"))]
start = time.time()
out = body(name, role, runtime, size, inputs)
if out.nbytes == 0:
    out = np.zeros(1, dtype=np.uint8)  # the S3 repository rejects empty uploads
os.makedirs("/out", exist_ok=True)
out.tofile("/out/data")
print(json.dumps({"task": name, "node": os.environ.get("NODE_NAME", ""), "start": start, "end": time.time()}))
