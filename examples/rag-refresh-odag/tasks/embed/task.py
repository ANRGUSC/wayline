#!/usr/bin/env python3
"""
RAG KB Refresh — embed-shard-{1..4} task.

Receives the embedding model (from prepare-model) and the chunks JSONL
(from its chunk-shard), runs real ONNX MiniLM-L6-v2 inference, and
sends an embedding blob (D=384) to its build-index task.

Embedding blob format:
  [4B header_json_len][header JSON][N * D * 4 bytes float32 vectors]
"""

import json
import struct
import tempfile
import time

from wl import WlTask
from mlembed import Embedder, unpack_model

task = WlTask()
shard = task.name.rsplit("-", 1)[1]
print(f"[{task.name}] node={task.node} shard={shard}", flush=True)

t0 = time.perf_counter()
model_blob = task.recv_raw(peer="prepare-model")
print(f"[{task.name}] recv model: {len(model_blob)} bytes "
      f"in {time.perf_counter() - t0:.2f}s", flush=True)

t1 = time.perf_counter()
chunks_raw = task.recv_raw(peer=f"chunk-shard-{shard}")
print(f"[{task.name}] recv chunks: {len(chunks_raw)} bytes "
      f"in {time.perf_counter() - t1:.2f}s", flush=True)

t2 = time.perf_counter()
model_dir = tempfile.mkdtemp()
unpack_model(model_blob, model_dir)
emb = Embedder(model_dir)
print(f"[{task.name}] model loaded in {time.perf_counter() - t2:.2f}s", flush=True)

chunk_ids, texts = [], []
for line in chunks_raw.decode().strip().split("\n"):
    if not line:
        continue
    chunk = json.loads(line)
    chunk_ids.append(chunk["chunk_id"])
    texts.append(chunk["text"])

t3 = time.perf_counter()
vectors = emb.embed(texts)
elapsed = time.perf_counter() - t3
print(f"[{task.name}] embedded {len(texts)} chunks x {emb.dim}d "
      f"in {elapsed:.2f}s ({len(texts)/max(elapsed,1e-9):.0f}/s)", flush=True)

header = json.dumps({
    "num_vectors": len(chunk_ids),
    "dim": emb.dim,
    "chunk_ids": chunk_ids,
}).encode()
blob = struct.pack(">I", len(header)) + header + vectors.tobytes()
print(f"[{task.name}] blob: {len(blob)} bytes", flush=True)

t4 = time.perf_counter()
task.send_raw(blob)
print(f"[{task.name}] send_raw() completed in {time.perf_counter() - t4:.2f}s", flush=True)
print(f"[{task.name}] done", flush=True)
task.close()
