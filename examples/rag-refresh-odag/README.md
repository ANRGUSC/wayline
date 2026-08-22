# RAG Knowledge Base Refresh + QA Regression (ODAGTemplate)

A periodic RAG (Retrieval-Augmented Generation) knowledge base refresh
pipeline with a real embedding model: materialize the model, ingest
documents, chunk, embed in parallel shards (ONNX MiniLM-L6-v2, D=384),
build per-shard indices, merge into a global index, run an evaluation
query suite, and produce a QA regression report.

Two properties make this the LLM-workflow benchmark for the paper:

* **The model is a data object.** `prepare-model` materializes the
  ~90MB ONNX model + tokenizer as a Wayline object that fans out to
  five consumers (4 embed shards + eval). How those bytes move is a
  per-object realization choice.
* **The model is reused across runs.** `prepare-model` declares
  `cacheKey: rag-minilm-l6-v2-onnx-v1`; after run 1 the controller
  binds it to the previous run's installed copy (alias, no pod), and
  the scheduler places embed/eval near the cached bytes through its
  ordinary cost model.

Uses `ODAGTemplate` for profiling, retention (`keepLatest` — required
for cross-run reuse), and repeated runs.

## DAG Structure (19 tasks)

```
                         /--> chunk-shard-1 --> embed-shard-1 --> build-index-1 --\
ingest-shard --+--> chunk-shard-2 --> embed-shard-2 --> build-index-2 ---+--> merge-index --> eval-queries --> report
                         \--> chunk-shard-3 --> embed-shard-3 --> build-index-3 --/
                          \-> chunk-shard-4 --> embed-shard-4 --> build-index-4 -/
```

| Stage | Tasks | Description | Data | Placement |
|-------|-------|-------------|------|-----------|
| Ingest | ingest-shard | Generate synthetic corpus, pack 4 shards | ~20 MB blob | Pinned: data-local node |
| Chunk | chunk-shard-1..4 | Extract shard, split into 500-char passages | ~6 MB each | Co-located with ingest |
| Embed | embed-shard-1..4 | Feature-hash embeddings (D=128, L2-norm) | ~5 MB each | Constrained: embedding-capable nodes |
| Index | build-index-1..4 | Add coarse bucket inverted index | ~6 MB each | Unconstrained (scheduler picks) |
| Merge | merge-index | Fan-in: concatenate 4 shard indices | ~22 MB | Near inference tier |
| Eval | eval-queries | Brute-force cosine retrieval, latency stats | ~1 MB JSON | Near merge |
| Report | report | Formatted QA regression report | terminal | Gateway node |

## Key Properties

- **Data locality**: ingest and chunk tasks co-located on the same node (zero-hop shard extraction)
- **Feasible-node constraints**: embed tasks restricted to "embedding-capable" node subset
- **Fan-out / fan-in**: ingest fans out to 4 chunk pipelines; merge fans in from 4 indices
- **Real embeddings**: ONNX MiniLM-L6-v2 via onnxruntime CPU (D=384), tokenizer + mean-pooling + L2-norm; deterministic for a fixed corpus
- **Non-trivial artifacts**: binary embedding blobs (header JSON + packed float32 arrays) flow through index/merge/eval

## Quick Start

### 1. Generate template with real node names

```bash
cd examples/rag-refresh-odag
python gen_odag.py --registry 192.168.1.163:5000
```

Or manually edit `template.yml` and replace node names.

### 2. Build and push images

From the **repo root**:

```bash
REGISTRY=192.168.1.163:5000

examples/rag-refresh-odag/build.sh   # builds + pushes all 8 images
```

### 3. Apply the ODAGTemplate

```bash
kubectl apply -f examples/rag-refresh-odag/template.yml
```

### 4. Trigger a run

```bash
wayline run rag-refresh -n wl-system
```

### 5. Monitor

```bash
wayline status rag-refresh-run-001 -n wl-system
wayline logs rag-refresh-run-001 merge-index -n wl-system
wayline logs rag-refresh-run-001 report -n wl-system
```

Or view in the UI at `http://localhost:8080`.

### 6. Re-run (with profiling)

```bash
wayline run rag-refresh -n wl-system   # creates run-002, run-003, ...
```

Each run records profiled runtimes/data sizes. After `minSamples` (2) runs, HEFT
scheduling uses profiled values instead of YAML hints.

## What to Look At

- **Makespan**: total end-to-end time. The critical path is ingest → chunk → embed → index → merge → eval → report. The 4-way parallelism in chunk/embed/index should keep the makespan close to the single-shard critical path.
- **Bytes moved at fan-in**: `merge-index` logs total bytes received from the 4 index shards. This is the bottleneck transfer.
- **Eval latency**: `eval-queries` logs per-query latency and p50/p95. With ~20K vectors and brute-force search, expect 1-3s per query in pure Python.
- **Network shaping**: apply `tc netem` to throttle links between tiers and observe makespan changes (the embed→index→merge cross-node transfers are most affected).

## Embedding Method

Uses **feature hashing** (no ML dependencies):
1. Tokenize by whitespace, lowercase
2. For each token: `h = SHA-256(token)`, `idx = h[:4] mod D`, `sign = ±1 from h[4]`
3. Accumulate into D-dimensional vector
4. L2-normalize

This produces meaningful similarity: documents sharing vocabulary cluster together.
Swap in a real embedding model (sentence-transformers, etc.) by replacing `embed/task.py`.
