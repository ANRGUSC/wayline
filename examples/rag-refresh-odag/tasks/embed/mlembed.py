"""Shared ONNX embedding helper for the RAG tasks (embed + eval).

Loads MiniLM-L6-v2 from an unpacked model blob (model.onnx +
tokenizer.json), and embeds batches of texts: tokenize, run the
transformer, mean-pool over the attention mask, L2-normalize.
Output: float32 vectors, D=384.
"""

import io
import tarfile

import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

MAX_LEN = 256
BATCH = 32


def unpack_model(blob, dest):
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r") as tar:
        tar.extractall(dest)


class Embedder:
    def __init__(self, model_dir):
        self.tok = Tokenizer.from_file(f"{model_dir}/tokenizer.json")
        self.tok.enable_truncation(max_length=MAX_LEN)
        self.tok.enable_padding()
        self.sess = ort.InferenceSession(
            f"{model_dir}/model.onnx", providers=["CPUExecutionProvider"])
        self.input_names = {i.name for i in self.sess.get_inputs()}
        self.dim = 384

    def embed(self, texts):
        out = np.empty((len(texts), self.dim), dtype=np.float32)
        for start in range(0, len(texts), BATCH):
            batch = texts[start:start + BATCH]
            enc = self.tok.encode_batch(batch)
            ids = np.array([e.ids for e in enc], dtype=np.int64)
            mask = np.array([e.attention_mask for e in enc], dtype=np.int64)
            feed = {"input_ids": ids, "attention_mask": mask}
            if "token_type_ids" in self.input_names:
                feed["token_type_ids"] = np.zeros_like(ids)
            hidden = self.sess.run(None, feed)[0]  # (B, L, 384)
            m = mask[:, :, None].astype(np.float32)
            pooled = (hidden * m).sum(axis=1) / np.clip(m.sum(axis=1), 1e-9, None)
            norms = np.linalg.norm(pooled, axis=1, keepdims=True)
            out[start:start + len(batch)] = pooled / np.clip(norms, 1e-9, None)
        return out
