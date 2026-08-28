"""Named-object helpers for the MCMT Wayline wrappers.

The wrappers originally emitted one unnamed output per task, which the
runtime installed under the producer's task name. The object contract
identifies an intermediate as <run, producer, name>, so each producer now
names its output and each consumer reads by "producer.object" key.

The renderer supplies the names through the environment, so these
wrappers stay workload-agnostic and the same code serves the direct and
store-lowered arms:

  WL_OUT_NAME     this task's output name (producers only)
  WL_INPUT_KEYS   comma-separated "producer.object" keys, in dependency
                  order (consumers only)
  WL_INPUT_PEERS  optional override naming pod-less data vertices to read
                  instead of the producers, positionally matched to
                  WL_INPUT_KEYS. Store lowering sets this; the direct arm
                  leaves it unset.

Keeping the canonical key alongside the peer matters: under store
lowering the bytes arrive from a vertex, but the key still identifies
which logical object they are, and callers that need to know (the
fan-in stage maps object to camera) must use the key, not the peer.
"""
import os


def out_name() -> str:
    n = os.environ.get("WL_OUT_NAME", "")
    if not n:
        raise RuntimeError(
            "WL_OUT_NAME unset: the renderer must declare spec.outputs and "
            "pass the object name to the wrapper")
    return n


def input_keys() -> list[str]:
    return [k for k in os.environ.get("WL_INPUT_KEYS", "").split(",") if k]


def input_peers() -> list[str]:
    """Where to actually read each input from: the vertex under store
    lowering, otherwise the producing object itself."""
    keys = input_keys()
    peers = [p for p in os.environ.get("WL_INPUT_PEERS", "").split(",") if p]
    return [peers[i] if i < len(peers) else keys[i] for i in range(len(keys))]


def recv_named(task) -> list[tuple[str, bytes]]:
    """Read every declared input. Returns (canonical_key, bytes) in
    dependency order."""
    keys, peers = input_keys(), input_peers()
    if not keys:
        raise RuntimeError("WL_INPUT_KEYS unset; consumer has no declared inputs")
    out = []
    for key, peer in zip(keys, peers):
        blob = task.recv_raw(peer=peer)
        print(f"[{task.name}] read {len(blob)}B for {key} via {peer}",
              flush=True)
        out.append((key, blob))
    return out


def recv_one(task) -> bytes:
    """Single-input convenience."""
    got = recv_named(task)
    if len(got) != 1:
        raise RuntimeError(f"expected exactly one input, got {len(got)}")
    return got[0][1]


def send_named(task, blob: bytes) -> None:
    """Emit this task's single named output."""
    name = out_name()
    print(f"[{task.name}] sending {len(blob)} bytes as {name}", flush=True)
    task.send_raw(name, blob)


def producer_of(key: str) -> str:
    """'track-1.tracks' -> 'track-1'."""
    return key.split(".", 1)[0]
