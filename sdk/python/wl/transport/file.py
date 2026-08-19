"""
FileTransport: push-based p2p file transport for one-shot ODAGs.

Flow
----
send(payload):
    1. PUT payload to the local data-agent at /<odag>/<task>/output with a
       Content-MD5 header. The agent installs atomically (temp file → fsync
       → rename → fsync parent dir) and sets the .wl-ready marker only
       after the bytes are durably on disk. This is the SAME install path
       used for cross-node receive — local and remote handoff are
       semantically identical from the SDK's perspective.
    2. POST /push/<odag>/<task> to the local data-agent with the list of
       remote (cross-node) successors. The agent persists the per-successor
       transfer queue durably before responding 202, then handles transfers
       in a background goroutine.
    3. Return immediately — the pod can continue and exit without waiting.

recv(peer) / recv_all():
    Always a local file read. The odag-controller starts a task pod only
    after all upstream deps' .wl-ready markers are present on THIS node, so
    the file is guaranteed installed.

close():
    Sets task state = ComputeDone and returns. The pod exits immediately
    regardless of whether the data-agent transfer is still in progress.

State protocol (locked, see project_atc2026_data_plane_state_model)
-------------------------------------------------------------------
Two independent signals tracked by the local data-agent at WL_NODE_IP:8082:

  Task lifecycle      PUT /state/<odag>/<task>  body ∈ {Pending, Running,
                                                       ComputeDone, Failed}
  Local data ready    PUT /ready/<odag>/<task>  presence-only marker

SDK writes:
    Running       — on FileTransport.__init__()
    ComputeDone   — on close()

The agent (not the SDK) is the only writer of .wl-ready, both for local
installs (when the SDK PUTs through it) and remote installs (when another
node pushes here). The SDK never writes Failed for transfer errors — that
is a per-successor transfer-state concern, not a task-lifecycle concern.

Environment variables injected by odag-controller
--------------------------------------------------
    WL_ODAG_NAME               name of this ODAG
    WL_TASK_NAME               name of this task
    WL_OUTPUT_DIR              directory where this task should write output
    WL_DEPS                    comma-separated upstream dependency names
    WL_NODE_IP                 host IP of this node (for data-agent state PUT)
    NODE_NAME                   this pod's node (downward API)

    WL_SUCCESSORS              comma-separated successor task names
    WL_SUCC_<SUCC>_NODE        node name where that successor will run
    WL_SUCC_<SUCC>_HOST        internal IP of that node (for data-agent PUT)

    (<SUCC> is the task name uppercased with hyphens replaced by underscores)
"""

import hashlib
import json
import os
import time
import urllib.request


_DATA_AGENT_PORT = 8082
# Generous local-install timeout: the PUT is to localhost over hostPort, so
# wall-clock is bounded by local disk write + fsync. 5 minutes covers any
# payload size we expect on edge nodes (multi-GB would already exceed disk
# headroom). Not the place to micro-tune.
_INSTALL_TIMEOUT_S = 300

# Wire-level headers — must match cmd/data-agent/main.go constants.
_HDR_CONTENT_SHA256 = "X-Wayline-Content-SHA256"
_HDR_UNCOMPRESSED_LENGTH = "X-Wayline-Uncompressed-Length"


class FileTransport:
    """
    File-based transport for one-shot (ODAG) task graphs.

    send(payload)   — write output locally and hand off remote pushes to the
                      data-agent; returns immediately (non-blocking).
    recv(peer)      — read a specific upstream dep's output (local file).
    recv_all()      — read all deps; returns {dep_name: bytes}.
    close()         — signal Done and exit; transfer continues in data-agent.
    """

    def __init__(self) -> None:
        self.odag_name: str = os.environ["WL_ODAG_NAME"]
        self.task_name: str = os.environ["WL_TASK_NAME"]
        self.output_dir: str = os.environ["WL_OUTPUT_DIR"]
        self.node_name: str = os.environ.get("NODE_NAME", "")
        self.node_ip: str = os.environ.get("WL_NODE_IP", "")
        # Phase boundaries, wall-clock epoch seconds. Reported once at
        # close() so a scheduler's predicted compute time can be compared
        # against the time actually spent computing, separately from
        # container startup, input reads, and output handoff. All are
        # best-effort: instrumentation never fails a task.
        self._t_task_start: float = time.time()
        self._t_recv_first: float | None = None
        self._t_recv_last: float | None = None
        self._t_send_start: float | None = None
        self._t_send_installed: float | None = None
        self._bytes_in: int = 0
        self._bytes_out: int = 0
        self._set_task_state("Running")

    # ------------------------------------------------------------------ #
    # state protocol                                                        #
    # ------------------------------------------------------------------ #

    def _set_task_state(self, state: str) -> None:
        """Write a value from the locked task-state vocabulary."""
        if not self.node_ip:
            return
        url = f"http://{self.node_ip}:{_DATA_AGENT_PORT}/state/{self.odag_name}/{self.task_name}"
        try:
            req = urllib.request.Request(url, data=state.encode(), method="PUT")
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            print(f"[{self.task_name}] WARNING: failed to set task state={state}: {e}", flush=True)

    def _install_local(self, payload: bytes) -> None:
        """
        PUT payload through the local data-agent so it goes through the
        same atomic install path (temp → fsync → rename → fsync parent →
        .wl-ready marker) used for remote receives. Local and remote
        handoff share one code path; the SDK is a thin client.

        Computes X-Wayline-Content-SHA256 (hex) over the uncompressed
        payload so the agent can verify the installed digest matches what
        was sent (idempotency + integrity).
        """
        if not self.node_ip:
            raise RuntimeError("WL_NODE_IP not set; cannot install via data-agent")
        digest = hashlib.sha256(payload).hexdigest()
        url = f"http://{self.node_ip}:{_DATA_AGENT_PORT}/{self.odag_name}/{self.task_name}/output"
        req = urllib.request.Request(
            url,
            data=payload,
            method="PUT",
            headers={
                _HDR_CONTENT_SHA256: digest,
                _HDR_UNCOMPRESSED_LENGTH: str(len(payload)),
                "Content-Length": str(len(payload)),
                "Content-Type": "application/octet-stream",
            },
        )
        with urllib.request.urlopen(req, timeout=_INSTALL_TIMEOUT_S) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    f"data-agent install rejected with status {resp.status}"
                )

    # ------------------------------------------------------------------ #
    # send                                                                  #
    # ------------------------------------------------------------------ #

    def send(self, payload: bytes) -> None:
        """
        Install payload via the local data-agent (atomic + marker), then
        ask the agent to push to remote successors. Returns once the local
        install completes; remote transfers happen in the background.
        """
        # 1. Local install via the data-agent. The agent writes a temp file,
        # fsyncs, renames into place, fsyncs the parent dir, then writes the
        # .wl-ready marker. Same-node consumers can proceed as soon as this
        # call returns. The SDK never touches the hostPath directly.
        if self._t_send_start is None:
            self._t_send_start = time.time()
        self._install_local(payload)
        self._t_send_installed = time.time()
        self._bytes_out += len(payload)
        print(
            f"[{self.task_name}] installed {len(payload)} bytes via local data-agent",
            flush=True,
        )

        # 2. Build list of cross-node successors for the data-agent to push to.
        succs_env = os.environ.get("WL_SUCCESSORS", "")
        successors = []
        for succ in [s for s in succs_env.split(",") if s]:
            succ_key = succ.upper().replace("-", "_")
            succ_node = os.environ.get(f"WL_SUCC_{succ_key}_NODE", "")
            succ_host = os.environ.get(f"WL_SUCC_{succ_key}_HOST", "")
            if succ_node == self.node_name:
                continue  # same-node: file already present on shared hostPath
            if not succ_host:
                print(
                    f"[{self.task_name}] WARNING: no host for successor {succ}; skipping",
                    flush=True,
                )
                continue
            successors.append({"name": succ, "host": succ_host, "node": succ_node})

        # 3. Hand off to data-agent. The agent durably persists the per-successor
        # queue entries and Pending state files before responding 202, so
        # this call may take a few hundred ms for many successors.
        self._request_push(successors)

    def _request_push(self, successors: list) -> None:
        if not self.node_ip:
            return
        url = f"http://{self.node_ip}:{_DATA_AGENT_PORT}/push/{self.odag_name}/{self.task_name}"
        body = json.dumps({"successors": successors}).encode()
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30):
                pass
            print(
                f"[{self.task_name}] handed off push to data-agent "
                f"({len(successors)} remote successor(s))",
                flush=True,
            )
        except Exception as e:
            print(f"[{self.task_name}] WARNING: failed to request push: {e}", flush=True)

    # ------------------------------------------------------------------ #
    # recv                                                                  #
    # ------------------------------------------------------------------ #

    def recv(self, peer: str | None = None) -> bytes:
        """
        Read the output of an upstream dependency from the local hostPath.
        """
        if peer is None:
            deps = [d for d in os.environ.get("WL_DEPS", "").split(",") if d]
            if not deps:
                raise RuntimeError("recv() called with no peer and WL_DEPS is empty")
            if len(deps) > 1:
                raise RuntimeError(
                    f"recv() called with no peer but task has multiple deps: {deps}. "
                    "Use recv(peer) or recv_all()."
                )
            peer = deps[0]

        if self._t_recv_first is None:
            self._t_recv_first = time.time()
        path = f"/data/wl-outputs/{self.odag_name}/{peer}/output"
        with open(path, "rb") as f:
            data = f.read()
        self._t_recv_last = time.time()
        self._bytes_in += len(data)
        print(f"[{self.task_name}] read {len(data)} bytes from {peer} ({path})", flush=True)
        return data

    def recv_all(self) -> dict[str, bytes]:
        """Read outputs from all upstream dependencies."""
        deps = [d for d in os.environ.get("WL_DEPS", "").split(",") if d]
        if not deps:
            raise RuntimeError("recv_all() called but WL_DEPS is empty")
        return {dep: self.recv(dep) for dep in deps}

    # ------------------------------------------------------------------ #
    # close                                                                 #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """
        Signal that the task's compute phase has finished cleanly, then
        return. The pod exits immediately. The data-agent completes any
        in-flight transfers independently; per-successor transfer state
        captures their outcome.
        """
        self._report_timings()
        self._set_task_state("ComputeDone")

    # ------------------------------------------------------------------ #
    # instrumentation                                                       #
    # ------------------------------------------------------------------ #

    def _report_timings(self) -> None:
        """
        PUT the task's phase boundaries to the local data-agent.

        Compute is defined as the interval between the last input read and
        the first output handoff — the span a scheduler's cost model claims
        to predict. For the usual ODAG shape (read inputs, compute, send
        once) that is exact; a task that interleaves reads and writes still
        gets monotone boundaries, but computeSeconds then covers the whole
        interleaved span and should be read as an upper bound.

        Best-effort: any failure is logged and ignored. Instrumentation must
        never fail a task that has already produced its output.
        """
        if not self.node_ip:
            return
        now = time.time()
        compute_start = self._t_recv_last if self._t_recv_last is not None else self._t_task_start
        compute_end = self._t_send_start if self._t_send_start is not None else now
        rec = {
            "taskStartUnix": self._t_task_start,
            "computeStartUnix": compute_start,
            "computeEndUnix": compute_end,
            "closeUnix": now,
            "computeSeconds": max(compute_end - compute_start, 0.0),
            "inputReadSeconds": (
                max(self._t_recv_last - self._t_recv_first, 0.0)
                if self._t_recv_first is not None and self._t_recv_last is not None
                else 0.0
            ),
            "handoffSeconds": (
                max(self._t_send_installed - self._t_send_start, 0.0)
                if self._t_send_start is not None and self._t_send_installed is not None
                else 0.0
            ),
            "bytesIn": self._bytes_in,
            "bytesOut": self._bytes_out,
        }
        url = f"http://{self.node_ip}:{_DATA_AGENT_PORT}/timings/{self.odag_name}/{self.task_name}"
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(rec).encode(),
                method="PUT",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            print(f"[{self.task_name}] WARNING: failed to report timings: {e}", flush=True)
