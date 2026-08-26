"""
WlTask: the main user-facing class for task communication.

Usage in task images:

    from wl import WlTask

    task = WlTask()

    # One-shot ODAG (file transport — layer-by-layer execution):
    data  = task.recv("upstream-task")   # read one upstream's output
    inputs = task.recv_all()             # read all upstreams at once -> dict
    task.send(result)                    # routes to all successors automatically

The odag-controller injects (file transport):
    WL_TRANSPORT_PATTERN     file
    WL_ODAG_NAME             ODAG CR name
    WL_TASK_NAME             this task's name
    WL_OUTPUT_DIR            where this task writes its output
    WL_DEPS                  comma-separated dependency names
    WL_SUCCESSORS            comma-separated successor names
    WL_NODE_IP               host IP for data-agent state reporting
    NODE_NAME                this pod's node (downward API)
    WL_DEP_<NAME>_NODE       node name where dependency <NAME> ran
    WL_RUNTIME               expected wall-clock runtime in seconds
    WL_DATA_SIZE             expected output size in bytes

The pub/sub transport methods (subscribe/publish/on/run) target continuous
DAGs (CDAGs), which are out of scope for this release; the transport code is
retained for future work.
"""

import json
import os
import sys
from typing import Any, Callable, Generator

from wl.transport.router import build_transport

# Ensure stdout/stderr are line-buffered so logs are visible in kubectl logs.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)  # type: ignore[union-attr]


_UNSET = object()


class WlTask:
    """
    Entry point for Wayline task communication.

    Instantiate once at the start of your task. Reads configuration from
    environment variables injected by the odag-controller or cdag-controller.

    Attributes
    ----------
    name : str
        This task's name (WL_TASK_NAME).
    node : str
        The cluster node this pod is running on (NODE_NAME).
    dependencies : list[str]
        Names of upstream tasks this task reads from (WL_DEPS).
    successors : list[str]
        Names of downstream tasks that read from this task (WL_SUCCESSORS).
    is_root : bool
        True if this task has no dependencies.
    is_leaf : bool
        True if this task has no successors (no need to call send()).
    expected_runtime : float
        Expected wall-clock runtime in seconds from the ODAG spec (WL_RUNTIME).
    expected_data_size : int
        Expected output size in bytes from the ODAG spec (WL_DATA_SIZE).
    template_name : str
        Name of the ODAGTemplate this run was created from, or empty string
        if submitted directly (WL_TEMPLATE_NAME).
    run_id : str
        Run number within the template (e.g. "3"), or empty string if not
        a template run (WL_RUN_ID).
    """

    def __init__(self) -> None:
        self.name: str = os.environ.get("WL_TASK_NAME", "unknown")
        self.node: str = os.environ.get("NODE_NAME", "")

        deps_env = os.environ.get("WL_DEPS", "")
        self.dependencies: list[str] = [d for d in deps_env.split(",") if d]

        succs_env = os.environ.get("WL_SUCCESSORS", "")
        self.successors: list[str] = [s for s in succs_env.split(",") if s]

        self.is_root: bool = len(self.dependencies) == 0
        self.is_leaf: bool = len(self.successors) == 0

        self.expected_runtime: float = float(os.environ.get("WL_RUNTIME", "0") or "0")
        self.expected_data_size: int = int(os.environ.get("WL_DATA_SIZE", "0") or "0")

        self.template_name: str = os.environ.get("WL_TEMPLATE_NAME", "")
        self.run_id: str = os.environ.get("WL_RUN_ID", "")

        self._transport = build_transport()
        pattern = os.environ.get("WL_TRANSPORT_PATTERN", "pushpull")
        print(f"[{self.name}] WlTask initialized (transport: {pattern})", flush=True)

    def dep_node(self, dep: str) -> str:
        """
        Return the cluster node name where a dependency task ran.

        Args:
            dep: name of the upstream dependency task.

        Returns:
            Node name string, or empty string if not available.
        """
        key = dep.upper().replace("-", "_")
        return os.environ.get(f"WL_DEP_{key}_NODE", "")

    def send(self, data: Any, _payload: Any = _UNSET) -> None:
        """
        Emit an output object.

        One-argument form emits the task's single default output to all
        successors: send(data).

        Two-argument form emits a NAMED output: send("alert", data). A
        named output is an independent, immutable object with identity
        <run, task, name>; it is delivered to exactly the consumers whose
        spec.inputs name it, and external policy can revise its
        realization (copies, serving source, lifetime) independently of
        the task's other outputs. The name must be declared in the
        task's spec.outputs.

        Args:
            data: output name (two-arg form) or JSON-serializable value.
            _payload: JSON-serializable value (two-arg form only).
        """
        if _payload is _UNSET:
            self._transport.send(json.dumps(data).encode())
        else:
            self._transport.send(json.dumps(_payload).encode(),
                                 output=str(data))

    def send_raw(self, data: Any, _payload: bytes = _UNSET) -> None:
        """
        Raw-bytes variant of send(); same one/two-argument forms.

        Skips JSON serialization, avoiding a second in-memory copy. Use
        for large payloads where memory is tight.
        """
        if _payload is _UNSET:
            self._transport.send(data)
        else:
            self._transport.send(_payload, output=str(data))

    def recv(self, peer: str | None = None) -> Any:
        """
        Receive data from an upstream task.

        For file transport: reads from the local hostPath output file written
        by the upstream task. Peer defaults to the single dep in WL_DEPS.

        For pubsub transport: subscribes to peer's PUB socket (peer required).

        Args:
            peer: name of the upstream task. Required when there are multiple
                  upstream dependencies; omit if there is exactly one.

        Returns:
            The deserialized value sent by the upstream task.
        """
        payload = self._transport.recv(peer)
        return json.loads(payload)

    def recv_all(self) -> dict[str, Any]:
        """
        Receive data from all upstream dependencies at once.

        Returns:
            A dict mapping dependency name -> deserialized value.
            Keys match the names in WL_DEPS / spec.tasks[].dependencies.
        """
        raw = self._transport.recv_all()
        return {k: json.loads(v) for k, v in raw.items()}

    def recv_raw(self, peer: str | None = None) -> bytes:
        """Receive raw bytes from an upstream task (no JSON deserialization)."""
        return self._transport.recv(peer)

    def recv_all_raw(self) -> dict[str, bytes]:
        """Receive raw bytes from all upstream dependencies."""
        return self._transport.recv_all()

    def close(self) -> None:
        """Close all open sockets / file handles. Call on shutdown."""
        self._transport.close()

    # ------------------------------------------------------------------
    # Streaming API (CDAG pub/sub)
    # ------------------------------------------------------------------

    def publish(self, data: Any, *, to: str | None = None) -> None:
        """
        Publish data to downstream subscribers (CDAG streaming).

        Args:
            data: JSON-serializable value.
            to:   If None, broadcast to ALL successors.
                  If set, send only to the named successor.

        Examples:
            task.publish(result)                  # broadcast
            task.publish(result, to="processor")  # targeted
        """
        payload = json.dumps(data).encode()
        topic = to.encode() if to else b""
        self._transport.publish(payload, topic=topic)

    def publish_raw(self, data: bytes, *, to: str | None = None) -> None:
        """
        Publish raw bytes to downstream subscribers (no JSON serialization).

        Args:
            data: Raw bytes payload.
            to:   If None, broadcast. If set, targeted to named successor.
        """
        topic = to.encode() if to else b""
        self._transport.publish(data, topic=topic)

    def subscribe(self, peer: str) -> Generator[Any, None, None]:
        """
        Subscribe to a peer and yield messages as a continuous stream.

        Blocks on each iteration until the next message arrives.

        Args:
            peer: Name of the upstream task to subscribe to.

        Yields:
            Deserialized JSON values from the peer.

        Example:
            for msg in task.subscribe("producer"):
                result = process(msg)
                task.publish(result)
        """
        sock = self._transport.subscribe(peer)
        while True:
            frames = sock.recv_multipart()
            payload = frames[1] if len(frames) == 2 else frames[0]
            yield json.loads(payload)

    def subscribe_raw(self, peer: str) -> Generator[bytes, None, None]:
        """
        Subscribe to a peer and yield raw bytes as a continuous stream.

        Args:
            peer: Name of the upstream task to subscribe to.

        Yields:
            Raw bytes from the peer (no JSON deserialization).
        """
        sock = self._transport.subscribe(peer)
        while True:
            frames = sock.recv_multipart()
            payload = frames[1] if len(frames) == 2 else frames[0]
            yield payload

    def subscribe_all(self) -> Generator[tuple[str, Any], None, None]:
        """
        Subscribe to ALL upstream dependencies and yield messages from any of them.

        Uses polling across all dependency sockets. Blocks until at least one
        message is available from any peer.

        Yields:
            (peer_name, deserialized_value) tuples.

        Example:
            for peer, msg in task.subscribe_all():
                if peer == "sensor-1":
                    handle_sensor(msg)
                elif peer == "sensor-2":
                    handle_camera(msg)
        """
        sockets: dict[str, Any] = {}
        for dep in self.dependencies:
            sockets[dep] = self._transport.subscribe(dep)
        while True:
            results = self._transport.poll_subscribers(sockets, timeout_ms=-1)
            for peer_name, payload in results:
                yield peer_name, json.loads(payload)

    def subscribe_all_raw(self) -> Generator[tuple[str, bytes], None, None]:
        """
        Subscribe to ALL upstream dependencies and yield raw bytes from any of them.

        Yields:
            (peer_name, raw_bytes) tuples.
        """
        sockets: dict[str, Any] = {}
        for dep in self.dependencies:
            sockets[dep] = self._transport.subscribe(dep)
        while True:
            results = self._transport.poll_subscribers(sockets, timeout_ms=-1)
            for peer_name, payload in results:
                yield peer_name, payload

    def on(self, peer: str) -> Callable:
        """
        Decorator to register a callback for messages from a specific peer.

        Use with task.run() to start the event loop.

        Example:
            @task.on("sensor-1")
            def handle_sensor(data):
                task.publish(fuse(data))

            @task.on("sensor-2")
            def handle_camera(data):
                task.publish(classify(data))

            task.run()
        """
        if not hasattr(self, "_handlers"):
            self._handlers: dict[str, Callable] = {}

        def decorator(fn: Callable) -> Callable:
            self._handlers[peer] = fn
            return fn
        return decorator

    def run(self) -> None:
        """
        Start the callback event loop (CDAG streaming).

        Blocks forever, polling all peers that have registered handlers
        via @task.on() and dispatching messages to the appropriate callback.

        Example:
            @task.on("producer")
            def handle(data):
                task.publish(process(data))

            task.run()  # blocks forever
        """
        if not hasattr(self, "_handlers") or not self._handlers:
            raise RuntimeError("No handlers registered. Use @task.on('peer') to register handlers before calling run().")

        sockets: dict[str, Any] = {}
        for peer in self._handlers:
            sockets[peer] = self._transport.subscribe(peer)

        print(f"[{self.name}] event loop started, listening to: {list(self._handlers.keys())}", flush=True)
        while True:
            results = self._transport.poll_subscribers(sockets, timeout_ms=-1)
            for peer_name, payload in results:
                data = json.loads(payload)
                handler = self._handlers.get(peer_name)
                if handler:
                    handler(data)
