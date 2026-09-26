"""
Wayline warm runner: execute tasks as calls on a long-lived process
instead of in a fresh pod per task.

A runner is a pod (usually one per node, from a DaemonSet) labeled
wl.io/runner=<name>. A task opts in with `runner: <name>` in its spec; the
controller then POSTs the invocation here with the exact environment the
task's pod would have had. The task body is unchanged: it builds a WlTask,
reads inputs, sends outputs, closes.

Two ways to provide task bodies:

    # 1. functions
    import wl

    @wl.function                      # name defaults to the function name
    def resize(task):                 # receives a ready WlTask
        img = task.recv_raw()
        task.send_raw(shrink(img))    # close() is called for you

    wl.serve()

    # 2. an existing task script, unchanged
    python -m wl.runner --script task.py [--preload numpy,torch]

Execution model (zygote): serve() forks a single-threaded zygote after the
user's modules are imported; the zygote forks one child per invocation,
with the invocation's environment applied to os.environ. Children start
in milliseconds with every import already done, run in parallel on
separate cores (no GIL sharing), and a crash kills only that invocation.
At most `slots` children run at once; the rest queue in arrival order.

HTTP API (port 8090):
    POST /invoke                     {odag, task, function, env} -> 202, 409 if seen
    GET  /invocations/<odag>/<task>  {state: queued|running|exited, exit}
    GET  /functions                  registered names
    GET  /healthz
"""

import argparse
import importlib
import inspect
import json
import os
import runpy
import select
import sys
import threading
import time
import traceback
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_FUNCTIONS: dict = {}
_SCRIPT: str | None = None
_AGENT_PORT = 8082


def function(fn=None, *, name: str | None = None):
    """Register a task body. Use as @function or @function(name="x")."""
    def reg(f):
        _FUNCTIONS[name or f.__name__] = f
        return f
    return reg(fn) if fn is not None else reg


# ─── invocation child ────────────────────────────────────────────────────────

def _agent_state(env: dict, state: str | None = None) -> str:
    ip, odag, task = env.get("WL_NODE_IP"), env.get("WL_ODAG_NAME"), env.get("WL_TASK_NAME")
    if not (ip and odag and task):
        return ""
    url = f"http://{ip}:{_AGENT_PORT}/state/{odag}/{task}"
    try:
        if state is None:
            with urllib.request.urlopen(url, timeout=5) as r:
                return r.read().decode().strip()
        req = urllib.request.Request(url, data=state.encode(), method="PUT")
        with urllib.request.urlopen(req, timeout=5):
            return state
    except Exception:
        return ""


def _run_child(fname: str, env: dict) -> None:
    """Runs in the forked child; never returns."""
    code = 0
    try:
        os.environ.update(env)
        fn = _FUNCTIONS.get(fname)
        if fn is None and _SCRIPT:
            sys.argv = [_SCRIPT]
            runpy.run_path(_SCRIPT, run_name="__main__")
        elif fn is None:
            raise LookupError(f"no function {fname!r} on this runner")
        elif len(inspect.signature(fn).parameters) >= 1:
            from wl.api import WlTask
            task = WlTask()
            fn(task)
            if not getattr(task, "_closed", False):
                task.close()
        else:
            fn()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    except BaseException:
        traceback.print_exc()
        code = 1
    if code != 0:
        _agent_state(env, "Failed")
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


# ─── zygote ──────────────────────────────────────────────────────────────────

def _zygote(req_fd: int, ev_fd: int, slots: int) -> None:
    """Single-threaded: reads invocations, forks children, reports exits."""
    queue: deque = deque()
    running: dict = {}
    buf = b""

    def emit(obj):
        os.write(ev_fd, (json.dumps(obj) + "\n").encode())

    while True:
        r, _, _ = select.select([req_fd], [], [], 0.01 if running or queue else 0.5)
        if r:
            chunk = os.read(req_fd, 65536)
            if not chunk:                       # parent is gone
                os._exit(0)
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                queue.append(json.loads(line))
        while True:                             # reap
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if pid == 0:
                break
            key = running.pop(pid, None)
            if key:
                emit({"key": key, "event": "exit", "code": os.waitstatus_to_exitcode(status)})
        while queue and len(running) < slots:   # start
            inv = queue.popleft()
            pid = os.fork()
            if pid == 0:
                os.close(req_fd)
                os.close(ev_fd)
                _run_child(inv["function"], inv["env"])
            running[pid] = inv["key"]
            emit({"key": inv["key"], "event": "start", "pid": pid, "t": time.time()})


# ─── server ──────────────────────────────────────────────────────────────────

class _Runner:
    def __init__(self, slots: int):
        self.lock = threading.Lock()
        self.table: dict = {}                   # key -> {state, exit, env, t}
        req_r, self.req_w = os.pipe()
        ev_r, ev_w = os.pipe()
        pid = os.fork()                         # before any thread exists
        if pid == 0:
            os.close(self.req_w)
            os.close(ev_r)
            _zygote(req_r, ev_w, slots)
        os.close(req_r)
        os.close(ev_w)
        self.ev = os.fdopen(ev_r, "rb")
        threading.Thread(target=self._events, daemon=True).start()

    def invoke(self, inv: dict) -> int:
        key = f"{inv['odag']}/{inv['task']}"
        if inv["function"] not in _FUNCTIONS and not _SCRIPT:
            return 404
        with self.lock:
            if key in self.table:
                return 409
            self.table[key] = {"state": "queued", "exit": None, "env": inv["env"], "t": time.time()}
            os.write(self.req_w, (json.dumps({**inv, "key": key}) + "\n").encode())
        return 202

    def _events(self):
        for line in self.ev:
            ev = json.loads(line)
            with self.lock:
                rec = self.table.get(ev["key"])
                if rec is None:
                    continue
                if ev["event"] == "start":
                    rec["state"] = "running"
                else:
                    rec["state"], rec["exit"] = "exited", ev["code"]
            if ev["event"] == "exit" and ev["code"] != 0:
                # A child killed outright (OOM, signal) could not report.
                if _agent_state(rec["env"]) != "ComputeDone":
                    _agent_state(rec["env"], "Failed")
            self._prune()

    def _prune(self, keep_s: float = 3600):
        now = time.time()
        with self.lock:
            for k in [k for k, v in self.table.items() if v["state"] == "exited" and now - v["t"] > keep_s]:
                del self.table[k]

    def status(self, key: str):
        with self.lock:
            rec = self.table.get(key)
            return None if rec is None else {"state": rec["state"], "exit": rec["exit"]}


def serve(port: int = 8090, slots: int | None = None) -> None:
    """Serve registered functions (and the --script, if any) forever."""
    runner = _Runner(slots or os.cpu_count() or 1)

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, code, obj=None):
            body = json.dumps(obj if obj is not None else {}).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            p = self.path.strip("/").split("/")
            if p == ["healthz"]:
                return self._json(200, {"ok": True})
            if p == ["functions"]:
                return self._json(200, {"functions": sorted(_FUNCTIONS), "script": _SCRIPT})
            if len(p) == 3 and p[0] == "invocations":
                st = runner.status(f"{p[1]}/{p[2]}")
                return self._json(404) if st is None else self._json(200, st)
            self._json(404)

        def do_POST(self):
            if self.path.rstrip("/") != "/invoke":
                return self._json(404)
            n = int(self.headers.get("Content-Length", "0"))
            try:
                inv = json.loads(self.rfile.read(n))
                inv = {"odag": inv["odag"], "task": inv["task"],
                       "function": inv.get("function") or inv["task"], "env": inv.get("env", {})}
            except Exception as e:
                return self._json(400, {"error": str(e)})
            self._json(runner.invoke(inv))

    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    print(f"[wl-runner] serving {sorted(_FUNCTIONS) or []}"
          f"{' + script ' + _SCRIPT if _SCRIPT else ''} on :{port}", flush=True)
    srv.serve_forever()


def main(argv=None) -> None:
    global _SCRIPT
    ap = argparse.ArgumentParser(prog="python -m wl.runner")
    ap.add_argument("--script", help="run this task script for any invocation not matching a function")
    ap.add_argument("--module", action="append", default=[], help="import to register @wl.function bodies")
    ap.add_argument("--preload", default="", help="comma-separated modules to import before forking")
    ap.add_argument("--port", type=int, default=int(os.environ.get("WL_RUNNER_PORT", "8090")))
    ap.add_argument("--slots", type=int, default=int(os.environ.get("WL_RUNNER_SLOTS", "0")) or None)
    a = ap.parse_args(argv)
    for m in [m for m in a.preload.split(",") if m] + a.module:
        importlib.import_module(m)
    import wl.api  # noqa: F401  (warm the SDK itself)
    if a.script:
        _SCRIPT = os.path.abspath(a.script)
    serve(a.port, a.slots)


if __name__ == "__main__":
    # `python -m wl.runner` executes this file as __main__, a second copy of
    # the module; @wl.function registers into the canonical wl.runner, so
    # run from that one.
    from wl import runner as _canonical
    _canonical.main()
