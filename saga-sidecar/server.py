"""
SAGA scheduler sidecar for the Wayline odag-controller.

A small stdlib HTTP server (no framework deps beyond anrg-saga itself)
meant to run as a second container in the odag-controller pod, reachable
at 127.0.0.1 over the shared pod network.

Endpoints:
  GET  /healthz     -> 200 "ok"
  GET  /algorithms  -> JSON list of built-in SAGA algorithm names
  POST /schedule    -> {"algorithm": ..., "options": {...}, "dag": ...,
                        "clusterState": ...}
                       -> {"assignments": [...], "estimatedMakespan": ...}

Bringing your own scheduler
---------------------------
"algorithm" accepts a dotted path to any saga.Scheduler subclass, so a
scheduler developed and validated against SAGA runs here unmodified. Two
env vars make such code importable without rebuilding this image:

  WL_SAGA_PATH             colon-separated directories appended to sys.path.
                           Point at a mounted ConfigMap or PVC holding a
                           package.
  WL_SAGA_EXTRA_PACKAGES   whitespace-separated pip requirements installed at
                           startup (e.g. a git+https:// URL or a wheel path).
                           Failures are logged, not fatal — the built-ins
                           keep working.

Errors return 4xx/5xx with a JSON {"error": ...} body; the Go side treats
any non-200 as "fall back to the built-in HEFT scheduler".

Run: python server.py [--port 8090]
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import subprocess
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import bridge

logger = logging.getLogger("saga-sidecar")

MAX_BODY = 32 * 1024 * 1024  # 32 MB request cap


class Handler(BaseHTTPRequestHandler):
    server_version = "saga-sidecar/0.1"

    def _send_json(self, code: int, payload: dict | list) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        if self.path == "/healthz":
            self._send_json(200, {"status": "ok"})
        elif self.path == "/algorithms":
            self._send_json(200, bridge.available_algorithms())
        else:
            self._send_json(404, {"error": f"no such path {self.path}"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/schedule":
            self._send_json(404, {"error": f"no such path {self.path}"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length <= 0 or length > MAX_BODY:
                self._send_json(400, {"error": f"bad Content-Length {length}"})
                return
            request = json.loads(self.rfile.read(length))
        except (ValueError, json.JSONDecodeError) as e:
            self._send_json(400, {"error": f"invalid JSON: {e}"})
            return

        try:
            result = bridge.schedule_request(request)
        except KeyError as e:
            self._send_json(400, {"error": str(e)})
            return
        except Exception as e:  # scheduler bugs must not kill the server
            logger.error("schedule failed: %s\n%s", e, traceback.format_exc())
            self._send_json(500, {"error": f"{type(e).__name__}: {e}"})
            return
        self._send_json(200, result)

    def log_message(self, fmt: str, *args) -> None:
        logger.info("%s %s", self.address_string(), fmt % args)


def bootstrap_user_code() -> None:
    """Make operator-supplied scheduler packages importable.

    Runs before the first request so a dotted-path scheduler resolves without
    an image rebuild. Both steps are best-effort: a broken extra package must
    not take down scheduling for everyone else.
    """
    extra_path = os.environ.get("WL_SAGA_PATH", "")
    for d in [p for p in extra_path.split(":") if p]:
        if os.path.isdir(d):
            sys.path.insert(0, d)
            logger.info("added %s to sys.path", d)
        else:
            logger.warning("WL_SAGA_PATH entry %s is not a directory; ignored", d)

    pkgs = os.environ.get("WL_SAGA_EXTRA_PACKAGES", "").split()
    if not pkgs:
        return
    logger.info("installing extra packages: %s", " ".join(pkgs))
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-cache-dir", *pkgs],
            check=True, capture_output=True, timeout=600,
        )
        logger.info("extra packages installed")
    except subprocess.CalledProcessError as e:
        logger.error("pip install failed (built-ins still available): %s",
                     e.stderr.decode(errors="replace")[-2000:])
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.error("pip install failed (built-ins still available): %s", e)
    importlib.invalidate_caches()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap_user_code()
    # Fail fast at startup if SAGA is broken, and log the roster once.
    logger.info("built-in algorithms: %s", ", ".join(bridge.available_algorithms()))
    logger.info("plus any importable saga.Scheduler subclass by dotted path")

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    logger.info("saga-sidecar listening on %s:%d", args.host, args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
