"""
Wayline Python SDK.

Usage in task images:

    from wl import WlTask

    task = WlTask()
    data = task.recv("upstream-task")        # blocks until data arrives
    alert, features = process(data)
    task.send(result)                        # single default output
    task.send("alert", alert)                # named output <run, task, alert>
    task.send("features", features)          # independently realizable

The controller injects peer configuration as environment variables:
    WL_TASK_NAME=<this-task-name>
    WL_PEER_<NAME>=<transport>://<endpoint>

The SDK reads these env vars automatically.
"""

from wl.api import WlTask

__all__ = ["WlTask", "function", "serve"]


def __getattr__(name):
    # Lazy, so `python -m wl.runner` does not import wl.runner twice.
    if name in ("function", "serve"):
        from wl import runner
        return getattr(runner, name)
    raise AttributeError(f"module 'wl' has no attribute {name!r}")
