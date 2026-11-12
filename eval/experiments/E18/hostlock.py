"""One cluster of a framework on the control node at a time.

Ray's head and Dask's scheduler run on the control node's host network on fixed ports, and a teardown deletes every
pod of the framework's label. Two experiments (E18 lanes, E19) must never hold one at once: a cluster's up() takes
the framework's lock and keeps it until its down(). One lock object per framework and process, so every Cluster
object of that process shares it.
"""
import fcntl
import os


class HostLock:
    def __init__(self, name):
        self.path = os.path.expanduser(f"~/.wl-{name}.lock")
        self.name = name
        self.f = None

    def try_acquire(self):
        if self.f is not None:
            return True
        f = open(self.path, "a+")
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            f.close()
            return False
        self.f = f
        return True

    def acquire(self):
        """Block until no other experiment holds the framework's cluster."""
        if self.try_acquire():
            return
        print(f"[{self.name}] another experiment's cluster is up; waiting for it to come down", flush=True)
        f = open(self.path, "a+")
        fcntl.flock(f, fcntl.LOCK_EX)
        self.f = f

    def release(self):
        if self.f is not None:
            fcntl.flock(self.f, fcntl.LOCK_UN)
            self.f.close()
            self.f = None
