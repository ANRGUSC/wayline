"""The warm runner preloads single-threaded and gives each call as many
compute threads as CPUs it was admitted with. Without the first, a call that
runs a parallel torch kernel after the parent started an OpenMP pool hangs."""
import os
import subprocess
import sys
import textwrap

import pytest

from wl import runner


def test_threads_follow_cpu_request(monkeypatch):
    for v in runner._THREAD_VARS:
        monkeypatch.delenv(v, raising=False)
    assert runner._threads_for({"WL_CPU_MILLIS": "2000"}) == 2
    assert runner._threads_for({"WL_CPU_MILLIS": "1500"}) == 2
    assert runner._threads_for({"WL_CPU_MILLIS": "250"}) == 1
    assert runner._threads_for({}) == 1
    assert runner._threads_for({"WL_CPU_MILLIS": "4000", "OMP_NUM_THREADS": "3"}) == 3
    assert os.environ["OMP_NUM_THREADS"] == "3"


def test_parallel_kernel_after_preload_does_not_hang(tmp_path):
    torch = pytest.importorskip("torch")
    # Parent: preload as the runner does, running large parallel kernels
    # (loading weights copies tensors in parallel). Zygote and call: two
    # forks, then a 2-CPU call runs parallel kernels. Without the
    # single-threaded preload this hangs with torch's libgomp (Linux).
    script = textwrap.dedent("""
        import os, sys
        from wl import runner
        for v in runner._THREAD_VARS:
            os.environ[v] = "1"
        import torch
        torch.empty(2048, 512, 3, 3).copy_(torch.ones(2048, 512, 3, 3))
        torch.nn.functional.relu(torch.randn(8, 64, 128, 128)).sum()
        zygote = os.fork()
        if zygote == 0:
            pid = os.fork()
            if pid == 0:
                runner._threads_for({"WL_CPU_MILLIS": "2000"})
                assert torch.get_num_threads() == 2
                x = torch.randn(1, 3, 270, 480)
                torch.nn.functional.conv2d(x, torch.randn(64, 3, 7, 7), stride=2).sum()
                torch.nn.functional.relu(torch.randn(8, 64, 128, 128)).sum()
                os._exit(0)
            _, status = os.waitpid(pid, 0)
            os._exit(os.waitstatus_to_exitcode(status))
        _, status = os.waitpid(zygote, 0)
        sys.exit(os.waitstatus_to_exitcode(status))
    """)
    env = {**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(runner.__file__))}
    r = subprocess.run([sys.executable, "-c", script], env=env, timeout=120)
    assert r.returncode == 0
