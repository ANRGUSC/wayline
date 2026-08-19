# Bring your own scheduler

You wrote a DAG scheduler, implemented it against
[SAGA](https://github.com/ANRGUSC/saga), and compared it in simulation.
Running it on a real Kubernetes cluster should not require rewriting it.
It doesn't: Wayline loads any `saga.Scheduler` subclass by name.

```yaml
spec:
  scheduler: saga/mypkg.schedulers.MyScheduler
```

No Go code, no entry in a registry, no image rebuild of the controller.
The only requirement is that the class is *importable inside the scheduler
sidecar*, which is what the rest of this page is about.

## The four forms of `spec.scheduler`

| value | meaning |
|---|---|
| `random` | **default.** Random placement among each task's allowed nodes |
| `heft` | compiled-in HEFT (resource- and contention-aware) |
| `saga/heft`, `saga/cpop`, `saga/minmin`, … | a built-in SAGA algorithm |
| `saga/pkg.module.ClassName` | **any importable `saga.Scheduler` subclass** |
| `http://host:port` | any service speaking the scheduler contract, in any language |

If an external scheduler fails for any reason — unreachable, import error,
exception, a placement that violates task constraints — the controller logs
the reason and falls back to random placement — not to an optimising
scheduler, which would silently produce a good placement and leave you
measuring HEFT while you believed you were measuring your own scheduler.

All placements, including random, honour `constraints.nodeNames`.

## Writing the scheduler

Exactly the SAGA interface, nothing Wayline-specific:

```python
from saga import Network, Schedule, ScheduledTask, Scheduler, TaskGraph

class MyScheduler(Scheduler):
    lookahead: int = 2          # pydantic field -> settable from the ODAG spec

    def schedule(self, network: Network, task_graph: TaskGraph,
                 schedule=None, min_start_time: float = 0.0) -> Schedule:
        ...
        return sched
```

Only the task→node mapping in the returned `Schedule` is load-bearing.
Wayline dispatches each task when its inputs are ready on its assigned
node, so predicted start/end times are treated as a prediction to measure
against (see `eval/tools/fidelity.py`), not as an instruction.

Constructor options come from the spec:

```yaml
spec:
  scheduler: saga/mypkg.MyScheduler
  schedulerConfig:
    options:
      lookahead: 4
```

## Getting your code into the sidecar

Three ways, in increasing order of permanence.

**1. ConfigMap (quickest, good for a single-file scheduler).** The
deployment already mounts a ConfigMap named `wl-user-schedulers` at
`/opt/wl-schedulers`, which is on `WL_SAGA_PATH`:

```bash
kubectl create configmap wl-user-schedulers -n wl-system \
  --from-file=mysched/__init__.py=./mysched.py
kubectl rollout restart deploy/odag-controller -n wl-system
```

Then `spec.scheduler: saga/mysched.MyScheduler`.

**2. pip at startup (good for a package in git).** Set on the sidecar
container:

```yaml
- name: WL_SAGA_EXTRA_PACKAGES
  value: "git+https://github.com/you/my-schedulers@v0.2"
```

Installed when the sidecar starts; failures are logged and non-fatal.

**3. Your own image.** Build `FROM` the sidecar image with your package
installed, and point the container at it. Best for anything with compiled
or heavyweight dependencies.

## Running your own scheduler service

If you would rather not run Python in our sidecar — a different language, a
solver with a big runtime, a service you already operate — implement the
contract yourself and point at it:

```yaml
spec:
  scheduler: http://my-scheduler.default.svc.cluster.local:8090
```

`POST /schedule` receives:

```json
{
  "algorithm": "",
  "options": {},
  "dag": {"tasks": [
    {"name": "t1", "dependencies": [], "runtime": 10, "dataSize": "100MB",
     "runtimeProfile": {"node-a": 8.2, "node-b": 11.7},
     "dataSizeProfile": {"node-a": 104857600},
     "constraints": {"nodeNames": ["node-a", "node-b"]}}
  ]},
  "clusterState": {
    "nodes": [{"name": "node-a", "ready": true, "cpuMillis": 4000, "memBytes": 8589934592}],
    "bandwidth": [{"from": "node-a", "to": "node-b", "bytesPerSec": 125000000}]
  }
}
```

and must return:

```json
{"assignments": [{"task": "t1", "node": "node-a"}], "estimatedMakespan": 42.0}
```

Note `runtimeProfile` and `dataSizeProfile`: these are the *measured*
per-(task, node) values Wayline's profiler learned from previous runs, and
they are the full matrix. A scheduler whose cost model is separable
(`cost / speed`, as SAGA's is) necessarily discards some of that; the
sidecar reports how much as `costModelFitRMSE` in its response, which is
worth watching when your algorithm underperforms on heterogeneous
workloads. `GET /healthz` should return 200.

## Checking it worked

```bash
kubectl logs deploy/odag-controller -n wl-system -c odag-controller | grep '\[saga\]'
# [saga] mypkg.MyScheduler placed 14 tasks (makespan estimate 21.4s, cost-model fit RMSE 0.163)
```

A line mentioning "falling back to random placement" means your scheduler
was not used; the same log line says why.
