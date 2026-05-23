# Getting Started with Wayline

A step-by-step walkthrough: from a fresh checkout to running your own DAG and
inspecting it. Assumes a working **k3s** cluster with `~/.kube/config` configured
and a local registry reachable at `<master-ip>:5000` (see
[README → Cluster setup](../README.md#cluster-setup) for first-time registry setup).

Throughout, replace `192.168.1.163:5000` with your registry if different.

---

## 1. Build the tools

```bash
git clone git@github.com:mali-kh/wayline.git
cd wayline
make build          # → bin/wayline, bin/odag-controller, bin/data-agent, bin/ui-server
./bin/wayline --help
```

## 2. Install the control plane

```bash
# CRDs (odags.wl.io, odagtemplates.wl.io), the wl-system namespace, and RBAC
make install

# Build the control-plane + example images and push them to the registry
make push-all

# Deploy the data-agent DaemonSet, odag-controller, and ui-server
make deploy
```

Verify everything is up:

```bash
kubectl get pods -n wl-system
# data-agent-xxxxx        Running   (one per node)
# odag-controller-xxxxx   Running
# ui-server-xxxxx         Running
```

If the controller is `Pending`, see
[README → Troubleshooting](../README.md#troubleshooting) (usually a master-node
toleration/`nodeSelector` mismatch).

## 3. Run the bundled example

The `dag-pipeline` example is a three-task ODAG: `generate → transform → output`.

```bash
make example-odag
# equivalently: ./bin/wayline apply -f examples/dag-pipeline/odag.yml
```

Watch it:

```bash
./bin/wayline get    odags
./bin/wayline status dag-pipeline
./bin/wayline logs   dag-pipeline generate
```

When `status` shows `phase: Succeeded`, you'll see a `makespan` and a per-task
table with the node each task ran on. Then clean up:

```bash
./bin/wayline delete dag-pipeline
```

Or open the UI at `http://<master-ip>:30080`.

---

## 4. Write your own DAG

A Wayline task is a container that uses the `wl` SDK. The SDK reads the `WL_*`
environment variables the controller injects, so task code never hard-codes
peers or paths.

**`tasks/producer/task.py`**

```python
from wl import WlTask

task = WlTask()
result = {"numbers": list(range(100))}
task.send(result)            # routed to all successors via the local data-agent
```

**`tasks/consumer/task.py`**

```python
from wl import WlTask

task = WlTask()
inputs = task.recv_all()     # {"producer": {...}} — a local file read
print("got", len(inputs["producer"]["numbers"]), "numbers", flush=True)
```

**`tasks/<name>/Dockerfile`** (built from the repo root so it can `COPY` the SDK):

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY tasks/<name>/task.py .
CMD ["python", "task.py"]
```

Build and push both images:

```bash
docker build -f tasks/producer/Dockerfile -t 192.168.1.163:5000/my-producer:latest .
docker build -f tasks/consumer/Dockerfile -t 192.168.1.163:5000/my-consumer:latest .
docker push 192.168.1.163:5000/my-producer:latest
docker push 192.168.1.163:5000/my-consumer:latest
```

**`my-dag.yml`**

```yaml
apiVersion: wl.io/v1
kind: ODAG
metadata:
  name: my-dag
  namespace: default
spec:
  scheduler: heft
  tasks:
    - name: producer
      image: 192.168.1.163:5000/my-producer:latest
      command: ["python", "task.py"]
      dependencies: []
      resources: { cpu: "200m", memory: "128Mi" }
      dataSize: "1MB"
      runtime: 5
    - name: consumer
      image: 192.168.1.163:5000/my-consumer:latest
      command: ["python", "task.py"]
      dependencies: ["producer"]
      resources: { cpu: "200m", memory: "128Mi" }
```

Submit and watch:

```bash
./bin/wayline apply  -f my-dag.yml
./bin/wayline status my-dag
./bin/wayline logs   my-dag consumer
```

---

## 5. Reusable templates and profiling

To run the same DAG repeatedly and let the scheduler learn task runtimes, use an
`ODAGTemplate` (set `kind: ODAGTemplate`, add a `profiling:` block) and create
runs from it:

```bash
./bin/wayline apply -f my-template.yml      # register the template (in wl-system)
./bin/wayline run   my-dag -n wl-system     # create run #1, #2, ...
./bin/wayline runs  my-dag -n wl-system     # list runs + makespans
./bin/wayline show  my-dag -n wl-system     # template detail + per-(task,node) profile
```

See `examples/rag-refresh-odag/` and `examples/multi-odag-heft/` for richer DAGs,
constraints, and template usage.

---

## 6. Where to go next

- [architecture.md](architecture.md) — how the data plane, state model, and
  scheduler fit together.
- [sdk-quickstart.md](sdk-quickstart.md) — the full `WlTask` API.
- [local-dev.md](local-dev.md) — running the UI server as a local binary.
- [../eval/EXPERIMENTS.md](../eval/EXPERIMENTS.md) — reproducing the paper results.
