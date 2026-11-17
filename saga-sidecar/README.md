# SAGA scheduler sidecar

Bridges Wayline's scheduler/data-plane interface to
[SAGA](https://github.com/ANRGUSC/saga), the ANRG library of DAG scheduling
algorithms — so any ODAG can be scheduled by a SAGA algorithm by setting

```yaml
spec:
  scheduler: saga/heft   # or saga/cpop, saga/minmin, saga/peft, ...
```

`GET /algorithms` lists the roster. On any sidecar failure the controller
falls back to random placement (placement quality degrades, availability
does not) and records it in the run's `status.scheduling` (`used: random`,
`fallbackError`).

## How it works

The controller (`cmd/odag-controller/saga.go`) densifies its runtime,
data-size, and bandwidth resolvers into full matrices and POSTs them to
`/schedule` using the JSON contract from `sdk/python/wl/scheduler.py`.
The sidecar converts to SAGA's `Network`/`TaskGraph` model and returns the
task→node assignment; only the placement is load-bearing (dispatch stays
data-readiness-driven, mirroring ncsim's SAGA adapter).

Model-conversion details (rank-1 cost fit, bandwidth symmetrization,
super-node stripping, constraint post-override) are documented in
`bridge.py`'s module docstring. The response reports `costModelFitRMSE`,
the log-space residual of fitting Wayline's true per-(task,node) runtime
matrix into SAGA's separable `cost/speed` model — 0.0 when heterogeneity
is expressible exactly, larger when information is lost.

## Run locally

```bash
uv venv --python 3.12 && uv pip install anrg-saga pytest
python server.py --port 8090
pytest test_bridge.py                       # unit tests
go test ./cmd/odag-controller -run TestSaga # Go integration tests (skip if sidecar down)
```

## Node model (opt-in)

By default a node is one SAGA node whose capacity is its free CPU. A request
(or an ODAG's `spec.schedulerConfig.model`, which the controller forwards) can
ask for the classic model instead, where every node is C one-task-at-a-time
processors:

```yaml
schedulerConfig:
  model:
    nodeModel: cores                      # SAGA nodes "<node>/c<i>", capacity 1
    cores: {node-a: 8, node-b: 8}         # default: floor(cpuMillis / 1000)
    selfBandwidth: {node-a: 2.0e9}        # self-loop (same core), default 1e12
    intraBandwidth: 1.0e9                 # between cores of one node, default 1e12
```

Assignments still name the node (placement) and add `core`; the controller
keeps both in `status.scheduling.externalAssignments`. With SAGA from PyPI
(no capacity model) only `cores` mode is accepted, and only when runtimes fit
cost/speed exactly; the default model is refused rather than silently
mis-modelled. `pytest test_bridge_cores.py` runs with any SAGA;
`test_bridge.py` needs feature/capacity.

## Use the bridge offline

The package installs the `bridge` and `server` modules, so another project
can schedule a request exactly as the controller would, without a cluster:

```bash
pip install ./saga-sidecar        # or, with uv:
# [tool.uv.sources] wl-saga-sidecar = { git = "https://github.com/ANRGUSC/wayline", subdirectory = "saga-sidecar" }
python -c 'import bridge, json; print(bridge.schedule_request(json.load(open("req.json"))))'
```

`req.json` is the body the controller POSTs (archived per call when
`WL_SAGA_DUMP_DIR` is set). Install the same SAGA build as the sidecar image
(see `Dockerfile`), and set `PYTHONHASHSEED=0` as the deployment does:
SAGA breaks ties in string-hash order. The response reports `sagaVersion`,
which the controller records in `status.scheduling.externalSagaVersion`.

## Deploy

Built into the odag-controller pod as a second container (see
`deployments/odag-controller/deployment.yml`); the controller reaches it at
`127.0.0.1:8090` (override with `WL_SAGA_SCHEDULER_URL`).
