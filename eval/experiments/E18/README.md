# E18: E16's campaign on the rebuilt testbed

E16 (plan-driven copies under loss and link degradation, `../E16/PLAN.md`) ran
on eight UP boards with E13's link classes. E18 is the same campaign for the
rebuilt testbed: eclipse as k3s server, 65 workers (9 UP, 4 Pi 4, 52 Pi 3),
a control network and a data VLAN. Same workloads, faults, Wayline arms and
Ray / Argo baselines; what changed is how nodes, links and results are
handled:

- **Lanes**: a suite runs on a set of workers given by names, globs or label
  selectors (`lanes.py`). Workloads never name hosts: pinned sources and
  fault victims are roles ("camera#4", "victim") resolved per lane. The `e16`
  lane is E16's eight boards with E16's roles and link classes.
- **Links**: every directed pair of lane nodes, and each node's link to and
  from eclipse, is an HTB class on the data VLAN interface (`netem.py`, from
  E17's emulator). Nothing else is touched.
- **Results**: one JSON per run with the full ODAG status, flows, objects,
  recovery events, fault timestamps, link counters and a validity verdict;
  a manifest per suite; randomized blocks; resumable.

Nothing here has run yet. The code was checked with `python3 -m py_compile`
and offline tests of the lane, template, ordering, validity and report
logic against the live inventory and node list.

## Files

| File | What |
|---|---|
| `e18.py` | harness: `run`, `preflight`, `verify-links`, `calibrate`, `cleanup` |
| `arms.py` | every arm in one module (Wayline realizationPolicy per arm; baseline names) |
| `lanes.py` | inventory and node discovery, lane and role resolution, link classes, speed factors |
| `suites.py` | E16's matrix as rows (shared with the report), fault specs, suite definitions |
| `workloads.py` | templates for iot, GDTM, FOCAL, chain, chain-link, fan, the sweep, warm variants |
| `netem.py` | root shells and the link emulator (rates, cuts, partition, readback, counters) |
| `faults.py` | crash / restore, the out-of-service taint watcher |
| `kube.py` | `k3s kubectl`, the API through a private proxy, ODAG helpers, registry arch lookup |
| `ray/` | Ray cluster on a lane (`rayrun.py`) and the three drivers (E16's, by data IP, pins as input) |
| `argo/` | Argo workflows with an S3 repository on eclipse (`argorun.py`), task and stage wrappers, `install.sh` |
| `dask/` | Dask cluster on a lane (`daskrun.py`), the iot / chain driver (`e18_dask.py`), the image (`image.sh`) |
| `baselines.py` | the baselines outside the harness: up / down, plan probe, pinned runs, footprint (`BASELINES.md`) |
| `runner/` | iot warm-runner image (E16's) |
| `deploy/runners.py` | warm-runner DaemonSets selected by node class and image architecture |
| `matrix_e18.py` | the matrix and per-run details |
| `iot-base.json` | E16's iot DAG with role placeholders |
| `images.py` | image tags by set: `mx1` (multi-arch, default) or `e16` (E16's amd64-only), `E18_IMAGES` |
| `build.sh`, `deploy/buildkit.yml` | image builds (amd64 + arm64 through two BuildKit pods), the BuildKit pods |
| `sync.sh`, `run.sh` | copy to eclipse, venv wrapper |

## Running

The harness runs on eclipse (it needs `k3s kubectl`, the data VLAN and ssh to
the workers' control addresses). The workers' sudo password is read from the
first line of stdin, never from the environment, a file or the command line.
Commands that only read (`preflight`, `run --dry-run`) do not ask for it.

```sh
# on the Mac: copy E18 and the repo files it uses to eclipse:~/testbed/wl-e18
eval/experiments/E18/sync.sh

# on eclipse
cd ~/testbed/wl-e18/eval/experiments/E18
./run.sh preflight --suite smoke
./run.sh run --suite smoke --dry-run                 # lane, roles, rates, cells, block order
{ IFS= read -rs P; printf '%s\n' "$P"; } | ./run.sh verify-links --lane smoke
{ IFS= read -rs P; printf '%s\n' "$P"; } | ./run.sh run --suite smoke
python3 matrix_e18.py ~/E18-results/<date>_smoke --detail
```

The smoke suite (three UP boards, iot without a fault and chain-link under a
degraded link, arms W and W-plan-only) needs nothing beyond what the cluster
has today: pod tasks only, images in the registry, no node crash.

Long suites in the background (the password is read in the foreground):

```sh
IFS= read -rs P && (printf '%s\n' "$P" | nohup ./run.sh run --suite e16-wayline --reps 3 > e16w.log 2>&1 &); unset P
```

Options of `run`:

- `--reps N` repetitions; each is a block: systems (Wayline, Ray per image,
  Argo) in random order, cells within each in random order, seeded by
  `--seed` and the repetition, so a resumed suite replays the same order.
- Resuming is the default: `run` picks the latest results directory of the
  suite with the same configuration fingerprint (lane, roles, link rates,
  speed factors, cells, fence, arch mode, seed) and skips every cell
  repetition whose `rep<k>.json` exists. An invalid run is kept as
  `rep<k>.attempt<n>.invalid.json` and retried, up to `--max-attempts` (2).
  `--new` starts a fresh directory, `--out` names one. Adding reps later
  (`--reps 5`) extends the same directory. A changed controller or agent tag
  refuses to resume unless `--allow-mixed-builds`.
- `--lane` overrides the suite's lane: a `lanes.LANES` key or a string such as
  `anrg-1,anrg-2,testbed.lab/class=rpi4:1,testbed.lab/class=rpi3:13`
  (`label=value:N` takes N matching nodes, `:N+S` skips S first). `--roles
  'camera=rpi1,rpi2,rpi3,rpi4;victim=anrg-1'` and `--speed rpi3=0.2` override.
- `--rows`, `--systems`, `--arms`, `--claims`, `--workloads` filter cells.
- `--fence constraints|cordon|nodes` (see Lanes), `--arch strict|constrain`
  (see item 3 below).
- `cleanup --lane L [--out DIR]` undoes what an interrupted session left
  (shaping, the bandwidth ConfigMap, cordons, out-of-service taints, Ray
  pods, the Argo repository, leftover runs).

Suites (`suites.py`): `smoke`; `e16` (the whole matrix on the `e16` lane, 118
cells), `e16-wayline`, `e16-baselines`; `mixed16` and `all65` (Wayline rows
for heterogeneous lanes; their iot and chain images exist for arm64, see below).

## Lanes and roles

A lane spec (`lanes.LANES`) has `nodes` and/or `select` items (name, glob, or
`{"label": "testbed.lab/class=rpi3", "count": 13, "skip": 0}`), `roles`
(explicit lists or selectors over the lane), `links` and `repo_class`.

Roles used by the workloads:

| Role | Used by | E16 node |
|---|---|---|
| `camera#1..4` | iot capture-1..4 pins; survival victim `camera#4` | anrg-1, 3, 4, 5 |
| `sensor#1..3` | GDTM source-n1..n3 pins; survival victim `sensor#2` | anrg-4, 5, 9 |
| `source#1..2` | FOCAL seismic, audio pins; survival victim `source#2` | anrg-4, 9 |
| `victim` | chain: p and c fast here; chain-link / fan: p's node; crash, crash2, partition, link victim | anrg-7 |
| `dst` | chain-link / fan: c fast here; link-pair peer | anrg-6 |
| `holder` | fan: d fast here (holds a second copy of p's output) | anrg-1 |

The controller plans over every schedulable worker. When a lane is a subset,
every unpinned task gets `constraints.nodeNames` = the lane's nodes (HEFT,
recovery and the plan-aware copy policy all honor it). W-static does not: it
places copies by a rendezvous hash over the whole cluster. Cells with
W-static on a subset lane therefore need `--fence cordon`, which cordons the
workers outside the lane for the suite and uncordons them after (the `e16`
and `e16-wayline` suites default to it; preflight refuses otherwise).
`--fence nodes` sets `spec.nodes` = the lane on every run instead, for a
controller that supports it (the uncommitted controller work in this tree
adds it; the deployed milcom9 does not, and preflight checks the installed
CRD). Every run is also checked for nodes outside the lane.

## Links

Pair rate u -> v = min(B / max(k_u, k_v), NIC cap of u, NIC cap of v), with
k the node's link class (E13: a class-k node has B/k), B = 942 Mbit/s (E13's
measured rate), caps 942 (UP, Pi 4) and 94 (Pi 3, 100BASE-TX). The `e16` lane
uses E13's classes; new lanes default to uniform. eclipse's links (the Argo
repository, the Ray head, the controller's calls to agents and runners) get
`repo_class` (default 1) and are shaped in every arm. The controller's
bandwidth ConfigMap gets the lane's pair rates for the suite and is restored
after.

Shaping: an HTB tree with handle `18:` on each node's data VLAN interface
(`enp1s0.20`, `eth0.20`, `enp4s0f0.20` on eclipse), one class per peer
matched by its MAC, fq_codel leaves, a 10 Gbit/s default class for
everything else. Setup refuses an interface whose root is another HTB (an
E17 lane). Cuts drop frames to the peer's MAC and reset TCP to its data IP
(nftables table `inet wle18`).

Faults on links: `link` sets every link of a node (eclipse's included) to
4 Mbit/s right after the run is planned; `link-pair` only one pair;
`partition` cuts every data link of a node both ways, eclipse included, and
heals after the run. Before each run the emulator's state is read back and
compared with the intended rates (tc keeps whole bytes/s; a printed rate must
bracket the set one); after the run the per-class byte counters are kept.

`verify-links` measures one link per distinct rate and one to eclipse with a
few lines of Python on both ends (receiver-side goodput; no iperf).

## Arms and baselines

| Arm | Stands for | Configuration |
|---|---|---|
| W | the thesis | `{mode: planAware, revise: true}`: plan copies, link re-planning, re-protection after a repair |
| W-plan-only | Jupiter-style placement, lineage recovery | `{mode: none, revise: false}` |
| W-static | Arvados replication level / Rucio rule | `{mode: static, replicas: 2, revise: false}` |
| W-frozen | Iridium-style plan-time copies | `{mode: planAware, revise: false}` |
| W-replan | Tactical-Jupiter-style re-planning, no copies | `{mode: none, revise: true}` |
| Ray | plan-pinned placement, lineage reconstruction | Ray 2.58 on the lane, tasks pinned to Wayline's plan (soft), sources hard; head on eclipse with 0 CPUs, every node started with `--node-ip-address <data IP>` |
| Ray+copies | a Ray user emulating copies in application code | as Ray, plus `hold` copies of exactly the objects W's policy copies, on the same nodes, and a fallback in each consumer |
| Argo | central artifact repository | Argo Workflows v3.5.5, every artifact in an S3 repository (`rclone serve s3`) on eclipse's data address, plan placement preferred, retry on another node, 5 s eviction |
| Argo+oos | Argo plus node-lifecycle automation | as Argo, and the crashed node tainted `out-of-service` once NotReady (force-deletes its pods) |
| Dask | plan-pinned placement, lineage recomputation | Dask distributed 2026.8.0 on the lane, scheduler on eclipse's data address, one worker per node bound to its data IP; tasks pinned by host to Wayline's plan (sources hard, the rest soft, work stealing off) |
| Dask+copies | Dask's own replicas of W's copies | as Dask, plus `client.replicate` of exactly the objects W's policy copies, to the same nodes (the Active Memory Manager off so they stay) |

The Wayline arms share one runtime and differ only in `realizationPolicy`
(`arms.py`; the module is the single place to swap when the policies move to
an external process). Node-loss recovery is on in every Wayline arm.
Ray and Argo follow the plan of a probe run: once per suite and workload,
E18 creates the workload's W run, reads its plan and its plan-time copy
decisions, deletes it, and stores them in `plans/<workload>.json`.

Workloads: iot (14 tasks, 4 pinned cameras, 80 to 150 MB captures), GDTM (29
tasks, 3 pinned sensors), FOCAL (7 tasks, 2 pinned sources), chain (12 s
producer, 8 MB, co-located consumer), chain-link (30 MB planned transfer),
fan (chain-link plus a second consumer holding a copy), the sweep
`chain-r{2,12,30}-s{8,100}`; `-warm` variants run every task on warm runners
(the dispatch Ray uses). Faults: crash (k3s-killall on the victim, a delay
after a trigger task), crash2 (then the consumer's new node), partition,
link, link-pair (see `suites.py`).

## Results and validity

```
~/E18-results/<date>_<suite>/manifest.json        suite, lane (nodes, addresses, roles, link classes, pair rates),
                                                  controller / agent images and settings, git commit, harness file
                                                  hashes, arms, images, cells, block order per rep, sessions
~/E18-results/<date>_<suite>/<cell>/rep<k>.json   a valid run
~/E18-results/<date>_<suite>/plans/<workload>.json
~/E18-results/<date>_<suite>/state.json           last link fault, cordoned nodes, saved ConfigMap
```

A Wayline record holds the full ODAG status (tasks, predicted tasks, flows,
objects, recovery events, realizationPolicy), agent timings, digests of every
copy of every object, fault timestamps (`plannedAt`, `triggerAt`, `faultAt`,
`fault2At`, `victim2`, `unlinkAt`, `restoredAt`), link readbacks, link byte
counters per pair and data-agent counter deltas. Ray and Argo records hold the
driver's or workflow's result, plan, copies, fault timestamps and counters.

A run is valid when:

- it ended Succeeded or Failed (or hit the time limit where that is the
  expected outcome: plain Argo on a crash);
- its fault was injected as specified (a crash whose trigger never finished is
  not a crash run);
- the links read back as intended before the run and after a link fault;
- (Wayline) every completed transfer's payload crossed the shaped class of its
  pair (class bytes >= payload bytes: wire bytes are never fewer, fewer means
  an unshaped path);
- (Wayline) every object held on two or more nodes has equal digests on at
  least two of them (a succeeded run must verify), and no agent counted a
  checksum mismatch;
- no task, copy or transfer left the lane.

`python3 matrix_e18.py DIR [--detail] [--invalid]` prints E16's matrix from
the valid runs (mean completion with the number of runs, `fails`, `hangs`,
`invalid`, `missing`, `= Arm`) and, with `--detail`, one line per run.

## Before the first real suite (needs decisions; nothing done)

1. **Controller settings.** The deployed controller is E17's: `WL_NODE_LOSS=off`
   (no node-loss detection, so no recovery from a crash or partition), poll
   100 ms, status pacing 250 ms, QPS 400. E16 ran with node-loss detection on
   and the default poll. Preflight refuses crash and partition cells while it
   is off. To turn it on: `k3s kubectl -n wl-system set env deploy/odag-controller WL_NODE_LOSS-`.
   Which poll and pacing E18 should use is open; the manifest records them.
2. **Agent settings.** E17's agents run `WL_PUSH_RETRIES=0` (retry until the
   push succeeds), `--sync=none`, unbounded pushes; E16 ran the defaults
   (5 retries). Open; recorded per suite.
3. **Images (2026-11-08).** `images.py` picks the tags; `E18_IMAGES=mx1`
   (default) is linux/amd64 + linux/arm64 for wl-e11, wl-runner-e11,
   wl-runner-iot, wl-iot-{capture,preprocess,infer,fuse,report},
   wl-e16-task, wl-focal (`:mx1`) and wl-ray-baseline (`:2.58.0-mx1`), and
   the upstream Dask image `dask:2026.8.0-py3.12` (ghcr.io/dask/dask, mirrored;
   not what the Dask arm runs: `wl-dask-baseline:2026.8.0`, see BASELINES.md). Each
   ran once on an UP board, a Pi 4 and a Pi 3. The mx1 amd64 variants are
   rebuilds (current SDK; Python 3.12.16 / 3.11.18 where E16's had 3.12.14 /
   3.11.16; wl-focal pinned to E16's torch 2.14.0, torchvision 0.29.0, timm
   1.0.30, numpy 2.5.2). `E18_IMAGES=e16` selects E16's tags (`:latest`,
   `wl-ray-baseline:2.58.0`), all amd64-only; a suite's fingerprint includes
   the set, so the two never share a results directory. Still amd64-only:
   wl-gdtm (could build for arm64: torch CPU, OpenCV, h5py have aarch64
   wheels; not built), wl-gdtm-model (mmcv-full 1.7.1 has no aarch64 wheel,
   only its sdist, so its C++ ops would compile against torch 1.12 on the
   Pi, not tried; the pinned torch 1.12.1+cpu exists only for x86_64, aarch64
   has plain 1.12.1; its 2 GiB stages do not fit a Pi 3 either) and
   wl-gdtm-ray; wl-focal-ray and the Argo stage images are not built (item
   4). `--arch constrain` narrows tasks to the nodes their images run on
   (pinned sources on a Pi still refuse). Builds: `build.sh` (header) with
   the BuildKit pods of `deploy/buildkit.yml` (arm64 on rpi4-3, amd64 on
   eclipse; eclipse's Docker cannot run buildx's docker-container driver).
4. **Missing images.** wl-focal-ray, wl-gdtm-argo, wl-gdtm-model-argo and
   wl-focal-argo were never built (E16's gap-filling script did not run) and
   are not in the registry: `build.sh heavy`. FOCAL's Ray image and the Argo
   FOCAL tasks need the working-tree `examples/focal-mod/gen_focal.py`
   (`stages()`, `PINS`), which is not committed.
5. **Warm runners** for e11, iot, gdtm, gdtm-model and focal are not deployed
   (only E17's runners are): `python3 deploy/runners.py --runners e11,iot,gdtm,gdtm-model,focal --classes up`
   prints the DaemonSets (by `testbed.lab/class` and architecture).
6. **Argo Workflows v3.5.5 is installed** (2026-11-08, `argo/install.sh`; `argo/install.sh down`
   removes it). The S3 repository listens on 10.20.0.1:9500, not 9000: a Docker swarm service on
   eclipse publishes 9000 (BASELINES.md).
7. **Ray on Pi 3** (1 GB, about 900 MB allocatable, 100 Mbit/s NIC): raylet,
   an object store of 30% of memory by default and Python workers leave
   little room; iot's 80 to 150 MB objects will not fit. Exclude Pi 3 from
   Ray lanes, or cap `--object-store-memory` and accept spilling. Ray 2.58
   waits a fixed 30 s for the raylet to register: an idle Pi 3 made it in
   about 20 s (rpi30, rpi36), a busy one (a pull or another pod) did not. Ray's head
   on eclipse needs ports 6379, 8076, 8077, 8265, 10001 (free today; its
   worker ports stay off 10010, which is in use there).
8. **Speed factors.** Chain tasks hash a fixed amount, so on a Pi they run
   longer; their runtime profiles divide by a per-class factor that only UP
   has (1.0). `calibrate --lane L` measures E11's kernel per class (pods of
   wl-e11:mx1 on every class; one bench each on 2026-11-08: anrg-4 2.49,
   rpi4-3 0.34, rpi30 0.074 Mhash/s, so about 0.14 and 0.03, far below
   E17's kernel ratios). GDTM and FOCAL keep E16's scalar runtimes (HEFT
   plans as if every node were an UP board, which FOCAL on a Pi is not);
   iot's bodies run for their runtime on any node.
9. **Link classes and caps** for new lanes (uniform by default; E13's classes
   are defined only for E16's eight boards) and the B and Pi 3 cap values
   (`verify-links`).
10. **Fencing** a subset lane for W-static cordons up to 57 workers during the
   suite with the deployed controller. Once a controller with `spec.nodes`
   is deployed, `--fence nodes` replaces the cordon (make it the suites'
   default then).
11. **Partition** is now a data-plane partition: the control network stays up,
   so Kubernetes keeps the node Ready and its pods running; the controller
   loses the node through its agent probes (5 s grace). E16's partition
   (never run) also cut kubelet, API server and VXLAN traffic.
12. **Repository link.** eclipse's links are shaped in every arm, including the
   controller's calls to agents and runners over the data VLAN; E16 left the
   repository unshaped (favoring Argo) and throttled it only for Argo's
   link faults.
13. **Ray+copies** take W's plan-time copy decisions from the probe, where E16
   took the copies W's completed run had made; the same unless a copy failed
   to land.
14. **Image age.** The iot task images date from 2026-08-21 and wl-e11 from
   2026-09-25; they talk to the agent through the SDK of then. The smoke test
   shows whether that still matches controller milcom9 and agent milcom2.
15. **Timing hygiene.** Monitoring DaemonSets run on every worker (preflight
   warns); anrg-7, the `e16` lane's victim, ran hot under sustained load in
   E17; E11's hash rate assumes UP boards at full clock.
16. **Dask baseline** (arms Dask, Dask+copies; `dask/`): written and validated on three UP
   boards without faults (BASELINES.md); not yet run under the harness's faults and shaping.

## Differences from E16 on purpose

- Link faults are injected once the run is planned (E16: right after
  submission, racing the plan).
- The link-evidence cooldown (65 s) is kept in `state.json` and applies to
  every Wayline run after a link fault or partition (E16: only before link
  cells, only within one process).
- A crash whose trigger never finished is not injected; the run is invalid
  (E16 crashed the node anyway after 300 s).
- Outcomes are recorded (`Succeeded`, `Failed`, `Timeout`, `Error`) instead of
  inferring a hang from the wall time in the report.
- Health checks look only at the lane's nodes, their agents (on the data IP)
  and the runners the next cell calls.
