#!/usr/bin/env python3
"""E18 Dask driver: the iot and chain workloads on Dask distributed, each task
pinned to its node in Wayline's plan, with the same task bodies as the Ray
driver and the Argo task (argo/e16_ray_body.py: E11's SHA-256 loop for the
chains, iot's capture / preprocess / infer / fuse / report), shipped to the
workers with Client.upload_file.

Placement: client.submit(fn, ..., key=<task>-<run>, workers=[planned node's
  data IP], pure=False). Pinned sources (iot's captures; --pins) are hard
  (allow_other_workers=False). With --pin soft (the default) every other task
  is soft (allow_other_workers=True; the scheduler runs with work stealing
  off): it runs on its planned node unless that node is gone, as with Ray's
  soft node affinity, so a lost node's tasks and objects can be rerun
  elsewhere. --pin hard pins every task (no task ever leaves its node; a lost
  node's tasks wait for it). Without a fault both place every task on its
  planned node.
CPU: with --cpu-resource each task asks for its template CPU request as the
  worker resource CPU (the accounting Ray's num_cpus does here).
Data: a task's inputs are its producers' futures; Dask moves each object
  worker to worker over the workers' data IPs when the consumer is about to
  run. No task data goes through the client (completion is observed, results
  are never gathered).
Copies (--copies {object: [node]}): once the object exists,
  client.replicate([future], n=1, workers=[node's IP]) makes a second replica
  that the scheduler tracks (its Active Memory Manager is off, or it would
  drop the replica): losing the primary's node then loses no data and
  recomputes nothing, and consumers read the surviving replica. The Dask
  analogue of Ray+copies, as one call per copied object and no fallback code
  in the consumers (Dask has replicas; Ray has to emulate them).
Policy (--repairs {"task", "nodes", "reprotect"}): the consumer is pinned hard;
  when its worker is lost the driver re-submits it pinned to the node
  Wayline's repair policy chose (next to the replica), then replicates its
  inputs to the node Wayline's copy policy chose; a second loss moves it there.
Recovery: when a worker is lost the scheduler recomputes its objects from
  lineage (unless a replica survives) and reschedules its running tasks
  (elsewhere for soft tasks; a hard task waits for a worker on its node: the
  driver prints NOWORKER for it).

Runs inside the scheduler pod (daskrun.py copies the files there):
  python e18_dask.py --scheduler tcp://10.20.0.1:8786 --tasks tasks.json --plan plan.json
                     --nodes nodes.json --body e16_ray_body.py [--pins pins.json] [--copies copies.json]
                     [--pin soft|hard] [--cpu-resource] [--run-id ID]
(tasks.json: [{name, role, runtime, size, cpu, deps, pinned}]; plan.json: {task: node};
nodes.json: {lane node: data IP}; pins.json: {task: node} for pinned tasks.)
Prints "DONE <task> <unix>" as tasks finish, then "RESULT <json>" in the Ray
drivers' shape (phase, completion, tasks {name: {task, node, start, end}},
errors), plus attempts per task, copies made and the replicas at the end.
"""
import argparse
import json
import sys
import threading
import time

from distributed import Client, get_worker, wait

TOPIC = "e18-task"


def run_task(name, role, runtime, size, *inputs):
    """One task: the shared body; its timing goes to the scheduler's event
    log (the result itself stays on the worker)."""
    import e16_ray_body  # uploaded with Client.upload_file
    start = time.time()
    out = e16_ray_body.body(name, role, runtime, size, list(inputs))
    end = time.time()
    w = get_worker()
    w.log_event(TOPIC, {"task": name, "node": w.address.split("://")[-1].rsplit(":", 1)[0],
                        "worker": w.name, "start": start, "end": end})
    return out


def host_of(addr):
    return addr.split("://")[-1].rsplit(":", 1)[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scheduler", required=True)
    ap.add_argument("--tasks", required=True)
    ap.add_argument("--plan", required=True, help="{task: node}")
    ap.add_argument("--nodes", required=True, help="{lane node: data IP}")
    ap.add_argument("--pins", help="{task: node}")
    ap.add_argument("--copies", help="{object: [node]}: replicas to make")
    ap.add_argument("--body", required=True)
    ap.add_argument("--pin", choices=["soft", "hard"], default="soft")
    ap.add_argument("--cpu-resource", action="store_true")
    ap.add_argument("--run-id", default=f"r{int(time.time() * 1000)}")
    ap.add_argument("--repairs", help='{"task": t, "nodes": [repair node, ...], "reprotect": [node, ...]}')
    a = ap.parse_args()

    tasks = json.load(open(a.tasks))
    plan = json.load(open(a.plan))
    ip = json.load(open(a.nodes))
    pins = json.load(open(a.pins)) if a.pins else {}
    copies = json.load(open(a.copies)) if a.copies else {}
    repairs = json.load(open(a.repairs)) if a.repairs else None
    spec = {t["name"]: t for t in tasks}
    repaired = []
    client = Client(a.scheduler, timeout=30)
    client.upload_file(a.body)
    seen_events = len(client.get_events(TOPIC))

    futs, made, copy_errors = {}, {}, {}
    lock = threading.Lock()

    def replicate(fut, name, nodes):
        """Copies of one object, once it exists (a done callback, in its own thread)."""
        if fut.status != "finished":
            return
        for node in nodes:
            t1 = time.time()
            try:
                client.replicate([fut], n=1, workers=[ip[node]])
                with lock:
                    made.setdefault(name, []).append({"node": node, "start": t1, "end": time.time()})
            except Exception as e:  # noqa: BLE001 - recorded, the run goes on without this copy
                with lock:
                    copy_errors[f"{name}->{node}"] = f"{type(e).__name__}: {str(e)[:200]}"

    t0 = time.time()
    for t in tasks:
        name = t["name"]
        hard = t["pinned"] or name in pins or a.pin == "hard" or bool(repairs and name == repairs["task"])
        target = pins.get(name) or plan[name]
        kw = {"key": f"{name}-{a.run_id}", "workers": [ip[target]], "allow_other_workers": not hard,
              "pure": False}
        if a.cpu_resource:
            kw["resources"] = {"CPU": t["cpu"]}
        futs[name] = client.submit(run_task, name, t["role"], t["runtime"], t["size"],
                                   *[futs[d] for d in t["deps"]], **kw)
        if copies.get(name):
            futs[name].add_done_callback(lambda f, n=name: replicate(f, n, copies[n]))

    pending = {f: n for n, f in futs.items()}
    done_at, errors, noworker = {}, {}, set()
    while pending:
        try:
            ready, _ = wait(list(pending), timeout=1.0, return_when="FIRST_COMPLETED")
        except TimeoutError:
            ready = set()
        for f in ready:
            name = pending.pop(f)
            if f.status == "finished":
                done_at[name] = time.time()
                print(f"DONE {name} {done_at[name]:.3f}", flush=True)
            else:
                try:
                    exc = f.exception(timeout=5)
                    errors[name] = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200] if str(exc) else ''}"
                except Exception as e:  # noqa: BLE001
                    errors[name] = f"{f.status}: {type(e).__name__}"
                print(f"FAILED {name} {errors[name]}", flush=True)
        if pending:
            keys = {f.key: n for f, n in pending.items()}
            stuck = client.run_on_scheduler(
                lambda dask_scheduler, keys=tuple(keys): [k for k in keys if k in dask_scheduler.tasks
                                                          and dask_scheduler.tasks[k].state == "no-worker"])
            for k in stuck:
                n = keys[k]
                if repairs and n == repairs["task"] and len(repaired) < len(repairs["nodes"]):
                    # POLICY: re-run the task where Wayline's repair policy put it
                    idx = len(repaired)
                    node = repairs["nodes"][idx]
                    old = next(f for f, nm in pending.items() if nm == n)
                    pending.pop(old)
                    client.cancel(old)
                    t = spec[n]
                    kw = {"key": f"{n}-{a.run_id}-r{idx + 1}", "workers": [ip[node]], "allow_other_workers": False,
                          "pure": False}
                    if a.cpu_resource:
                        kw["resources"] = {"CPU": t["cpu"]}
                    futs[n] = client.submit(run_task, n, t["role"], t["runtime"], t["size"],
                                            *[futs[d] for d in t["deps"]], **kw)
                    pending[futs[n]] = n
                    repaired.append({"at": time.time(), "to": node})
                    print(f"REPAIRED {n} -> {node} {time.time():.3f}", flush=True)
                    if idx < len(repairs.get("reprotect", [])):
                        to = repairs["reprotect"][idx]
                        for d in t["deps"]:   # POLICY: re-protect the inputs where Wayline's copy policy did
                            threading.Thread(target=lambda f=futs[d], w=ip[to]: client.replicate([f], n=1, workers=[w]),
                                             daemon=True).start()
                        print(f"REPROTECTED {n} inputs -> {to} {time.time():.3f}", flush=True)
                    continue
                if keys[k] not in noworker:
                    noworker.add(keys[k])
                    print(f"NOWORKER {keys[k]} {time.time():.3f}", flush=True)
    completion = time.time() - t0

    # Task timings from the workers' events (the last attempt of each task).
    metas, attempts = {}, {}
    deadline = time.time() + 15
    while True:
        attempts = {}
        for _, m in client.get_events(TOPIC)[seen_events:]:
            if m["task"] in futs:
                attempts.setdefault(m["task"], []).append(m)
        if all(n in attempts for n in done_at) or time.time() > deadline:
            break
        time.sleep(0.2)
    for n, ms in attempts.items():
        metas[n] = max(ms, key=lambda m: m["end"])
    # Copies still landing when the last task finished are waited for (and timed).
    t_copies = time.time()
    while any(f.status == "finished" and len(made.get(n, [])) + sum(1 for k in copy_errors if k.startswith(n + "->"))
              < len(copies.get(n, [])) for n, f in futs.items()) and time.time() - t_copies < 60:
        time.sleep(0.1)
    who = client.who_has(list(futs.values()))
    replicas = {n: sorted({host_of(w) for w in who.get(f.key, [])}) for n, f in futs.items()}
    try:
        info = client.scheduler_info(n_workers=-1)
    except TypeError:   # before n_workers existed every worker was listed
        info = client.scheduler_info()
    ok = len(done_at) == len(tasks) and not errors
    result = {"phase": "Succeeded" if ok else "Failed", "completion": completion, "tasks": metas,
              "errors": errors, "attempts": {n: len(ms) for n, ms in attempts.items()},
              "noWorker": sorted(noworker), "copiesMade": made, "copyErrors": copy_errors,
              "replicas": replicas, "pin": a.pin, "runId": a.run_id, "repaired": repaired,
              "workers": sorted({(host_of(w), str(v.get("name"))) for w, v in info["workers"].items()})}
    print("RESULT " + json.dumps(result), flush=True)
    client.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
