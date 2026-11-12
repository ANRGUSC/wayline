#!/usr/bin/env python3
"""Functional checks for policies outside the controller (rebuilt testbed).

Not an experiment: each scenario runs once and asserts behavior.

DAG: p (about 12 s, one 8 MB output "emb") -> c (about 20 s), confined to
three UP boards by spec.nodes. Runtime profiles make HEFT co-locate both on
the victim, so p's output has a single planned copy there.

Scenarios (the policy process, wl-policy, must be running):
  plan-crash   realizationPolicy planAware: the policy copies p.emb to c's
               fallback node; crash the victim 3 s after p succeeds; the
               policy moves c to the copy's node (spec.placement), the
               controller restarts it there, p does not run again.
  none-crash   realizationPolicy mode none (repair still on): same crash; the
               policy asks p to run again (spec.rerun) elsewhere, then c.
  none-crash2  mode none, two losses: crash the victim after p succeeds, then
               crash the node p ran again on once it succeeds there; the
               policy must ask p to run a second time (spec.rerun p: 2, enacted
               as status.rerunEnacted p: 2) and the run completes.
  nopolicy     no realizationPolicy: same crash; nothing repairs the run, its
               tasks on the victim show "lost": true and the run waits.
  link         p (30 MB output) on anrg-4, c planned on anrg-5; right after
               submission the anrg-4 -> anrg-5 data link is throttled to
               4 Mbit/s. The controller judges the transfer late and publishes
               the link as degraded; the policy (revise) moves c to a node the
               object reaches quickly.
  link-frozen  the same with revise false: nothing moves, c waits for the slow
               transfer.

Run on eclipse; the sudo password for the victim comes on the first line of
stdin (crash and restore need it):
  printf '%s\\n' "$PW" | python3 ft_policy.py plan-crash none-crash nopolicy
"""
import json
import subprocess
import sys
import time

NS = "wl-system"
IMAGE = "10.10.0.1:5000/wl-e11:latest"
RATE = 2.568e6            # hashes/s on a full-clock UP board (E11 calibration)
LANE = ["anrg-3", "anrg-4", "anrg-5"]
VICTIM = "anrg-4"
CTL = {"anrg-3": "10.10.0.13", "anrg-4": "10.10.0.14", "anrg-5": "10.10.0.15"}
PASSWORD = sys.stdin.readline().rstrip("\n")


def kubectl(args, stdin=None):
    return subprocess.run(["k3s", "kubectl", "-n", NS] + args, input=stdin, capture_output=True, text=True)


def profile(fast, slow):
    return {n: (fast if n == VICTIM else slow) for n in LANE}


def odag_link(name, policy):
    """p fast on anrg-4 with a 30 MB output; c fast on anrg-5."""
    prof_p = {"anrg-3": 16, "anrg-4": 12, "anrg-5": 16}
    prof_c = {"anrg-3": 24, "anrg-4": 24, "anrg-5": 20}
    spec = {"scheduler": "heft", "nodes": LANE, "realizationPolicy": policy, "tasks": [
        {"name": "p", "image": IMAGE, "command": ["python", "task.py"],
         "outputs": [{"name": "emb", "dataSize": "30MB"}], "runtime": 12, "runtimeProfile": prof_p,
         "resources": {"cpu": "2", "memory": "512Mi"},
         "env": [{"name": "E11_ITERS", "value": str(int(12 * RATE))}, {"name": "E11_INPUTS", "value": ""},
                 {"name": "E11_OUTPUTS", "value": "emb:30000000"}]},
        {"name": "c", "image": IMAGE, "command": ["python", "task.py"], "dependencies": ["p"],
         "inputs": [{"producer": "p", "object": "emb"}], "runtime": 20, "runtimeProfile": prof_c,
         "resources": {"cpu": "2", "memory": "512Mi"},
         "env": [{"name": "E11_ITERS", "value": str(int(20 * RATE))}, {"name": "E11_INPUTS", "value": "p.emb"},
                 {"name": "E11_OUTPUTS", "value": ""}]},
    ]}
    return {"apiVersion": "wl.io/v1", "kind": "ODAG",
            "metadata": {"name": name, "namespace": NS, "labels": {"wl.io/experiment": "ft-policy"}},
            "spec": spec}


def throttle(node, dst_ip, mbit):
    dev = "enp1s0.20"
    node_shell(node, f"tc qdisc del dev {dev} root 2>/dev/null; tc qdisc add dev {dev} root handle 1: htb default 10 && "
                     f"tc class add dev {dev} parent 1: classid 1:10 htb rate 1gbit && "
                     f"tc class add dev {dev} parent 1: classid 1:20 htb rate {mbit}mbit && "
                     f"tc filter add dev {dev} parent 1: protocol ip prio 1 u32 match ip dst {dst_ip}/32 flowid 1:20")


def unthrottle(node):
    node_shell(node, "tc qdisc del dev enp1s0.20 root 2>/dev/null; true")


def run_link(sc):
    policy = {"mode": "none", "revise": sc == "link"}
    name = f"ft-pol-{sc}-{int(time.time()) % 100000}"
    r = kubectl(["apply", "-f", "-"], stdin=json.dumps(odag_link(name, policy)))
    if r.returncode:
        raise SystemExit(r.stderr)
    t0 = time.time()
    throttle("anrg-4", "10.20.0.15", 4)
    try:
        while time.time() - t0 < 300:
            st = get(name).get("status", {})
            if st.get("phase") in ("Succeeded", "Failed"):
                break
            time.sleep(1)
        o = get(name)
        st, spec = o.get("status", {}), o.get("spec", {})
        took = time.time() - t0
        print(f"  {sc}: phase {st.get('phase')} after {took:.1f}s; placement {spec.get('placement')}", flush=True)
        for d in (st.get("policy") or {}).get("decisions", []):
            print(f"    policy: {d.get('kind')} moved={d.get('moved')} copies={d.get('copies')}", flush=True)
        for t in ("p", "c"):
            e = task(st, t)
            print(f"    {t}: {e.get('phase')} on {e.get('node')}", flush=True)
        moved = task(st, "c").get("node") != "anrg-5"
        ok = st.get("phase") == "Succeeded" and (moved if sc == "link" else not moved)
        print(f"  {sc}: {'PASS' if ok else 'FAIL'}", flush=True)
        return ok
    finally:
        unthrottle("anrg-4")
        kubectl(["delete", "odag", name, "--ignore-not-found", "--wait=false"])


def odag(name, policy):
    spec = {"scheduler": "heft", "nodes": LANE, "tasks": [
        {"name": "p", "image": IMAGE, "command": ["python", "task.py"],
         "outputs": [{"name": "emb", "dataSize": "8MB"}], "runtime": 12, "runtimeProfile": profile(12, 16),
         "resources": {"cpu": "2", "memory": "512Mi"},
         "env": [{"name": "E11_ITERS", "value": str(int(12 * RATE))}, {"name": "E11_INPUTS", "value": ""},
                 {"name": "E11_OUTPUTS", "value": "emb:8000000"}]},
        {"name": "c", "image": IMAGE, "command": ["python", "task.py"], "dependencies": ["p"],
         "inputs": [{"producer": "p", "object": "emb"}], "runtime": 20, "runtimeProfile": profile(20, 24),
         "resources": {"cpu": "2", "memory": "512Mi"},
         "env": [{"name": "E11_ITERS", "value": str(int(20 * RATE))}, {"name": "E11_INPUTS", "value": "p.emb"},
                 {"name": "E11_OUTPUTS", "value": ""}]},
    ]}
    if policy is not None:
        spec["realizationPolicy"] = policy
    return {"apiVersion": "wl.io/v1", "kind": "ODAG",
            "metadata": {"name": name, "namespace": NS, "labels": {"wl.io/experiment": "ft-policy"}},
            "spec": spec}


def get(run):
    r = kubectl(["get", "odag", run, "-o", "json"])
    return json.loads(r.stdout) if r.returncode == 0 else {}


def task(st, name):
    for t in st.get("tasks", []) or []:
        if t.get("name") == name:
            return t
    return {}


def node_shell(node, cmd):
    """Run cmd as root on node, the password on sudo's stdin."""
    return subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
                           f"anrg@{CTL[node]}", f"sudo -S -p '' sh -c '{cmd}'"],
                          input=PASSWORD + "\n", capture_output=True, text=True, timeout=120)


def crash(node):
    node_shell(node, "/usr/local/bin/k3s-killall.sh >/dev/null 2>&1")


def restore(node):
    node_shell(node, "systemctl start k3s-agent")
    for _ in range(120):
        r = kubectl(["get", "node", node, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"])
        a = kubectl(["get", "pods", "-l", "app=data-agent", "--field-selector", f"spec.nodeName={node}",
                     "-o", "jsonpath={.items[0].status.phase}"])
        if r.stdout.strip() == "True" and a.stdout.strip() == "Running":
            time.sleep(8)
            return
        time.sleep(2)
    raise SystemExit(f"{node} did not come back")


def run_scenario(sc):
    if sc.startswith("link"):
        return run_link(sc)
    policy = {"plan-crash": {"mode": "planAware"}, "none-crash": {"mode": "none"}, "none-crash2": {"mode": "none"},
              "nopolicy": None}[sc]
    name = f"ft-pol-{sc}-{int(time.time()) % 100000}"
    kubectl(["delete", "odag", name, "--ignore-not-found"])
    r = kubectl(["apply", "-f", "-"], stdin=json.dumps(odag(name, policy)))
    if r.returncode:
        raise SystemExit(r.stderr)
    t0 = time.time()
    crashed = None
    victims = []
    try:
        while time.time() - t0 < 400:
            o = get(name)
            st = o.get("status", {})
            if crashed is None and task(st, "p").get("phase") == "Succeeded":
                placed = task(st, "p").get("node")
                if placed != VICTIM:
                    print(f"  {sc}: p ran on {placed}, not the victim; HEFT did not co-locate", flush=True)
                time.sleep(3)
                print(f"  {sc}: crash {VICTIM} at {time.time() - t0:.1f}s", flush=True)
                crash(VICTIM)
                victims.append(VICTIM)
                crashed = time.time()
            elif (sc == "none-crash2" and len(victims) == 1 and task(st, "p").get("phase") == "Succeeded"
                  and task(st, "p").get("node") not in victims):
                second = task(st, "p").get("node")
                time.sleep(3)
                print(f"  {sc}: p ran again on {second}; crash it at {time.time() - t0:.1f}s", flush=True)
                crash(second)
                victims.append(second)
            if st.get("phase") in ("Succeeded", "Failed"):
                break
            if sc == "nopolicy" and crashed and time.time() - crashed > 40:
                break
            time.sleep(1)
        o = get(name)
        st = o.get("status", {})
        spec = o.get("spec", {})
        print(f"  {sc}: phase {st.get('phase')} after {time.time() - t0:.1f}s", flush=True)
        print(f"    placement {spec.get('placement')}  rerun {spec.get('rerun')}", flush=True)
        print(f"    realization {[(e['object'], e.get('copies'), e.get('origin')) for e in spec.get('realization', [])]}",
              flush=True)
        for d in (st.get("policy") or {}).get("decisions", []):
            print(f"    policy: {d.get('kind')} moved={d.get('moved')} rerun={d.get('rerun')} copies={d.get('copies')}",
                  flush=True)
        for rec in st.get("recovery", []) or []:
            print(f"    enacted: {rec.get('reason')} {rec.get('plan')}", flush=True)
        for t in ("p", "c"):
            e = task(st, t)
            print(f"    {t}: {e.get('phase')} on {e.get('node')} lost={e.get('lost', False)}", flush=True)
        ok = {
            "plan-crash": st.get("phase") == "Succeeded" and not spec.get("rerun"),
            "none-crash": st.get("phase") == "Succeeded" and "p" in (spec.get("rerun") or {}),
            "none-crash2": (st.get("phase") == "Succeeded" and len(victims) == 2
                            and (spec.get("rerun") or {}).get("p") == 2 and (st.get("rerunEnacted") or {}).get("p") == 2),
            "nopolicy": st.get("phase") == "Running" and task(st, "c").get("lost") is True,
        }[sc]
        print(f"  {sc}: {'PASS' if ok else 'FAIL'}", flush=True)
        return ok
    finally:
        for v in victims:
            restore(v)
        kubectl(["delete", "odag", name, "--ignore-not-found", "--wait=false"])


if __name__ == "__main__":
    results = {sc: run_scenario(sc) for sc in sys.argv[1:]}
    print(json.dumps(results))
    sys.exit(0 if all(results.values()) else 1)
