"""Where a Wayline run's time goes along its critical path, edge by edge.

Walks back from the last task to close, each time to the input that became
available last on the consumer's node (its install time there), and splits
every step into:
  compute   the producer's compute (SDK compute interval)
  handoff   compute end -> output installed locally (SDK serialize + PUT + fsync)
  queue     local install done -> push started (hash, push-slot wait)
  transfer  push duration (network + remote install), against plan
            (bytes / pair rate at measured goodput)
  dispatch  input installed on the consumer's node -> consumer started
Same-node edges have no queue/transfer; their input is ready at handoff.

Usage: python3 datapath.py <results dir> <arm>
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import net

d, arm = sys.argv[1], sys.argv[2]
dag = json.load(open(f"{d}/dag.json"))["tasks"]
setup = json.load(open(f"{d}/setup.json"))
gp = (setup.get("network") or {}).get("goodput_factor", 1.0)
R = next(r for r in json.load(open(f"{d}/results.json")) if r["arm"] == arm)
T, pl, dp = R["timings"], R["placement"], R["data_path"]
flows = {(f["fromTask"], f["toTask"]): f for f in dp["flows"]}
inst = {k: v["unixTime"] for k, v in dp["installs"].items()}
t0 = R["submitted"]

def ready(t):
    """(time the task's binding input was installed on its node, producer, object)."""
    best = (t0, None, None)
    for p, o in dag[t]["inputs"]:
        k = f"{t}<-{p}.{o}"
        at = inst.get(k)
        if at is None:  # same-node input: ready once the producer installed it
            at = T[p]["computeEndUnix"] + T[p]["handoffSeconds"]
        best = max(best, (at, p, o))
    return best

last = max(T, key=lambda t: T[t]["closeUnix"])
chain, t = [], last
while True:
    at, p, o = ready(t)
    chain.append((t, at, p, o))
    if p is None:
        break
    t = p
chain.reverse()

tot = dict(compute=0, handoff=0, queue=0, transfer=0, transfer_plan=0, dispatch=0, start=0)
print(f"{arm}: makespan {T[last]['closeUnix'] - t0:.1f}s")
print(f"{'edge':22} {'compute':>7} {'handoff':>7} {'queue':>6} {'xfer':>6} {'(plan)':>6} {'MB':>5} {'dispatch':>8}")
for i, (t, at, p, o) in enumerate(chain):
    x = T[t]
    tot["dispatch"] += x["taskStartUnix"] - at if p else 0
    tot["start"] += x["taskStartUnix"] - t0 if not p else 0
    comp = x["computeEndUnix"] - x["computeStartUnix"]
    tot["compute"] += comp
    nxt = chain[i + 1] if i + 1 < len(chain) else None
    hand = x["handoffSeconds"]
    tot["handoff"] += hand if nxt else 0
    q = xf = plan = mb = 0.0
    if nxt:
        c, _, _, obj = nxt
        f = flows.get((f"{t}.{obj}", c))   # the agent records the object key
        size = next((n for oo, n in dag[t]["outputs"] if oo == obj), 0)
        mb = size / 1e6
        if f and pl[t] != pl[c]:
            done = x["computeEndUnix"] + hand
            q, xf = f["startUnix"] - done, f["endUnix"] - f["startUnix"]
            plan = size / (net.pair_mbit(pl[t], pl[c]) * 1e6 / 8 * gp)
    tot["queue"] += q; tot["transfer"] += xf; tot["transfer_plan"] += plan
    disp = (x["taskStartUnix"] - at) if p else x["taskStartUnix"] - t0
    label = f"{t}@{pl[t]}" + (f" -> {nxt[0]}@{pl[nxt[0]]}" if nxt else "")
    print(f"{label:22} {comp:7.2f} {hand if nxt else 0:7.2f} {q:6.2f} {xf:6.2f} {plan:6.2f} {mb:5.0f} {disp:8.2f}")
print("totals:", {k: round(v, 2) for k, v in tot.items()})
