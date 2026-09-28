"""Decompose a Wayline run's makespan along its realized critical path.

Walks back from the last task to close, each time to the parent whose
output was sealed last, and splits the time into: launch (last parent
sealed -> task start), input read, compute, and output handoff+close.
Compute is compared with the planned runtime (iterations / calibrated
rate) and the launch with the planned transfer (edge bytes / bandwidth).
"""
import json, sys
d = sys.argv[1]; arm = sys.argv[2]
dag = json.load(open(f"{d}/dag.json"))["tasks"]; rates = json.load(open(f"{d}/rates.json"))
ref = max(rates.values()); BW = 942e6 / 8
R = [r for r in json.load(open(f"{d}/results.json")) if r["arm"] == arm][0]
T, pl, t0 = R["timings"], R["placement"], R["submitted"]
last = max(T, key=lambda t: T[t]["closeUnix"])
chain = [last]
while dag[chain[-1]]["inputs"]:
    chain.append(max({p for p, _ in dag[chain[-1]]["inputs"]}, key=lambda p: T[p]["closeUnix"]))
chain.reverse()
tot = {"launch": 0, "xfer_plan": 0, "input": 0, "compute": 0, "compute_plan": 0, "handoff": 0}
print(f"{arm}: makespan {T[last]['closeUnix'] - t0:.2f}s, estimate {R['predicted_makespan']}s")
print(f"{'task':5} {'node':7} {'launch':>6} {'(plan xfer)':>11} {'input':>6} {'compute':>7} {'(plan)':>7} {'handoff':>7}")
prev = None
for t in chain:
    x = T[t]
    ready = t0 if prev is None else T[prev]["closeUnix"]
    launch = x["taskStartUnix"] - ready
    xfer = 0.0
    if prev is not None and pl[prev] != pl[t]:
        xfer = sum(n for o, n in dag[prev]["outputs"] if o == f"to-{t}") / BW
    plan = int(dag[t]["work"] * ref * 1e6) / (rates[pl[t]] * 1e6)
    comp = x["computeEndUnix"] - x["computeStartUnix"]
    inp = x["computeStartUnix"] - x["taskStartUnix"]
    hand = x["closeUnix"] - x["computeEndUnix"]
    for k, v in (("launch", launch), ("xfer_plan", xfer), ("input", inp), ("compute", comp), ("compute_plan", plan), ("handoff", hand)):
        tot[k] += v
    print(f"{t:5} {pl[t]:7} {launch:6.2f} {xfer:11.2f} {inp:6.2f} {comp:7.2f} {plan:7.2f} {hand:7.2f}")
    prev = t
print("totals:", {k: round(v, 2) for k, v in tot.items()})
