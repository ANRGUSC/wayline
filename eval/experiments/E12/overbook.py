"""Force pods and warm calls onto one node at once and measure the peak CPU
requested by tasks computing there at the same instant (capacity 7.4)."""
import json, os, subprocess, sys, time, urllib.request
sys.path.insert(0, os.path.expanduser("~/wayline-build-vertex/eval/experiments/E11"))
import gen_e11 as g
K = "kubectl -n wl-system "
sh = lambda c, i=None: subprocess.run(c, shell=True, capture_output=True, text=True, input=i)
node = "anrg-1"
d = {"order": ["s", "w1", "w2", "w3", "w4"], "tasks": {
    "s": {"work": 0.5, "cpu": 1, "allowed": [node], "inputs": [], "outputs": [[f"to-w{i}", 1000000] for i in range(1, 5)]},
    **{f"w{i}": {"work": 8.0, "cpu": 4, "allowed": [node], "inputs": [["s", f"to-w{i}"]], "outputs": []} for i in range(1, 5)}}}
rates = {node: 2.5}
y = g.template("overbook", "heft", d, rates, enact="none", runner="e12",
               image="192.168.1.163:5000/wl-e11:v2", warm_tasks={"w2", "w4"})
r = sh(K + "apply -f -", y); assert r.returncode == 0, r.stderr
run = sh(os.path.expanduser("~/wayline/bin/wayline") + " run overbook -n wl-system").stdout.split("Created run ")[1].split()[0]
for _ in range(240):
    if sh(K + f"get odag {run} -o jsonpath='{{.status.phase}}'").stdout in ("Succeeded", "Failed"):
        break
    time.sleep(1)
ip = json.loads(sh(K + "get pods -l app=data-agent -o json").stdout)
ip = next(p["status"]["podIP"] for p in ip["items"] if p["spec"]["nodeName"] == node)
T = {t: json.loads(urllib.request.urlopen(f"http://{ip}:8082/timings/{run}/{t}").read()) for t in d["order"]}
iv = {t: (x["computeStartUnix"], x["computeEndUnix"]) for t, x in T.items()}
peak = max(sum(d["tasks"][u]["cpu"] for u, (a, b) in iv.items() if a <= s < b) for s, _ in iv.values())
t0 = min(a for a, _ in iv.values())
for t in d["order"]:
    kind = "warm" if t in ("w2", "w4") else "pod"
    print(f"  {t:3} {kind:4} cpu={d['tasks'][t]['cpu']} compute +{iv[t][0]-t0:5.1f} .. +{iv[t][1]-t0:5.1f}")
print(f"peak CPU requested at once on {node}: {peak} (capacity 7.4)")
sh(os.path.expanduser("~/wayline/bin/wayline") + f" delete {run} -n wl-system")
