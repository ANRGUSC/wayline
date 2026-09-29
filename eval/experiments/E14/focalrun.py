"""Run the FOCAL template once, report per-stage timing and placement, and
check the pipeline's logits against the unsplit model in the same image."""
import io, json, os, subprocess, sys, time, urllib.request
import numpy as np
sys.path.insert(0, os.path.expanduser("~/wayline-build-vertex/examples/focal-mod"))
import gen_focal as G
K = "kubectl -n wl-system "
WL = os.path.expanduser("~/wayline/bin/wayline")
sh = lambda c, i=None: subprocess.run(c, shell=True, capture_output=True, text=True, input=i)
mode, batch, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
name = f"focal-{mode}"
r = sh(K + "apply -f -", G.template(name, mode, batch, seed)); assert r.returncode == 0, r.stderr
t0 = time.time()
run = sh(f"{WL} run {name} -n wl-system").stdout.split("Created run ")[1].split()[0]
while time.time() - t0 < 600:
    ph = sh(K + f"get odag {run} -o jsonpath='{{.status.phase}}'").stdout
    if ph in ("Succeeded", "Failed"):
        break
    time.sleep(0.25)
wall = time.time() - t0
st = json.loads(sh(K + f"get odag {run} -o json").stdout)["status"]
pl = {t["name"]: t["node"] for t in st["tasks"]}
ips = json.loads(sh(K + "get pods -l app=data-agent -o json").stdout)
ips = {p["spec"]["nodeName"]: p["status"]["podIP"] for p in ips["items"]}
get = lambda u: urllib.request.urlopen(u, timeout=20).read()
T = {t: json.loads(get(f"http://{ips[n]}:8082/timings/{run}/{t}")) for t, n in pl.items()}
first = min(x["taskStartUnix"] for x in T.values()); last = max(x["closeUnix"] for x in T.values())
print(f"{name}: {ph}  makespan {last - t0:.2f}s (first task start +{first - t0:.2f}s)  wall {wall:.2f}s")
for t in ["source-seismic", "source-audio", "fft-seismic", "fft-audio", "encoder-seismic", "encoder-audio", "fuse-head"]:
    x = T[t]
    print(f"  {t:16} {pl[t]:7} start +{x['taskStartUnix'] - t0:6.2f}  compute {x['computeSeconds']:5.2f}s  close +{x['closeUnix'] - t0:6.2f}")
logits = np.load(io.BytesIO(get(f"http://{ips[pl['fuse-head']]}:8082/{run}/fuse-head.logits/output")))
r2 = subprocess.run(["docker", "run", "--rm", "192.168.1.163:5000/wl-focal:latest", "python", "-c",
     f"import focal_stages as F, numpy as np, io, sys; fr={{m: F.fft(F.source(m, {batch}, {seed} + F.MODS.index(m))) for m in F.MODS}}; "
     f"b=io.BytesIO(); np.save(b, F.whole(fr)); sys.stdout.buffer.write(b.getvalue())"], capture_output=True)
refa = np.load(io.BytesIO(r2.stdout))
print(f"logits {logits.shape} match the unsplit model: {np.allclose(logits, refa, atol=1e-5)} (max diff {float(np.abs(logits - refa).max()):.2e})")
json.dump({"mode": mode, "run": run, "phase": ph, "makespan": last - t0, "wall": wall, "placement": pl, "timings": T, "submitted": t0},
          open(os.path.expanduser(f"~/focal-results/{name}-{int(t0)}.json"), "w"), indent=1)
sh(f"{WL} delete {run} -n wl-system")
