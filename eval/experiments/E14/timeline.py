import json, glob, os, sys
n = int(sys.argv[1]) if len(sys.argv) > 1 else 6
for f in sorted(glob.glob(os.path.expanduser("~/focal-results/*.json")), key=os.path.getmtime)[-n:]:
    r = json.load(open(f)); t0 = r["submitted"]; T = r["timings"]; pl = r["placement"]
    print(os.path.basename(f), r["phase"], "makespan %.1f" % r["makespan"])
    for t in ["source-seismic", "source-audio", "fft-seismic", "fft-audio", "encoder-seismic", "encoder-audio", "fuse-head"]:
        x = T[t]
        print("   %-16s %-7s start +%6.2f  close +%6.2f" % (t, pl[t], x["taskStartUnix"] - t0, x["closeUnix"] - t0))
