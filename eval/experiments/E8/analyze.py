import json, glob, statistics as st
from collections import defaultdict

def med(xs): xs=[x for x in xs if x is not None]; return round(st.median(xs),2) if xs else None
def p95(xs):
    xs=sorted(x for x in xs if x is not None)
    return round(xs[min(len(xs)-1,int(0.95*len(xs)))],2) if xs else None

# ---- Part B ----
B=defaultdict(list)
for f in glob.glob("E8-campaign/B-b*.json"):
    for r in json.load(open(f)):
        B[(r["O"],r["C"])].append(r)
print("="*96); print("PART B  (named-object / desired-copy scaling; 20 blocks/config)"); print("="*96)
print(f'{"O":>4} {"C":>3} {"copies":>6} {"n":>3} | {"conv_s med":>10} {"ops/s med":>9} | {"inst_p50":>8} {"inst_p95":>8} {"inst_p99":>8} | {"patchB":>7} {"statusB":>7} | {"ctrlCPU_cs":>10} {"agtCPU_cs":>9} {"infl":>4}')
for k in [(1,1),(8,1),(32,1),(128,1),(64,2),(32,4),(16,7)]:
    rs=B[k]; O,C=k; tot=O*C
    print(f'{O:>4} {C:>3} {tot:>6} {len(rs):>3} | {med([r["patch_to_conv_s"] for r in rs]):>10} {med([r["copy_ops_per_s"] for r in rs]):>9} | '
          f'{med([r["install_med_s"] for r in rs]):>8} {med([r["install_p95_s"] for r in rs]):>8} {med([r["install_p99_s"] for r in rs]):>8} | '
          f'{med([r["patch_bytes"] for r in rs]):>7} {med([r["max_status_bytes"] for r in rs]):>7} | '
          f'{med([r["ctrl_cpu_core_s"] for r in rs]):>10} {med([r["agent_cpu_core_s"] for r in rs]):>9} {max(r["max_push_inflight"] for r in rs):>4}')

# ---- Part C ----
C=defaultdict(list)
for f in glob.glob("E8-campaign/C-b*.json"):
    for r in json.load(open(f)):
        C[r["A"]].append(r)
print("\n"+"="*96); print("PART C  (simultaneously-active transfers; 256 MiB objects, 59 Mbit/pair; 20 blocks/config)"); print("="*96)
print(f'{"A":>3} {"n":>3} | {"peakConc med":>12} {"overlapOK":>9} | {"goodput_Mbps med":>16} {"conv_s med":>10} {"xfer_p95_s":>10} | {"ctrlCPU_cs":>10} {"agtCPU_cs":>9} {"maxInfl":>7}')
for A in [1,4,8,16,32]:
    rs=C[A]
    ok=sum(1 for r in rs if r.get("overlap_ok"))
    print(f'{A:>3} {len(rs):>3} | {med([r["peak_concurrent"] for r in rs]):>12} {ok}/{len(rs):>7} | '
          f'{med([r["agg_goodput_mbps"] for r in rs]):>16} {med([r["patch_to_conv_s"] for r in rs]):>10} {med([r["xfer_p95_s"] for r in rs]):>10} | '
          f'{med([r["ctrl_cpu_core_s"] for r in rs]):>10} {med([r["agent_cpu_core_s"] for r in rs]):>9} {max(r["max_push_inflight"] for r in rs):>7}')

# ---- Part D ----
D=defaultdict(list)
for f in glob.glob("E8-campaign/D-b*.json"):
    for r in json.load(open(f)):
        D[r["R"]].append(r)
print("\n"+"="*96); print("PART D  (concurrent runs; minimal 2-task DAG; 20 blocks/config)"); print("="*96)
print(f'{"R":>3} {"n":>3} | {"drain_s med":>11} {"runs/min med":>12} {"firstpod_s":>10} {"makespan_s":>10} | {"ctrlCPU med":>11} {"ctrlCPU p95":>11} {"ctrlRSS_max":>11} | {"maxPods":>7} {"maxODAG":>7} {"maxGoro":>7}')
for R in [1,4,8,16,32,64]:
    rs=D[R]
    print(f'{R:>3} {len(rs):>3} | {med([r["drain_span_s"] for r in rs]):>11} {med([r["runs_per_min"] for r in rs]):>12} '
          f'{med([r["sub_to_firstpod_med_s"] for r in rs]):>10} {med([r["makespan_med_s"] for r in rs]):>10} | '
          f'{med([r["ctrl_cpu_mc_median"] for r in rs]):>11} {med([r["ctrl_cpu_mc_p95"] for r in rs]):>11} {med([r["ctrl_rss_max_mb"] for r in rs]):>11} | '
          f'{max(r["max_live_pods"] for r in rs):>7} {max(r["max_live_odags"] for r in rs):>7} {max(r["max_agent_goroutines"] for r in rs):>7}')

# validity totals
def nvalid(files):
    n=v=0
    for f in glob.glob(files):
        for r in json.load(open(f)): n+=1; v+=1 if r.get("valid") else 0
    return v,n
for part,g in [("B","E8-campaign/B-b*.json"),("C","E8-campaign/C-b*.json"),("D","E8-campaign/D-b*.json")]:
    v,n=nvalid(g); print(f"\nPart {part}: {v}/{n} valid")
