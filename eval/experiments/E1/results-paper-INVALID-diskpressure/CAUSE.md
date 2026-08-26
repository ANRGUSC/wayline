# INVALID DATASET — do not use

Campaign 2026-08-25/26, 240 runs. At ~35 min in, accumulated run data
(per-run ODAG deletion does not purge agent data; ~1 GB/run) drove 7/8
workers into DiskPressure taints. Consequences: the shaper pod was
evicted (caps silently stopped applying: tc_rate=MISSING, then empty
degrade fields), the realization reconciler skipped tainted nodes
("no agent for copy target"), serve aliases hit 409 loops on full
disks (18/20 adaptive-early runs wedged), and digest sidecars were
unreadable. Retained per the final-data rules; the fixed campaign's
data replaces it wholesale.
