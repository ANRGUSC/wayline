# Retained: pilot censored by a deadline sized for a different workload

37/42 valid. All five invalid runs are store arms of the three
~100-task workflows (seismology x2, soykb x2, bwa x1) still Running at
the 900 s deadline, while sibling runs of the same arms completed at
401-754 s. The completed runs' controller timelines show steady
progress with no stall: store lowering of ~100-object DAGs is
uniformly slower because every task->vertex->task hop pays dispatch
cadence, compounded along per-node serial chains. The censored runs
are the slow tail of that variance, not wedges.

900 s was Part B's deadline, sized for a 154 s MCMT run. Deadline
raised to 1800 s and the pilot rerun IN FULL; this dataset is retained
unmixed. Two instrumentation notes: controller log slices used
--tail=8000, too small for 200-node DAGs, so some rows carry an
empty-slice artifact ("enactOrder=serial not confirmed", empty
ctrl-*.log); and the session monitor pattern missed per-run result
lines entirely (harness output was fine).

Direct arms: 21/21 valid, all placement/order/path checks clean.

## Superseding diagnosis (2026-08-29)

The 1800 s rerun failed seismology-store at the SAME 29/103 objects,
which ruled out the slow-tail explanation above. Actual root cause: the
controller was OOMKilled (256Mi limit; 12 restarts across the pilots).
A kill mid-run orphans the surviving ODAG -- the schedule-plan and
assignment caches are in-memory, so its remaining tasks never dispatch.
Censoring tracked crash timing, not workload behavior; store arms of
the biggest DAGs were hit most because they hold the most state and run
longest. The 1800 s deadline raise remains correct for bwa/soykb store
arms (753 s+ legitimate walls), but the wedges were the OOM.

Both this dataset and the aborted 1800 s rerun are invalid as
measurements; controller limit raised to 1Gi and the pilot rerun clean.
