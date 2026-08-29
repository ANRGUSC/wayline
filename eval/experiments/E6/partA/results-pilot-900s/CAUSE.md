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
