# CPU-decode sensitivity result, NOT a paper measurement

All nine runs are valid and all nine campaign criteria pass. The data is
sound as **mechanism validation**: named objects, frozen placement and
order replay, exact store routing, and report equivalence all hold.

It is NOT suitable as a paper measurement of the workload, because
hardware decode was not working:

    [decode_clip] VAAPI failed (code=234), retrying with software decode

VAAPI is part of the specified MCMT workload. Falling back to CPU decode
materially changes the compute-to-communication ratio, which is exactly
the quantity the direct-vs-store comparison depends on. With decode on
the CPU the critical path is compute-bound, so 744 MB of extra gateway
transfer hides in slack and the realization effect measures 1.018x.

Retained, labeled, and not to be mixed with the corrected pilot. If
cited at all, cite it as a CPU-decode sensitivity point: it shows that
when per-frame compute dominates, store-mediated realization is close to
free on this workload.

Superseded by the VAAPI-corrected rerun (same clips, matrix, frozen
placement and order, and images).
