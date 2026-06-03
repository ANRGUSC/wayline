#!/usr/bin/env bash
# Removes the tc-setup pods; host interfaces revert to no shaping the
# next time their tc qdiscs are reset (or on node reboot). For an
# active cleanup of tc rules, re-run setup-tc-matrix.sh which deletes
# the previous qdisc before applying new rules.
set -euo pipefail
kubectl delete pods -n wl-system -l app=tc-setup --ignore-not-found
echo "tc-setup pods removed. Host tc rules persist until re-applied or node reboot."
