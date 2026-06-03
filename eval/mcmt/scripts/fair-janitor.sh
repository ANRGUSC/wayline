#!/usr/bin/env bash
# Disk safety net: every 3 min, delete completed-run output dirs (>5 min old)
# on all worker nodes so retention-bug accumulation can't trigger DiskPressure.
SSH="sshpass -p anrg ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=8"
while true; do
  for n in anrg-1 anrg-3 anrg-4 anrg-5 anrg-6 anrg-7 anrg-8 anrg-9; do
    $SSH $n "echo anrg | sudo -S find /data/wl-outputs -maxdepth 1 -mindepth 1 -type d -mmin +5 -exec rm -rf {} + 2>/dev/null" >/dev/null 2>&1 &
  done
  wait
  sleep 180
done
