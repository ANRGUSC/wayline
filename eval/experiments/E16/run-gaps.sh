#!/usr/bin/env bash
# Fill the headline matrix's gaps, in order. Run on anrg-2 with SUDO_PASS set.
export KUBECONFIG=$HOME/.kube/config
cd ~/wl-e16/eval/experiments/E16
python3 -u e16.py --tag fill --only fill-iot-cost --reps 3
python3 -u e16.py --tag fill --only fill-wl
bash ./build-gaps.sh
python3 -u e16.py --tag fill --ray --gaps
python3 -u e16.py --tag fill --ray --gaps --focal
python3 -u e16.py --tag fill --argo --gaps
echo GAPS-ALL-DONE
