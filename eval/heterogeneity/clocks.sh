#!/usr/bin/env bash
# Per-node CPU-clock pinning for the heterogeneity benchmark.
#
#   clocks.sh apply    pin each node to its assigned fixed clock
#   clocks.sh verify   read back the actual current clock on every node
#   clocks.sh reset    restore performance governor / unpinned turbo
#
# Two drivers in the fleet:
#   Intel (anrg-*): intel_pstate. Pin by writing scaling_min_freq==scaling_max_freq
#                   on every core and disabling turbo (no_turbo=1).
#   Pi    (rpi*)  : cpufreq-dt. Pin via performance governor + scaling_max/min_freq
#                   to a discrete supported step (600..1200 MHz).
#
# Frequencies are in kHz. The assignment below is a deliberate heterogeneity
# gradient so HEFT must learn per-node speed and prefer faster nodes.
set -u

SSH="sshpass -p anrg   ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"
SSHP="sshpass -p anrgrpi ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"

# Intel nodes: name -> target kHz. The i3-N305 base clock is ~1800 MHz and
# turbo (up to 3.8 GHz) is left DISABLED (no_turbo=1) for determinism — turbo
# frequencies vary with thermal/power/active-core count and can't be pinned.
# So the controlled, reproducible band is 800000..1800000 kHz.
declare -A INTEL_KHZ=(
  [anrg-1]=1800000  [anrg-3]=900000   [anrg-4]=1600000  [anrg-5]=1100000
  [anrg-6]=1400000  [anrg-7]=800000   [anrg-8]=1700000  [anrg-9]=1200000
)
# Pi nodes: name -> target kHz (cpufreq-dt steps 600000..1200000, 100 MHz grid).
declare -A PI_KHZ=(
  [rpi11]=1200000  [rpi14]=600000   [rpi15]=1000000  [rpi17]=800000
  [rpi26]=1200000  [rpi27]=700000   [rpi28]=1100000  [rpi39]=900000
  [rpi44]=600000   [rpi47]=1000000  [rpi50]=800000   [rpi52]=1200000
)
# Pi name -> IP (192.168.1.(100+N)).
pi_ip() { echo "192.168.1.$((100 + ${1#rpi}))"; }

apply_intel() {
  local n=$1 khz=$2
  $SSH anrg@"$n" "echo anrg | sudo -S -p '' sh -c '
    echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null
    for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
      echo $khz > \$c/scaling_max_freq 2>/dev/null
      echo $khz > \$c/scaling_min_freq 2>/dev/null
    done'" >/dev/null 2>&1
}
reset_intel() {
  local n=$1
  $SSH anrg@"$n" "echo anrg | sudo -S -p '' sh -c '
    echo 0 > /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null
    for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
      cat \$c/cpuinfo_min_freq > \$c/scaling_min_freq 2>/dev/null
      cat \$c/cpuinfo_max_freq > \$c/scaling_max_freq 2>/dev/null
      echo performance > \$c/scaling_governor 2>/dev/null
    done'" >/dev/null 2>&1
}
apply_pi() {
  local n=$1 khz=$2 ip; ip=$(pi_ip "$n")
  $SSHP pi@"$ip" "echo anrgrpi | sudo -S -p '' sh -c '
    for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
      echo performance > \$c/scaling_governor 2>/dev/null
      echo $khz > \$c/scaling_max_freq 2>/dev/null
      echo $khz > \$c/scaling_min_freq 2>/dev/null
    done'" >/dev/null 2>&1
}
reset_pi() {
  local n=$1 ip; ip=$(pi_ip "$n")
  $SSHP pi@"$ip" "echo anrgrpi | sudo -S -p '' sh -c '
    for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
      echo performance > \$c/scaling_governor 2>/dev/null
      cat \$c/cpuinfo_min_freq > \$c/scaling_min_freq 2>/dev/null
      cat \$c/cpuinfo_max_freq > \$c/scaling_max_freq 2>/dev/null
    done'" >/dev/null 2>&1
}
# Read "setpoint cur" = scaling_max_freq (the pinned cap) and scaling_cur_freq
# (instantaneous; jittery at idle). We report the setpoint as authoritative.
read_khz_intel() { $SSH  anrg@"$1"          "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq" 2>/dev/null; }
read_khz_pi()    { $SSHP pi@"$(pi_ip "$1")" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq" 2>/dev/null; }

case "${1:-}" in
  apply)
    echo "[clocks] pinning Intel nodes..."
    for n in "${!INTEL_KHZ[@]}"; do apply_intel "$n" "${INTEL_KHZ[$n]}" & done; wait
    echo "[clocks] pinning Pi nodes..."
    for n in "${!PI_KHZ[@]}"; do apply_pi "$n" "${PI_KHZ[$n]}" & done; wait
    sleep 2
    "$0" verify
    ;;
  verify)
    echo "=== node    target(MHz)  actual(MHz) ==="
    for n in $(echo "${!INTEL_KHZ[@]}" | tr ' ' '\n' | sort); do
      a=$(read_khz_intel "$n"); printf "%-8s %8s     %8s\n" "$n" "$((${INTEL_KHZ[$n]}/1000))" "$([ -n "$a" ] && echo $((a/1000)) || echo '??')"
    done
    for n in $(echo "${!PI_KHZ[@]}" | tr ' ' '\n' | sort -V); do
      a=$(read_khz_pi "$n"); printf "%-8s %8s     %8s\n" "$n" "$((${PI_KHZ[$n]}/1000))" "$([ -n "$a" ] && echo $((a/1000)) || echo '??')"
    done
    ;;
  reset)
    echo "[clocks] resetting Intel nodes..."
    for n in "${!INTEL_KHZ[@]}"; do reset_intel "$n" & done; wait
    echo "[clocks] resetting Pi nodes..."
    for n in "${!PI_KHZ[@]}"; do reset_pi "$n" & done; wait
    "$0" verify
    ;;
  *)
    echo "usage: $0 {apply|verify|reset}"; exit 1 ;;
esac
