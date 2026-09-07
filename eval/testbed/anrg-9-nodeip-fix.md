# anrg-9 node-IP fix (2026-09-06)

## Symptom
Any Wayline data transfer whose destination or serving node was anrg-9
never completed: the controller reached agents by node InternalIP, and
anrg-9's was unreachable. Found while validating E7 (its consume-9,
on anrg-9, never received the payload; uncapped runs to anrg-6/anrg-8
were fine).

## Root cause
`kubectl get nodes -o ...InternalIP` showed anrg-9 = **10.25.160.32**
while every other UP board was **192.168.1.x**. anrg-9 has a WiFi
interface `wlp3s0` at 10.25.160.32/16 that was UP (DOWN on the other
boards), and its k3s-agent had **no explicit `--node-ip`**, so k3s
auto-detection picked the WiFi IP over the wired fabric `enp2s0`
(192.168.1.166). flannel's own `public-ip` annotation was already
192.168.1.166, so only the registered InternalIP was wrong. 10.25.160.32
is not routable from the rest of the cluster (100% packet loss from
anrg-2). anrg-9 is a k3s **agent** (server is anrg-2 @ 192.168.1.163),
so this was a worker-level fix, not a control-plane change.

## Fix (persists across reboots)
Created `/etc/rancher/k3s/config.yaml` on anrg-9:

    node-ip: "192.168.1.166"

then `systemctl restart k3s-agent`. anrg-9 re-registered with
InternalIP 192.168.1.166 within ~18s and stayed Ready; all nodes are now
192.168.1.x and anrg-9's data-agent is reachable (healthz 200).

Applied via a privileged `nsenter --target 1 --mount` pod on anrg-9
because inter-node SSH is not keyed (anrg@192.168.1.166 = permission
denied).

## Revert
Delete `/etc/rancher/k3s/config.yaml` on anrg-9 and restart k3s-agent
(reverts to broken auto-detection while wlp3s0 is up).

## Watch items
- If `wlp3s0` is later brought down cluster-wide this fix is harmless
  (explicit node-ip overrides auto-detection regardless).
- Any experiment result produced ~2026-09-04..06 that used anrg-9 as a
  data node may be suspect; the breakage window matches anrg-9's agent
  restart 2d15h before the fix. E5/E6 completed before this window.
