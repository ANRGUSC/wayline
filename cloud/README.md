# E9: the datacenter-fabric point (Chameleon)

Same E1 campaign — direct + store × {iot, hetero, wpf}, N=20 — on
bare-metal nodes with their stock fabric, no shaping. Completes the
three-point curve: datacenter (expect ≈1.0×) / flat 1 GbE (1.00–1.29×
measured) / shaped edge matrix (1.33–2.73× measured).

Budget: 8 standard compute nodes × 24–48 h ≈ 192–384 SUs.

## User side (web UI, your credentials)

1. CHI@TACC or CHI@UC → Blazar lease: **8 standard compute nodes**
   (Cascade Lake/Skylake class; NOT GPU nodes), 24–48 h.
2. Launch 8 instances from **CC-Ubuntu22.04** on the lease. Names
   don't matter (the scripts set hostnames); note which one gets the
   **floating IP** — that one becomes the k3s server (`anrg-2`).
3. Make sure one keypair reaches all instances (Chameleon injects
   your key for user `cc`; workers only need to be reachable from the
   server over the private network).
4. Hand over: floating IP + private IPs of the other 7 nodes.

## Bring-up (scripted, over ssh)

On the server node (the one with the floating IP):

```sh
git clone <wayline repo> && cd wayline
SERVER_IP=<server private ip> ./cloud/00-server.sh   # prints TOKEN
```

On each worker (from the server: `ssh cc@<worker-private-ip>`), with
the repo's `cloud/` dir copied over (scp is enough):

```sh
SERVER_IP=<server private ip> TOKEN=<token> ./01-agent.sh anrg-1
# ... anrg-3 anrg-4 anrg-5 anrg-6 anrg-7 anrg-8 anrg-9
```

Back on the server:

```sh
./cloud/02-build.sh        # images -> local registry (~15 min)
./cloud/03-deploy.sh       # CRDs, agents, controller, templates
N=20 ./cloud/04-campaign.sh  # iperf3 + 6 arms (~2.5 h)
```

`04-campaign.sh` measures the fabric with iperf3 first and rewrites
`wl-network-profile.defaultBandwidth` with the measured bytes/s, so
the later fidelity replay uses ground truth, not the nominal 10 G.

## After

- `scp -r cc@<floating-ip>:~/e9-results eval/policies/results-e9`
- **Delete the lease.** Everything else (fidelity replay with the
  measured capacity, fig-tax third group, fidelity-table row) is
  offline.

Notes:
- Node names replicate the testbed (`anrg-1..anrg-9`, server =
  `anrg-2`), so every template, constraint, and script runs unchanged;
  the only substitution is the registry address, done at render time
  by `03-deploy.sh`.
- Bare-metal Chameleon tests the fabric claim, not the
  managed-Kubernetes claim; the privileged-DaemonSet caveat for
  locked-down offerings stays in the discussion.
