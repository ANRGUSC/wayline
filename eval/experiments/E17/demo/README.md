# Repair Lab

An interactive demo of schedule repair on the real testbed: run a DAG
undisrupted, draw a disruption (a cut radio link, a jammer) and when it
happens, then run the same DAG again under one repair strategy (wait,
reroute, local repair, global repair: the MILCOM 2026 paper's) and compare.

Each run is an E17 run (`../driver.py`): the DAG executes through Wayline on
Tier 2 lane 1 (UP board anrg-7, Pi 4s rpi4-1 and rpi4-2, Pi 3s rpi51 and
rpi52) with real compute, and the links between them emulated
(`../emulator.py`): a radio link is 2 Mbit/s with 10 ms ±2 ms delay and 0.2%
loss; a wired link is the real switch, unshaped. Repair decisions come from the
paper's own `RepairingSagaScheduler`, running outside Wayline and enacted
through `spec.placement`.

## Run it

On eclipse (the sudo password is stdin's first line, as for E17):

    echo <password> | ~/testbed/e17/demo/run-demo.sh      # log: ~/testbed/e17/demo.log
    pkill -f "[r]epair-lab-server"                         # stop; tears the lane's emulation down

Open https://eclipse.tail5097fe.ts.net/repair/ from the tailnet
(`tailscale serve` maps `/repair` to 127.0.0.1:8095), or tunnel the port.

While it runs, the five lane nodes keep their emulated links to each other
(data VLAN): stop it before using them for anything else.

## What it shows

- Task graph: the DAG, each task colored by the hardware of the node it ran
  on, moved tasks outlined; edges by what the transfer crossed (same node,
  wire, radio, relayed) and how much it carried.
- A timeline per run: tasks per node, incoming transfers, disruption windows,
  repairs (what moved where), relayed stretches and forwarding.
- The map: the lane's links; with the Wire link tool, which pairs are wired.
- One time cursor links them: hovering a chart shows that moment on the map
  (links down, transfers in flight along the path they took, measured rates)
  and in the graph (task states).
- Per run, the traffic measured on every link against the baseline.

## How it works

- `server.py`: the catalog (the 30 Tier 2 DAG x CCR instances), a queue of
  runs executed one at a time on the lane, the live view of the run in
  progress, and the translation of drawn disruptions into the driver's trace.
  Runs are kept in `~/testbed/e17/results-demo/runs/` and reloaded on restart.
- Links: which pairs are wired is the lab's topology (default: all radio,
  kept in `runs/topology.json`). Each topology gets its own network model (a
  wired pair at 100 x the radio bandwidth, as in Tier 2), its own HEFT plan and
  undisrupted makespan (as `../scenarios.py` exports them), and its own
  baselines; the lane's emulation is rebuilt for it.
- Measurements (`LinkMeter`): an nftables table on every lane node, read twice
  a second, counts what the node sends to each next hop (`rt ip nexthop`: its
  own and relayed traffic) and what it relays for others, each split into
  data and acknowledgments: a TCP packet without payload is at most 80 bytes
  (20-byte IP header, at most 60 bytes of TCP header), so longer packets carry
  data. The emulator's route changes are recorded with each link change, and
  each transfer is annotated with the stretches it was relayed and its path.
- `static/`: the page (no build step).

Disruption semantics, as in the paper: a radio pair goes down (both
directions) while a cut on it is active, or while a jammer has one of the
pair's endpoints inside its circle (a jammer silences the radios of the nodes
it covers; a link merely crossing it is untouched). Overlapping windows merge.
Wired pairs are never disrupted. Positions are the lab's fixed map (`LAYOUT`),
not the paper's per-instance random geometry. Windows are drawn in seconds from
the baseline's first task start and replayed at the same times. There is no
inter-link interference, as in the paper's ncsim runs.
