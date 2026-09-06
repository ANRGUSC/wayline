# Jetson TX2 reflash to JetPack 4.6.x (Ubuntu 18.04) — runbook

The node `tegra-ubuntu` is a **Jetson TX2** (module "quill", Tegra186,
P3310) currently on **L4T R28.2.1 / Ubuntu 16.04 / kernel 4.4.38**. It is
connected by a direct GbE cable to anrg-2's second port (`enp1s0`) and is
reachable only through anrg-2 (see "Current network setup" below).

We decided **not** to attempt an in-place `do-release-upgrade` (assessed
2026-09-04). Reasons:

- The device is physically inaccessible to the remote session; a broken
  network/boot after an in-place upgrade would be unrecoverable without a
  physical reflash anyway.
- It carries 90 NVIDIA CUDA/L4T/VisionWorks packages from the BSP and
  local `file://` repos. An in-place 16.04->18.04 disables third-party
  sources and has no bionic equivalents for them, so the GPU stack — the
  entire reason to run a Jetson — would almost certainly break.
- NVIDIA provides **no** supported in-place apt path for Jetson. The
  supported route from R28 to an 18.04 userspace is a reflash to L4T R32
  via SDK Manager over USB recovery.

The reflash below needs **physical access** to the device and a separate
x86-64 Linux host. It cannot be done from this session.

## Target

- **JetPack 4.6.5** (latest for TX2) = **L4T R32.7.5**, **Ubuntu 18.04**,
  CUDA 10.2, cuDNN 8, TensorRT 8.2. This is the final JetPack line that
  supports TX2; JetPack 5/6 are Xavier/Orin only, so 18.04 is the ceiling.

## What you need on hand

1. The TX2 devkit (module + carrier board) and its power supply.
2. The **bundled USB micro-B -> USB-A cable** (flashing uses the micro-USB
   port on the module side of the carrier, next to the power jack).
3. An **x86-64 host running Ubuntu 18.04 or 20.04** (SDK Manager does not
   run on arm64 and is picky about host release). A VM works only with
   reliable USB passthrough; bare metal is far less fragile for flashing.
4. ~40 GB free on that host for the downloaded BSP + sample rootfs.
5. An NVIDIA Developer account (SDK Manager requires login).

## Procedure

### 1. Install SDK Manager on the x86 host
- Download `sdkmanager` .deb from developer.nvidia.com/sdk-manager,
  `sudo apt install ./sdkmanager_*.deb`, launch `sdkmanager`, log in.

### 2. Put the TX2 into Force Recovery (USB) mode
With the board powered **off**:
1. Connect the micro-USB cable from the TX2 to the x86 host.
2. Hold the **REC** (Force Recovery) button.
3. Tap the **RST** (reset) button while still holding REC (if the board
   was off, instead press **POWER** while holding REC).
4. Release REC after ~2 s.
5. On the host, `lsusb | grep -i nvidia` should show `Nvidia Corp. APX` —
   that confirms recovery mode. If it doesn't appear, repeat.

### 3. Flash with SDK Manager
1. SDK Manager Step 1: select **Jetson TX2**, target **JetPack 4.6.5**.
2. Step 2: accept licenses; it downloads the BSP and sample rootfs.
3. Step 3: it flashes the OS image, then prompts for **OEM config**.
   - Choose **"Pre-config"** and set username/password here if you want
     it unattended, OR leave "Runtime" and complete the first-boot setup
     on a monitor+keyboard once. Pre-config avoids needing a display.
   - Storage: **eMMC** (this devkit has 32 GB eMMC; the used 15 GB is
     wiped — nothing on the device is preserved, so no pre-cleanup is
     needed or useful).
4. After the OS flashes and the board reboots, SDK Manager installs the
   CUDA/cuDNN/TensorRT SDK components **over the network/USB** to the
   running device. For that step it needs the device's IP; the simplest
   path is to have a monitor attached for first boot, or use the USB
   device-mode Ethernet (192.168.55.1) SDK Manager sets up.

### 4. First-boot sanity (on the device)
```
head -1 /etc/nv_tegra_release      # expect R32 (release), REVISION: 7.5
lsb_release -a                     # Ubuntu 18.04
nvcc --version                     # CUDA 10.2 if SDK components installed
```

## Post-flash: rejoin the testbed network + Tailscale

A reflash resets username/password and wipes the network config, so the
anrg-2-side plumbing has to be re-pointed and Tailscale reinstalled. All
of this I *can* do remotely once the device is back on the cable and
SSH-reachable — hand it back to me at that point.

### Network (re-established from anrg-2)
The direct-cable setup I built for the upgrade attempt (still live on
anrg-2, see below) will hand the reflashed device a DHCP lease and NAT it
to the internet with no changes needed — it keys on the subnet, not the
old MAC. Confirm the new lease in `/tmp/jetson.leases` on anrg-2.

### Tailscale (the original goal)
On the reflashed 18.04 device:
```
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```
`tailscale up` prints a login URL to authorize the node into the tailnet,
or use a pre-auth key from the admin console for unattended join. On
18.04 the standard Tailscale apt repo and the install script both work
(kernel 4.9 with /dev/net/tun present). Once it is on the tailnet it no
longer needs the anrg-2 NAT path and can be reached directly by its
Tailscale IP.

## Current network setup on anrg-2 (built 2026-09-04, still live)

So it can be reproduced or torn down:

- `enp1s0` (second NIC, direct cable to TX2) holds `192.168.77.1/24`.
- `dnsmasq` serves DHCP + DNS on that interface only, scoped so the
  cluster network is untouched:
  `--interface=enp1s0 --bind-interfaces --dhcp-range=192.168.77.50,99,12h
   --server=8.8.8.8 --server=1.1.1.1`
  (pid `/tmp/dnsmasq-jetson.pid`, leases `/tmp/jetson.leases`).
- NAT: `iptables -t nat -A POSTROUTING -s 192.168.77.0/24 -o enp2s0 -j
  MASQUERADE` plus two FORWARD accepts. `net.ipv4.ip_forward=1`.
- **None of this survives an anrg-2 reboot** (no persistence configured).
  If anrg-2 is rebooted, re-run the setup or make it persistent before
  expecting the TX2 to have connectivity.

## What is NOT worth doing first

- No disk cleanup on the current 16.04 — the reflash wipes eMMC.
- No in-place package upgrades — same reason.
- The current install is fully patched within 16.04 already, if for some
  reason it must be kept alive short-term.
