#!/usr/bin/env bash
# Run ON the Chameleon server node (will become anrg-2). Installs docker,
# a local registry, and the k3s server. Prints the join token at the end.
set -euo pipefail
cd "$(dirname "$0")"
SERVER_IP=${SERVER_IP:-$(hostname -I | awk '{print $1}')}
. ./env.sh

sudo hostnamectl set-hostname anrg-2

# Docker (for image builds) + local registry with persistent storage.
if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
fi
sudo mkdir -p /var/lib/wl-registry /etc/docker
echo "{\"insecure-registries\":[\"$REG\"]}" | sudo tee /etc/docker/daemon.json >/dev/null
sudo systemctl restart docker
sudo docker rm -f registry 2>/dev/null || true
sudo docker run -d --restart=always --name registry \
  -p 5000:5000 -v /var/lib/wl-registry:/var/lib/registry registry:2

# k3s needs the insecure-registry mapping BEFORE install.
sudo mkdir -p /etc/rancher/k3s
cat << YAML | sudo tee /etc/rancher/k3s/registries.yaml >/dev/null
mirrors:
  "$REG":
    endpoint: ["http://$REG"]
YAML
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$K3S_VERSION" \
  sh -s - server --node-name anrg-2 --write-kubeconfig-mode 644

echo "=========================================================="
echo "server up. Join workers with:"
echo "  SERVER_IP=$SERVER_IP TOKEN=$(sudo cat /var/lib/rancher/k3s/server/node-token)"
echo "  ./01-agent.sh <anrg-N>"
echo "=========================================================="
