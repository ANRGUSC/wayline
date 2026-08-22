#!/usr/bin/env bash
# Run ON each Chameleon worker node: ./01-agent.sh anrg-3
# Env: SERVER_IP (server private IP), TOKEN (k3s node-token).
set -euo pipefail
cd "$(dirname "$0")"
NAME=${1:?usage: SERVER_IP=... TOKEN=... ./01-agent.sh <anrg-N>}
TOKEN=${TOKEN:?set TOKEN from the server's node-token}
. ./env.sh

sudo hostnamectl set-hostname "$NAME"
sudo mkdir -p /etc/rancher/k3s
cat << YAML | sudo tee /etc/rancher/k3s/registries.yaml >/dev/null
mirrors:
  "$REG":
    endpoint: ["http://$REG"]
YAML
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$K3S_VERSION" \
  K3S_URL="https://$SERVER_IP:6443" K3S_TOKEN="$TOKEN" \
  sh -s - agent --node-name "$NAME"
echo "$NAME joined."
