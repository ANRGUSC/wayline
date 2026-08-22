#!/usr/bin/env bash
# Shared config for the Chameleon E9 bring-up. Source from the other
# scripts. SERVER_IP is the PRIVATE ip of the k3s server node (anrg-2).
SERVER_IP=${SERVER_IP:?set SERVER_IP to the server node private IP}
REG="$SERVER_IP:5000"
K3S_VERSION=${K3S_VERSION:-v1.30.4+k3s1}
NODES=${NODES:-"anrg-1 anrg-3 anrg-4 anrg-5 anrg-6 anrg-7 anrg-8 anrg-9"}
