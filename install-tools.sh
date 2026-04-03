#!/usr/bin/env bash
# Example provisioning script for tachikoma VMs.
# Add tools your workflow needs, then reference this in your config:
#
#   # .tachikoma.toml
#   provision_scripts = ["./examples/install-tools.sh"]
#
# Runs once during initial VM creation (not on subsequent spawns).

set -euo pipefail

sudo apt-get update -qq

# Uncomment the tools you need:

# Java (OpenJDK 21)
sudo apt-get install -y -qq openjdk-17-jdk

# Docker
# sudo apt-get install -y -qq docker.io
# sudo usermod -aG docker "$USER"

# Node.js (via NodeSource)
# curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
# sudo apt-get install -y -qq nodejs

# Python
# sudo apt-get install -y -qq python3 python3-pip python3-venv

# Go
# sudo apt-get install -y -qq golang-go

# Build essentials
# sudo apt-get install -y -qq build-essential cmake

sudo apt-get clean
