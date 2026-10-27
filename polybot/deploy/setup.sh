#!/usr/bin/env bash
# One-time server setup (Amazon Linux 2023), re-runnable after each code sync. From the Mac, repo root:
#
#   rsync -az --exclude .venv --exclude runs --exclude __pycache__ polybot ec2-user@<server>:poly/
#   ssh ec2-user@<server> bash poly/polybot/deploy/setup.sh
#
# New server from GitHub instead (must land in ~/poly: the services expect /home/ec2-user/poly/polybot):
#
#   sudo dnf -y install git && git clone -b live-gap-fix https://github.com/atul2501/Polymarket.git ~/poly
#   bash ~/poly/polybot/deploy/setup.sh            later updates: git -C ~/poly pull && bash ~/poly/polybot/deploy/setup.sh
#
# Starts the paper bots, price recorder, latency monitor and dashboard. No live bot: Live-D (D-fast scalp) was removed
# 26 Oct after -$8.62 on 246 real trades. A future live unit must never run on two servers with the same wallet.
set -eu
cd "$(dirname "$0")/.."
sudo dnf -y -q install python3.11 python3.11-pip sqlite rsync
[ -d ~/venv ] || python3.11 -m venv ~/venv
~/venv/bin/pip -q install -U pip
~/venv/bin/pip -q install -r requirements.txt
mkdir -p runs
chronyc tracking | grep -E "System time|Leap"   # Polymarket rejects orders with a skewed clock
sudo cp deploy/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now polybot-wclonefast polybot-wsnipe polybot-wsnipeslow polybot-feedrace polybot-latmon polybot-dash
sudo systemctl restart polybot-wclonefast polybot-wsnipe polybot-wsnipeslow polybot-feedrace polybot-latmon polybot-dash   # pick up synced code
[ -f ~/.polybot-secrets ] && chmod 600 ~/.polybot-secrets   # wallet key, kept for a future live test
systemctl --no-pager -n0 status 'polybot-*' | grep -E "●|Active"
