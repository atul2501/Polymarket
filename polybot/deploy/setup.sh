#!/usr/bin/env bash
# One-time server setup (Amazon Linux 2023), re-runnable after each code sync. From the Mac, repo root:
#
#   rsync -az --exclude .venv --exclude runs --exclude __pycache__ polybot ec2-user@<server>:poly/
#   ssh ec2-user@<server> bash poly/polybot/deploy/setup.sh
#
# Starts the paper twin, latency monitor and dashboard. The live bot starts only once ~/.polybot-secrets exists,
# and then in LIVE-DRY mode (see polybot-live.service to go live).
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
sudo systemctl enable --now polybot-dfast polybot-dhy2 polybot-dhy3 polybot-ds03 polybot-ds03n polybot-latmon polybot-dash
sudo systemctl restart polybot-dfast polybot-dhy2 polybot-dhy3 polybot-ds03 polybot-ds03n polybot-latmon polybot-dash   # pick up synced code
if [ -f ~/.polybot-secrets ]; then
  chmod 600 ~/.polybot-secrets
  sudo systemctl enable polybot-live && sudo systemctl restart polybot-live
  echo "live: $(grep -h TK_LIVE_DRY ~/.polybot-mode 2>/dev/null || echo 'TK_LIVE_DRY=1 (dry)')"
else
  echo "no ~/.polybot-secrets yet: cp secrets.env.example ~/.polybot-secrets, fill it in, re-run this script"
fi
systemctl --no-pager -n0 status 'polybot-*' | grep -E "●|Active"
