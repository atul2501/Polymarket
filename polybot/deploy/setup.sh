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
# `bash setup.sh live` = the live server (Stockholm): only W-SNIPE-DOG2 (switch it with deploy/mode.sh), the price
# recorder (the live learner's prices), latency monitor, dashboard and nightly learner; $1 live caps as drop-ins on that
# server only, so the paper DOG2 elsewhere is unchanged.
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
sudo cp deploy/*.service deploy/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now polybot-learn.timer   # W-LEARN nightly re-train
UNITS="polybot-wclonefast polybot-wsnipedog polybot-wsnipedog2 polybot-wlearn polybot-dogai polybot-dogai2 polybot-feedrace polybot-latmon polybot-dash"
if [ "${1:-}" = live ]; then
    UNITS="polybot-wsnipedog2 polybot-feedrace polybot-latmon polybot-dash"
    # TK_SIZE=5 = Polymarket orderMinSize ($0.50-2.45 at ask .10-.49; $1 trades are impossible above 0.20); TK_SLIP=0.01: the paper unit's 0 gets real FAK buys killed
    sudo mkdir -p /etc/systemd/system/polybot-wsnipedog2.service.d /etc/systemd/system/polybot-dash.service.d
    echo -e "[Service]\nEnvironment=TK_SIZE=5 TK_MAX_ROUND_USD=5 TK_MAX_TRADE_USD=2.5 TK_MAX_DAY_LOSS=5 TK_SLIP=0.01" |
        sudo tee /etc/systemd/system/polybot-wsnipedog2.service.d/live.conf > /dev/null
    echo -e "[Service]\nEnvironment=DASH_BOTS=DOG2-LIVE=wsnipedog2live.db,DOG2-DRY=wsnipedog2dry.db,DOG2-PAPER=wsnipedog2.db" |
        sudo tee /etc/systemd/system/polybot-dash.service.d/live.conf > /dev/null
    sudo systemctl daemon-reload
fi
sudo systemctl enable --now $UNITS
sudo systemctl restart $UNITS   # pick up synced code
[ -f ~/.polybot-secrets ] && chmod 600 ~/.polybot-secrets   # wallet key, kept for a future live test
systemctl --no-pager -n0 status 'polybot-*' | grep -E "●|Active"
