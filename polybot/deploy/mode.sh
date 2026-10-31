#!/usr/bin/env bash
# Switch one bot between paper and real money, on the server:
#
#   bash ~/poly/polybot/deploy/mode.sh                    show every bot's mode
#   bash ~/poly/polybot/deploy/mode.sh wsnipedog2 dry     sign real orders, never send them (checks keys + geoblock)
#   bash ~/poly/polybot/deploy/mode.sh wsnipedog2 live    REAL orders (needs ~/.polybot-secrets)
#   bash ~/poly/polybot/deploy/mode.sh wsnipedog2 paper   back to paper
#
# Writes ~/.polybot-modes (not in git) and restarts that bot. Missing line = paper. dry/live use their own ledger
# (runs/<bot>dry.db, runs/<bot>live.db). Never set the same wallet live on two servers.
set -eu
F=~/.polybot-modes
BOTS="wclonefast wsnipedog wsnipedog2 wlearn"
touch "$F"
if [ $# -eq 0 ]; then
    for b in $BOTS; do printf "%-11s %s\n" "$b" "$(grep -m1 "^$b=" "$F" | cut -d= -f2 || true)"; done | sed 's/ $/ paper/'
    exit
fi
b=$1 m=$2
case " $BOTS " in *" $b "*) ;; *) echo "unknown bot $b (one of: $BOTS)"; exit 1 ;; esac
case $m in paper|dry|live) ;; *) echo "mode must be paper, dry or live"; exit 1 ;; esac
if [ "$m" != paper ] && [ ! -s ~/.polybot-secrets ]; then echo "no ~/.polybot-secrets: add the wallet keys first"; exit 1; fi
grep -v "^$b=" "$F" > "$F.tmp" || true
echo "$b=$m" >> "$F.tmp" && mv "$F.tmp" "$F"
sudo systemctl restart "polybot-$b"
sleep 5
echo "$b -> $m"; tail -3 ~/poly/polybot/runs/"$b$([ "$m" = paper ] || echo "$m")".log 2>/dev/null || true
systemctl is-active "polybot-$b"
