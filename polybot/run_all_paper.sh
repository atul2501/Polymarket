#!/usr/bin/env bash
# One command: realistic PAPER trading of four fast-taker models plus the dashboard (macOS or Linux). No real orders.
#
#   bash polybot/run_all_paper.sh        Ctrl+C stops everything
#
# Each model writes polybot/runs/<name>.db (+ .log, .out). Open http://127.0.0.1:8787 for the dashboard.
# T-0.1 is not run: under realistic.env it is identical to T-fast (they differed only in the old paper delay).
cd "$(dirname "$0")" || exit 1
VENV="${VENV:-$HOME/.venv-poly}"
[ -d "$VENV" ] || python3 -m venv "$VENV" || { echo "python3 with venv is required"; exit 1; }
. "$VENV/bin/activate"
pip -q install requests websockets certifi || { echo "pip install failed (internet?)"; exit 1; }
mkdir -p runs
PIDS=()

start() {   # start <name> <ledger> [VAR=value ...]
  local name=$1 ledger=$2
  shift 2
  ( set -a; . ./realistic.env; set +a
    export TK_LEDGER="runs/$ledger.db" TK_SIZE=10 "$@"
    exec python taker.py ) > "runs/$ledger.out" 2>&1 &
  PIDS+=($!)
  echo "started $name -> runs/$ledger.db (pid $!)"
}

start T-fast     t-fast     TK_MOVE_BPS=2 TK_MOVE_SEC=1 TK_HOLD=5
start T-fast-h12 t-fast-h12 TK_MOVE_BPS=2 TK_MOVE_SEC=1 TK_HOLD=12
start T-3bp      t-3bp      TK_MOVE_BPS=3 TK_MOVE_SEC=1 TK_HOLD=5
start W-3bp      w-3bp      TK_MOVE_BPS=3 TK_MOVE_SEC=3 TK_SETTLE=1 TK_MIN_ASK=0.15 TK_MAX_ASK=0.85 TK_COOLDOWN=2

( export DASH_BOTS="T-fast=t-fast.db,T-fast-h12=t-fast-h12.db,T-3bp=t-3bp.db,W-3bp=w-3bp.db"
  export DASH_SINCE="$(date +%H:%M)" DASH_HOURS=24 DASH_MARK="taker start"
  exec python dashboard.py ) > runs/dashboard.out 2>&1 &
PIDS+=($!)
echo "started dashboard (pid $!)"

trap 'echo; echo "stopping..."; kill "${PIDS[@]}" 2>/dev/null; exit 0' INT TERM
command -v caffeinate >/dev/null && { caffeinate -i -w $$ & }          # no sleep while this script runs (macOS)
sleep 8
(command -v open >/dev/null && open "http://127.0.0.1:8787") || (command -v xdg-open >/dev/null && xdg-open "http://127.0.0.1:8787") || true
echo
echo "Running. Dashboard: http://127.0.0.1:8787   Ctrl+C stops everything."
echo "Numbers any time:   python report.py runs/t-fast.db      (also t-fast-h12, t-3bp, w-3bp)"
echo "Logs:               tail -f runs/t-fast.log"
wait
