#!/usr/bin/env bash
# Safe export of EVERY bot database, log and model setting before a server is stopped. Never touches secrets.
#
#   bash export_data.sh                  -> ~/export/polybot-export-<utc time>.tar.gz   (run on the server)
#
# Then copy it to your PC:  scp -i yourkey.pem ec2-user@<server-ip>:~/export/polybot-export-*.tar.gz .
# `VACUUM INTO` writes a consistent copy even while a bot is still writing to the database.
set -u
STAMP=$(date -u +%Y%m%d-%H%M)
DIR="$HOME/export/polybot-export-$STAMP"
mkdir -p "$DIR"
command -v sqlite3 >/dev/null || { echo "sqlite3 is not installed: sudo dnf install -y sqlite"; exit 1; }
for runs in "$HOME"/*/polybot/runs; do
  [ -d "$runs" ] || continue
  repo=$(basename "$(dirname "$(dirname "$runs")")")                  # live, t01, ...
  for db in "$runs"/*.db; do
    [ -f "$db" ] || continue
    out="$DIR/$repo-$(basename "$db")"
    if sqlite3 "$db" "VACUUM INTO '$out'" 2>/dev/null; then
      ok=$(sqlite3 "$out" "PRAGMA quick_check" 2>&1 | head -1)
      n=$(sqlite3 "$out" "select count(*) from ticks" 2>/dev/null || echo "-")
      echo "db   $repo/$(basename "$db")  $(du -h "$out" | cut -f1)  ticks=$n  check=$ok"
    else
      echo "FAIL $repo/$(basename "$db") (is it a sqlite file?)"
    fi
  done
  for f in "$runs"/*.log "$runs"/*.json; do
    [ -f "$f" ] && cp "$f" "$DIR/$repo-$(basename "$f")"
  done
  cp "$(dirname "$runs")/model.env" "$DIR/$repo-model.env" 2>/dev/null
  git -C "$(dirname "$runs")" log --oneline -40 > "$DIR/$repo-git-log.txt" 2>/dev/null
done
date -u > "$DIR/exported-at.txt"
tar -C "$HOME/export" -czf "$DIR.tar.gz" "$(basename "$DIR")" && rm -rf "$DIR"
echo; ls -lh "$DIR.tar.gz"
echo "Not included on purpose: ~/.polybot-secrets (keys). Keep your own copy of the private key and funder."
