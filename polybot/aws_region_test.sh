#!/usr/bin/env bash
# Find the fastest AWS region for the bot: launches one small temporary server per region, runs region_probe.py
# on it, saves the result, and TERMINATES the server (also on Ctrl-C or error). Cost: a few cents per region.
#
#   ./aws_region_test.sh                                  # default regions, 180 s probe each (run in parallel)
#   SECS=600 ./aws_region_test.sh eu-west-1 eu-north-1    # pick regions / longer probe
#
# Needs: aws cli v2 configured (aws configure), an ssh key at ~/.ssh/id_ed25519(.pub) (ssh-keygen -t ed25519).
set -euo pipefail
cd "$(dirname "$0")"

REGIONS=("${@:-eu-west-1 eu-north-1 ap-northeast-1}")
read -r -a REGIONS <<< "${REGIONS[*]}"
SECS="${SECS:-180}"
TYPE="${TYPE:-t4g.small}"                      # 2 vCPU ARM, plenty for a 3-minute probe
KEY="${KEY:-$HOME/.ssh/id_ed25519}"
TAG="polybot-probe"
OUT="runs/region-probe-$(date +%Y%m%d-%H%M)"
mkdir -p "$OUT"
MYIP="$(curl -s https://checkip.amazonaws.com)/32"

probe_region() {
  R=$1 IID="" SG=""                  # not local: the EXIT trap reads them after the function returns
  cleanup() {
    [ -n "$IID" ] && aws ec2 terminate-instances --region "$R" --instance-ids "$IID" >/dev/null 2>&1 \
      && aws ec2 wait instance-terminated --region "$R" --instance-ids "$IID" 2>/dev/null
    [ -n "$SG" ] && aws ec2 delete-security-group --region "$R" --group-id "$SG" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT
  aws ec2 import-key-pair --region "$R" --key-name "$TAG" --public-key-material "fileb://$KEY.pub" >/dev/null 2>&1 || true
  SG=$(aws ec2 create-security-group --region "$R" --group-name "$TAG-$$" --description "polybot probe ssh" \
        --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --region "$R" --group-id "$SG" --protocol tcp --port 22 --cidr "$MYIP" >/dev/null
  AMI=$(aws ssm get-parameter --region "$R" --query Parameter.Value --output text \
        --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id)
  IID=$(aws ec2 run-instances --region "$R" --image-id "$AMI" --instance-type "$TYPE" --key-name "$TAG" \
        --security-group-ids "$SG" --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG}]" \
        --instance-initiated-shutdown-behavior terminate --query 'Instances[0].InstanceId' --output text)
  aws ec2 wait instance-running --region "$R" --instance-ids "$IID"
  IP=$(aws ec2 describe-instances --region "$R" --instance-ids "$IID" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR "ubuntu@$IP")
  for _ in $(seq 1 30); do "${SSH[@]}" true 2>/dev/null && break; sleep 5; done
  scp -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR region_probe.py "ubuntu@$IP:"
  "${SSH[@]}" "sudo shutdown -h +30 >/dev/null 2>&1;                       # safety: dies by itself after 30 min
    sudo apt-get -qq update >/dev/null && sudo apt-get -qq install -y python3-venv >/dev/null &&
    python3 -m venv v && v/bin/pip -q install requests websockets certifi && v/bin/python region_probe.py $SECS" \
    | tee "$OUT/$R.txt"
}

echo "probing ${REGIONS[*]} ($SECS s each, in parallel) -> $OUT"
for R in "${REGIONS[@]}"; do (probe_region "$R" > "$OUT/$R.log" 2>&1 && echo "done $R" || echo "FAILED $R, see $OUT/$R.log") & done
wait

python3 - "$OUT" <<'EOF'
import json, pathlib, sys
rows = []
for f in sorted(pathlib.Path(sys.argv[1]).glob("*.txt")):
    for line in f.read_text().splitlines():
        if line.startswith("RESULT "):
            rows.append((f.stem, json.loads(line[7:])))
rows.sort(key=lambda r: (bool(r[1].get("blocked")), r[1].get("reaction_p50_ms") or 1e9))
print(f"\n{'region':16}{'country':>8}{'blocked':>9}{'reaction p50':>14}{'p95':>8}{'binance p50':>13}{'book p50':>10}{'clob rtt':>10}")
for r, x in rows:
    print(f"{r:16}{x['country'] or '?':>8}{str(x['blocked']):>9}{x.get('reaction_p50_ms') or '-':>14}{x.get('reaction_p95_ms') or '-':>8}"
          f"{x['binance_age_ms']['p50'] or '-':>13}{x['book_age_ms']['p50'] or '-':>10}{x['clob_rtt_ms']['p50'] or '-':>10}")
ok = [r for r in rows if not r[1].get("blocked")]
print(f"\nbest allowed region: {ok[0][0]}" if ok else "\nno allowed region found")
EOF
echo "all probe servers terminated. Check: aws ec2 describe-instances --filters Name=tag:Name,Values=$TAG Name=instance-state-name,Values=running --region <region>"
