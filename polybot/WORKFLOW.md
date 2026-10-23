# From recorded data to a model you can trust (fast-taker)

Why this exists: on 23 Oct 2026 the old paper bot and replay predicted +3 c/share for T-0.1, but about 34 real
round trips from the Stockholm server averaged -4 c/share (-$9.6 of $33.93). The taker order has a ~150 ms
server-side delay, so a race that looks good with a 0.1 s paper delay is lost at ~0.5 s. Everything below is about
never trusting a paper number that has not been checked against reality.

## 1. Record (any server, paper only)
Every paper bot writes `runs/<ledger>.db`: `ticks` (books, 10 Hz), `binance` (mids), `prints`, `signals`, `trades`.
More days of data is the cure for small samples. Start with `source realistic.env; source model.env`.

## 2. Export before a server is stopped or terminated
`bash export_data.sh` on the server -> `~/export/polybot-export-<time>.tar.gz` (databases, logs, model.env, no keys).
Copy it to your PC with `scp`. Also take an EBS snapshot in the AWS console. *Stop* keeps the disk, *Terminate*
deletes it.

## 3. Check the simulator against reality (needs real orders in `signals`)
`python calibrate.py runs/live-t01.db --since <t> --pad 0.02` — if the best row is far below ~85%, recorded
books do not predict real fills and no paper result for that strategy can be trusted.

## 4. Look for settings that work out of sample
`python latency_sweep.py a.db b.db --pad 0.02 --hold 12 --delays 0.3,0.5,0.7` — edge per delay.
`python tune.py a.db b.db --quick` — search on the first 60% of the time, score on the last 40%. Only believe the
test column. Databases that record the same market at the same time count twice: use one per period.

## 5. Forward paper test with realistic fills (the server whose latency you will trade from)
`set -a; source realistic.env; source model.env; set +a; python taker.py`
Compare its trades with `python report.py` and, once real orders exist, `python trade_audit.py`.

## 6. Live only after
- the realistic paper result and the replay agree with each other on several days, and
- a tiny live test (loss cap per UTC day, `TK_LOSS_SINCE`) reproduces them within the noise (~±1 c/share per ~100
  trades). `python trade_audit.py` splits each live trade into move / entry / exit / fees to show where it differs.

Never use a VPN or a server in a blocked country to get lower latency (Ireland, France, Japan, Korea were tested and
are blocked; Stockholm and Mumbai passed). Measured latency does not improve there anyway.
