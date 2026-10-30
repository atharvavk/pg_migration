#!/usr/bin/env bash
# Zero-downtime switchover: move pgbouncer's appdb target from the current
# primary to <target_port> (5432 or 5433) while the app keeps working.
#
# Order of operations (this ordering is what makes it zero-downtime):
#   1. PAUSE            -> in-flight txs finish; new client requests buffer
#   2. (drain done)     -> capture LSN on the OLD source
#   3. bump target seq  -> sequences are not replicated; else new writes
#                          conflict with replicated PKs
#   4. wait catch-up    -> target's slot confirmed_flush_lsn >= captured LSN
#   5. RELOAD           -> new target takes effect while still paused
#   6. RESUME           -> buffered txs replay against the new target
#
# Config: PGUSER (default: your OS user), PGPORT_A (5432), PGPORT_B (5433).
set -euo pipefail

TARGET=${1:?usage: switchover.sh <5432|5433>}
BASE="$(cd "$(dirname "$0")" && pwd)"
PGUSER="${PGUSER:-$(whoami)}"
PGPORT_A="${PGPORT_A:-5432}"
PGPORT_B="${PGPORT_B:-5433}"
INI="$BASE/pgbouncer/pgbouncer.ini"
GBR="psql -h 127.0.0.1 -p 6432 -d pgbouncer -U pgbadmin -X -At"

case $TARGET in
  5433|$PGPORT_B) OLD=$PGPORT_A; SLOT=sub_names_forward ;;   # slot lives on old source (db1)
  5432|$PGPORT_A) OLD=$PGPORT_B; SLOT=sub_names_reverse ;;   # slot lives on old source (db2)
  *) echo "target must be $PGPORT_A or $PGPORT_B"; exit 1 ;;
esac
PG_OLD="psql -h 127.0.0.1 -p $OLD -d postgres -U $PGUSER -X -At -v ON_ERROR_STOP=1"
PG_NEW="psql -h 127.0.0.1 -p $TARGET -d postgres -U $PGUSER -X -At -v ON_ERROR_STOP=1"

echo "[switchover] $OLD -> $TARGET at $(date +%T.%N)"

echo "[1/6] PAUSE — draining in-flight txs, new requests buffer..."
# admin console works while paused, so we sample POOLS during the drain
( timeout 120 $GBR -c 'PAUSE;' >/dev/null || { echo "FATAL: PAUSE failed/timeout"; kill $$; } ) &
PAUSE_PID=$!

SEEN_BUFFERED=0
for i in $(seq 1 30); do
  wait_count=$($GBR -c "SHOW POOLS;" | awk -F'|' '$1=="appdb" {print $4 + 0}')
  if [ "${wait_count:-0}" -gt 0 ]; then
    SEEN_BUFFERED=1
    echo "        buffered client connections waiting: $wait_count"
    break
  fi
  kill -0 $PAUSE_PID 2>/dev/null || break   # pause already finished (nothing to drain)
  sleep 0.3
done
wait $PAUSE_PID

# evidence that drain finished: no more server connections on the old side
srv_count=$($GBR -c "SHOW SERVERS;" | grep -c '^appdb|' || true)
echo "      PAUSE complete. appdb server connections remaining: $srv_count"

LSN=$(timeout 60 $PG_OLD -c "SELECT pg_current_wal_lsn();")
echo "[2/6] drained. old-source write LSN captured: $LSN"
active=$($PG_OLD -c "SELECT count(*) FROM pg_stat_activity
                     WHERE datname='postgres' AND state <> 'idle'
                       AND application_name NOT LIKE 'sub_names%'
                       AND pid <> pg_backend_pid();")
echo "      non-idle (non-repl) backends on old source: $active"

echo "[3/6] bumping $TARGET identity sequence (sequences aren't replicated...)"
read -r LV IC <<<"$( $PG_OLD -At -F ' ' -c "SELECT last_value, is_called FROM public.names_id_seq;" )"
$PG_NEW -c "SELECT setval('public.names_id_seq', '$LV'::bigint, '$IC'::boolean);" >/dev/null
echo "      seq set to last_value=$LV is_called=$IC on $TARGET"

echo "[4/6] waiting for $TARGET to fully catch up to $LSN..."
for i in $(seq 1 120); do
  behind=$($PG_OLD -c "SELECT pg_wal_lsn_diff(confirmed_flush_lsn, '$LSN'::pg_lsn)
                       FROM pg_replication_slots WHERE slot_name='$SLOT';")
  if [ -n "$behind" ] && [ "$behind" -le 0 ]; then echo "      caught up."; break; fi
  [ "$i" = "120" ] && { echo "FATAL: replication did not catch up"; exit 1; }
  sleep 0.5
done

sed -i "s|^appdb *=.*|appdb = host=127.0.0.1 port=$TARGET dbname=postgres|" "$INI"

echo "[5/6] RELOAD (target=$TARGET active while paused)"
$GBR -c 'RELOAD;' >/dev/null

echo "[6/6] RESUME — releasing buffered traffic to :$TARGET"
$GBR -c 'RESUME;' >/dev/null

newtarget=$($GBR -c "SHOW DATABASES;" | awk -F'|' -v t=$TARGET '$1=="appdb" {print $2}')
echo "      current appdb target: $newtarget"
echo "[switchover] DONE at $(date +%T.%N) (buffered-traffic evidence: SEEN_BUFFERED=$SEEN_BUFFERED)"
