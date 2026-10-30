#!/usr/bin/env bash
# Full end-to-end switchover verification.
#
# Usage:  ./run_test.sh [DURATION_SECONDS] [SWITCH_AT_SECONDS]
#   ./run_test.sh 80 18     (quick: switch after 18s)
#   ./run_test.sh 300 90    (5-minute run, switch after 90s)
#
# Env:
#   REVERSE=0   one-way demo: skips the reverse subscription so post-switch
#               rows exist ONLY on db2 (setup is invoked internally with it)
#   SKIP_SETUP=1   don't reset the dataset before running
#
# Assertions: 0 failed transactions; (reverse mode) row counts identical on
# both instances; (REVERSE=0 mode) db2 has strictly MORE rows than db1.
set -euo pipefail
BASE="$(cd "$(dirname "$0")" && pwd)"
PGUSER="${PGUSER:-$(whoami)}"
DUR=${1:-80}
SW_AT=${2:-18}

# Each run starts from a clean dataset unless SKIP_SETUP=1.
# (stale rows from a previous run would break identity-sequence collisions)
if [ "${SKIP_SETUP:-0}" = "0" ]; then
  ./setup_replication.sh
else
  # keep pgbouncer config in sync with the template even when skipping setup
  sed -e "s|@BASE@|$BASE|g" pgbouncer/pgbouncer.ini.template > pgbouncer/pgbouncer.ini
  printf '"pgbadmin" ""\n"%s" ""\n' "$PGUSER" > pgbouncer/userlist.txt
fi

echo "== build app =="
(cd "$BASE/app" && go build -o ../switchovertest .)

echo "== start pgbouncer =="
if [ -f "$BASE/pgbouncer/pgbouncer.pid" ] && kill -0 "$(cat "$BASE/pgbouncer/pgbouncer.pid")" 2>/dev/null; then
  echo "   already running"
else
  pgbouncer -d "$BASE/pgbouncer/pgbouncer.ini"
  sleep 1
fi

# make sure we start from db1 (5432) target — idempotent
sed -i "s|^appdb *=.*|appdb = host=127.0.0.1 port=5432 dbname=postgres|" "$BASE/pgbouncer/pgbouncer.ini"
psql -h 127.0.0.1 -p 6432 -d pgbouncer -U pgbadmin -Atc 'RELOAD;' >/dev/null

echo "== run load (${DUR}s total; switchover at ~t+${SW_AT}s) =="
./switchovertest -duration "${DUR}s" -rate 5 -v \
  -dsn "host=127.0.0.1 port=6432 user=$PGUSER dbname=appdb sslmode=disable" \
  > "$BASE/load.log" 2>&1 &
LOAD_PID=$!
sleep "$SW_AT"

./switchover.sh 5433

wait $LOAD_PID || STATUS=$?
STATUS=${STATUS:-$?}
echo ""
echo "== load generator output =="
tail -n 40 "$BASE/load.log"

echo ""
echo "== post-run data consistency =="
c1=$(psql -h 127.0.0.1 -p 5432 -d postgres -U "$PGUSER" -Atc "SELECT count(*) FROM names;")
c2=$(psql -h 127.0.0.1 -p 5433 -d postgres -U "$PGUSER" -Atc "SELECT count(*) FROM names;")
u1=$(psql -h 127.0.0.1 -p 5432 -d postgres -U "$PGUSER" -Atc "SELECT count(*) FROM names WHERE last_name !~ '2\$';")
u2=$(psql -h 127.0.0.1 -p 5433 -d postgres -U "$PGUSER" -Atc "SELECT count(*) FROM names WHERE last_name !~ '2\$';")
echo "   db1(5432) rows=$c1 (missing-final-'2': $u1)"
echo "   db2(5433) rows=$c2 (missing-final-'2': $u2)"

if [ "${REVERSE:-auto}" = "auto" ]; then
  # auto-detect: does the reverse subscription exist? (was setup run with REVERSE=0?)
  if psql -h 127.0.0.1 -p 5432 -d postgres -U "$PGUSER" -Atc "SELECT 1 FROM pg_subscription WHERE subname='sub_names_reverse';" 2>/dev/null | grep -q '^1$'; then
    REVERSE=1
  else
    REVERSE=0
  fi
fi

if [ "$REVERSE" = "1" ]; then
  if [ "$STATUS" -eq 0 ] && [ "$c1" = "$c2" ] && [ "$u1" = "0" ] && [ "$u2" = "0" ]; then
    echo ""
    echo "✅ TEST PASSED — 0 failed txs; both instances consistent"
    echo "   (differences none because reverse replication copies every new db2"
    echo "    row straight back to db1 — see README 'Bidirectional replication')"
    exit 0
  fi
else
  if [ "$STATUS" -eq 0 ] && [ "$c2" -gt "$c1" ] && [ "$u1" = "0" ] && [ "$u2" = "0" ]; then
    echo ""
    echo "✅ TEST PASSED — 0 failed txs; db2 has $((c2 - c1)) rows that exist nowhere else,"
    echo "   proving post-switch writes landed exclusively on the new instance."
    exit 0
  fi
fi
echo "❌ TEST FAILED (app exit=$STATUS)"
exit 1
