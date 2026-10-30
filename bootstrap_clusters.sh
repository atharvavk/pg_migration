#!/usr/bin/env bash
# Creates the two throwaway postgres clusters (db1, db2) if they don't exist.
# Never deletes anything; safe to re-run. setup_replication.sh calls this,
# so on a fresh clone running setup is enough.
set -euo pipefail
BASE="$(cd "$(dirname "$0")" && pwd)"
PGUSER="${PGUSER:-$(whoami)}"
PGPORT_A="${PGPORT_A:-5432}"
PGPORT_B="${PGPORT_B:-5433}"

# postgres refuses to run as root
if [ "$(id -u)" = "0" ]; then
  echo "FATAL: run as a non-root user who also owns the data directories"; exit 1
fi

for entry in "db1:$PGPORT_A" "db2:$PGPORT_B"; do
  dir="${entry%%:*}"
  port="${entry##*:}"
  PGDATA="$BASE/$dir"
  if [ -f "$PGDATA/PG_VERSION" ]; then
    echo "== $dir already exists, skipping (target port $port) =="
    continue
  fi
  echo "== creating $dir via initdb (port $port, superuser $PGUSER) =="
  initdb -U "$PGUSER" -A trust --no-sync -D "$PGDATA" >/dev/null
  cat >> "$PGDATA/postgresql.conf" <<EOF
# appended by bootstrap_clusters.sh
port = $port
unix_socket_directories = '$BASE'
EOF
  # explicit replication rules: logical replication connections must match them
  cat >> "$PGDATA/pg_hba.conf" <<'EOF'
# appended by bootstrap_clusters.sh (trust rules; lab-only, see README §6)
local   replication   all                                   trust
host    replication   all            127.0.0.1/32           trust
host    replication   all            ::1/128                trust
EOF
done
echo "bootstrap done"
