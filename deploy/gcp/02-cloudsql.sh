#!/usr/bin/env bash
# Cloud SQL Postgres: the cheapest shared-core instance, holding Airflow's metadata (`airflow`)
# and the app's operational data (`basecast`). Idempotent.
# Needs `cloud-sql-proxy` on PATH (or CLOUD_SQL_PROXY=/path/to/binary) and `psql`.
set -euo pipefail
source "$(dirname "$0")/env.sh"
PROXY=${CLOUD_SQL_PROXY:-cloud-sql-proxy}
PROXY_PORT=5439

for s in pg-postgres-password pg-airflow-password pg-basecast-writer-password pg-basecast-reader-password; do
  secret_ensure "$s"
done

if ! gcloud sql instances describe "$SQL_INSTANCE" --project "$PROJECT" >/dev/null 2>&1; then
  # Shared-core tiers exist only in the Enterprise edition, and PG16+ defaults to Enterprise Plus.
  # SSD instead of HDD: HDD IOPS scale with size and are tiny at 10 GB; SSD costs US$ 0.80/month more.
  # Connector enforcement: no authorized networks, only the Cloud SQL proxy/connectors get in.
  gcloud sql instances create "$SQL_INSTANCE" --project "$PROJECT" \
    --database-version=POSTGRES_17 --edition=enterprise --tier=db-f1-micro \
    --zone="$ZONE" --availability-type=zonal \
    --storage-type=SSD --storage-size=10 --storage-auto-increase \
    --backup-start-time=08:00 --retained-backups-count=7 --no-enable-point-in-time-recovery \
    --maintenance-window-day=TUE --maintenance-window-hour=9 \
    --ssl-mode=ENCRYPTED_ONLY --connector-enforcement=REQUIRED \
    --deletion-protection \
    --root-password="$(secret_get pg-postgres-password)"
fi

for db in airflow basecast; do
  if ! gcloud sql databases describe "$db" --instance "$SQL_INSTANCE" --project "$PROJECT" >/dev/null 2>&1; then
    gcloud sql databases create "$db" --instance "$SQL_INSTANCE" --project "$PROJECT"
  fi
done

"$PROXY" --port "$PROXY_PORT" --quota-project "$PROJECT" "$SQL_CONNECTION" >/dev/null 2>&1 &
proxy_pid=$!
trap 'kill $proxy_pid 2>/dev/null' EXIT
for _ in $(seq 30); do nc -z 127.0.0.1 "$PROXY_PORT" 2>/dev/null && break; sleep 1; done

export PGHOST=127.0.0.1 PGPORT=$PROXY_PORT PGSSLMODE=disable  # the proxy already encrypts
PGPASSWORD=$(secret_get pg-postgres-password) psql -v ON_ERROR_STOP=1 -q -U postgres -d postgres \
  -v airflow_pw="$(secret_get pg-airflow-password)" \
  -v writer_pw="$(secret_get pg-basecast-writer-password)" \
  -v reader_pw="$(secret_get pg-basecast-reader-password)" \
  -f "$(dirname "$0")/sql/roles.sql"

# Tables the writer creates become readable by the reader (the API).
PGPASSWORD=$(secret_get pg-basecast-writer-password) psql -v ON_ERROR_STOP=1 -q -U basecast_writer -d basecast \
  -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO basecast_reader;"

echo "Cloud SQL ready: $SQL_CONNECTION"
