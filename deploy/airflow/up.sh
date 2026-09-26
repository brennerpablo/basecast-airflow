#!/bin/bash
# Runs on the VM as root (called by deploy/gcp/05-airflow-up.sh). Installs the compose project in
# /opt/basecast/airflow, writes its .env once, builds the image and starts Airflow. The repo checkout
# the DAGs and pipelines run from is already in /opt/basecast/src.
set -euo pipefail
: "${SQL_CONNECTION:?}"
SRC=$(dirname "$0")
DEST=/opt/basecast/airflow

mkdir -p "$DEST"/{logs,config,plugins}
rsync -a --exclude .env "$SRC"/ "$DEST"/

# Secrets come from Secret Manager (the VM's service account can read them); the Fernet key and
# JWT secret are generated here once and never leave the VM.
secret() { gcloud secrets versions access latest --secret "$1"; }
umask 077
if [ ! -f "$DEST/.env" ]; then
  cat > "$DEST/.env" <<EOF
AIRFLOW_UID=50000
SQL_CONNECTION=$SQL_CONNECTION
AIRFLOW_DB_PASSWORD=$(secret pg-airflow-password)
AIRFLOW_ADMIN_PASSWORD=$(secret airflow-admin-password)
FERNET_KEY=$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')
JWT_SECRET=$(openssl rand -hex 32)
EOF
fi
# Keys added after the first install are appended once.
grep -q '^BASECAST_DB_PASSWORD=' "$DEST/.env" ||
  echo "BASECAST_DB_PASSWORD=$(secret pg-basecast-writer-password)" >> "$DEST/.env"

chown -R 50000:0 "$DEST"/{logs,config,plugins}
cd "$DEST"
docker compose pull --quiet --ignore-buildable
docker compose build --quiet
docker compose up -d
# Task processes fork from the scheduler and keep modules already imported: restart so new pipeline
# code is picked up even when no container had to be recreated.
docker compose restart airflow-scheduler airflow-dag-processor
docker compose ps
