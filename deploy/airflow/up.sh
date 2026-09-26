#!/bin/bash
# Runs on the VM as root (called by deploy/gcp/05-airflow-up.sh). Installs the compose project in
# /opt/basecast/airflow, writes its .env once, and starts Airflow.
set -euo pipefail
: "${SQL_CONNECTION:?}"
SRC=$(dirname "$0")
DEST=/opt/basecast/airflow

mkdir -p "$DEST"/{dags,logs,config,plugins}
rsync -a --exclude .env "$SRC"/ "$DEST"/

# Secrets come from Secret Manager (the VM's service account can read them); the Fernet key and
# JWT secret are generated here once and never leave the VM.
if [ ! -f "$DEST/.env" ]; then
  secret() { gcloud secrets versions access latest --secret "$1"; }
  umask 077
  cat > "$DEST/.env" <<EOF
AIRFLOW_UID=50000
SQL_CONNECTION=$SQL_CONNECTION
AIRFLOW_DB_PASSWORD=$(secret pg-airflow-password)
AIRFLOW_ADMIN_PASSWORD=$(secret airflow-admin-password)
FERNET_KEY=$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')
JWT_SECRET=$(openssl rand -hex 32)
EOF
fi

chown -R 50000:0 "$DEST"/{dags,logs,config,plugins}
cd "$DEST"
docker compose pull --quiet
docker compose up -d
docker compose ps
