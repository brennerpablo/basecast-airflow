#!/usr/bin/env bash
# Copies deploy/airflow to the VM and (re)starts Airflow there. Safe to re-run after any change.
set -euo pipefail
source "$(dirname "$0")/env.sh"
ssh_opts=(--project "$PROJECT" --zone "$ZONE" --tunnel-through-iap)

gcloud compute ssh "$VM_NAME" "${ssh_opts[@]}" --command "rm -rf /tmp/airflow-deploy"
gcloud compute scp --recurse "$(dirname "$0")/../airflow" "$VM_NAME":/tmp/airflow-deploy "${ssh_opts[@]}"
gcloud compute ssh "$VM_NAME" "${ssh_opts[@]}" \
  --command "sudo SQL_CONNECTION='$SQL_CONNECTION' bash /tmp/airflow-deploy/up.sh"
