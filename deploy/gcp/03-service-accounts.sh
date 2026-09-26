#!/usr/bin/env bash
# One service account per workload, each with only what it needs. Idempotent (bindings are additive).
set -euo pipefail
source "$(dirname "$0")/env.sh"

sa_ensure() {  # email, display name
  gcloud iam service-accounts describe "$1" --project "$PROJECT" >/dev/null 2>&1 && return 0
  gcloud iam service-accounts create "${1%%@*}" --project "$PROJECT" --display-name "$2"
  sleep 15  # a new service account takes a moment before IAM accepts it in bindings
}
project_role() {
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$1" --role "$2" \
    --condition=None --quiet >/dev/null
}
bucket_role() {
  gcloud storage buckets add-iam-policy-binding "$LAKE_BUCKET" --project "$PROJECT" \
    --member "serviceAccount:$1" --role "$2" >/dev/null
}
secret_role() {
  gcloud secrets add-iam-policy-binding "$2" --project "$PROJECT" \
    --member "serviceAccount:$1" --role roles/secretmanager.secretAccessor >/dev/null
}

secret_ensure airflow-admin-password
secret_ensure get-data-api-token

# Airflow VM: reaches Cloud SQL through the proxy, writes the lake, ships logs and metrics.
sa_ensure "$VM_SA" "Airflow VM"
for role in roles/cloudsql.client roles/logging.logWriter roles/monitoring.metricWriter; do
  project_role "$VM_SA" "$role"
done
bucket_role "$VM_SA" roles/storage.objectAdmin
# BigQuery: run load and query jobs, and write the basecast dataset.
for role in roles/bigquery.jobUser roles/bigquery.dataEditor; do
  project_role "$VM_SA" "$role"
done
# Vertex AI: Gemini reads chart values and image-only pages of the documents.
project_role "$VM_SA" roles/aiplatform.user
for s in pg-airflow-password pg-basecast-writer-password airflow-admin-password; do
  secret_role "$VM_SA" "$s"
done

# get-data on Cloud Run: read-only on the lake and on Postgres (basecast_reader).
sa_ensure "$API_SA" "get-data on Cloud Run"
project_role "$API_SA" roles/cloudsql.client
bucket_role "$API_SA" roles/storage.objectViewer
for role in roles/bigquery.jobUser roles/bigquery.dataViewer; do
  project_role "$API_SA" "$role"
done
for s in pg-basecast-reader-password get-data-api-token; do
  secret_role "$API_SA" "$s"
done

echo "Service accounts ready: $VM_SA, $API_SA"
