#!/usr/bin/env bash
# Project-wide setup: APIs, budget alert, lake bucket, firewall. Idempotent.
set -euo pipefail
source "$(dirname "$0")/env.sh"

gcloud services enable --project "$PROJECT" \
  compute.googleapis.com sqladmin.googleapis.com run.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com secretmanager.googleapis.com \
  iap.googleapis.com billingbudgets.googleapis.com cloudresourcemanager.googleapis.com \
  iam.googleapis.com bigquery.googleapis.com

# Budget alert scoped to this project only (the billing account may pay for other projects).
billing=$(gcloud billing projects describe "$PROJECT" --format='value(billingAccountName)')
billing=${billing#billingAccounts/}
if ! gcloud billing budgets list --billing-account="$billing" --billing-project="$PROJECT" \
    --format='value(displayName)' | grep -qx "$BUDGET_NAME"; then
  gcloud billing budgets create --billing-account="$billing" --billing-project="$PROJECT" \
    --display-name="$BUDGET_NAME" --budget-amount="$BUDGET_AMOUNT" \
    --filter-projects="projects/$PROJECT_NUMBER" \
    --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
fi

if ! gcloud storage buckets describe "$LAKE_BUCKET" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud storage buckets create "$LAKE_BUCKET" --project "$PROJECT" \
    --location="$REGION" --default-storage-class=STANDARD \
    --uniform-bucket-level-access --public-access-prevention
fi
# Postgres dumps under backups/ expire after 14 days; the lake itself never expires.
lifecycle=$(mktemp)
cat > "$lifecycle" <<'JSON'
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 14, "matchesPrefix": ["backups/"]}}]}
JSON
gcloud storage buckets update "$LAKE_BUCKET" --project "$PROJECT" --lifecycle-file="$lifecycle"
rm -f "$lifecycle"

# BigQuery dataset for the large processed datasets (their Parquet source of truth stays in the lake).
if ! bq --project_id="$PROJECT" show --dataset "$PROJECT:$BQ_DATASET" >/dev/null 2>&1; then
  bq --project_id="$PROJECT" --location="$REGION" mk --dataset \
    --description "basecast processed datasets too large for Postgres" "$PROJECT:$BQ_DATASET"
fi

# SSH only through IAP; no RDP; nothing else open from the internet.
for rule in default-allow-ssh default-allow-rdp; do
  if gcloud compute firewall-rules describe "$rule" --project "$PROJECT" >/dev/null 2>&1; then
    gcloud compute firewall-rules delete "$rule" --project "$PROJECT" --quiet
  fi
done
if ! gcloud compute firewall-rules describe allow-ssh-from-iap --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules create allow-ssh-from-iap --project "$PROJECT" \
    --network=default --direction=INGRESS --source-ranges="$IAP_RANGE" \
    --allow=tcp:22 --target-tags="$VM_TAG"
fi
