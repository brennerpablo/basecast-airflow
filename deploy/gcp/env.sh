# Shared names for the GCP scripts. Sourced, never run.
# Every command passes --project explicitly: the local gcloud default project may be another one.

PROJECT=basecast-509812
PROJECT_NUMBER=132503253928
REGION=us-central1
ZONE=us-central1-a

LAKE_BUCKET=gs://basecast-509812-lake
BUDGET_NAME="basecast monthly"
# The billing account is in BRL, so the budget is too (~US$ 45).
BUDGET_AMOUNT=250BRL

SQL_INSTANCE=basecast-pg
SQL_CONNECTION="$PROJECT:$REGION:$SQL_INSTANCE"

VM_NAME=basecast-airflow
VM_TAG=airflow
VM_SA="airflow-vm@$PROJECT.iam.gserviceaccount.com"
API_SA="get-data-run@$PROJECT.iam.gserviceaccount.com"

# Google's IAP TCP forwarding range: the only source allowed to reach SSH.
IAP_RANGE=35.235.240.0/20

# Creates a secret holding a random 32-char value, unless it already exists.
secret_ensure() {
  gcloud secrets describe "$1" --project "$PROJECT" >/dev/null 2>&1 && return 0
  openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c 32 |
    gcloud secrets create "$1" --project "$PROJECT" \
      --replication-policy=user-managed --locations="$REGION" --data-file=-
}

secret_get() {
  gcloud secrets versions access latest --secret "$1" --project "$PROJECT"
}
