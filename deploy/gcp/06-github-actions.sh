#!/usr/bin/env bash
# GitHub Actions deploys, as in fundsys: Workload Identity Federation (no service account keys), one
# deploy service account per repo, and the repo secrets the workflows read. Idempotent.
# Needs `gh` logged in with admin on both repos. AIRFLOW_ENV_FILE and ENV_YAML are seeded only when
# missing (from the VM's current .env, so the Fernet key survives, and from Secret Manager); after
# that the GitHub secret is the source of truth and every deploy writes it out.
set -euo pipefail
source "$(dirname "$0")/env.sh"

AIRFLOW_REPO=brennerpablo/basecast-airflow
GET_DATA_REPO=brennerpablo/basecast-get-data
POOL=github-pool
PROVIDER=github-provider
POOL_NAME="projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL"
AIRFLOW_DEPLOY_SA="airflow-deploy@$PROJECT.iam.gserviceaccount.com"
GET_DATA_DEPLOY_SA="get-data-deploy@$PROJECT.iam.gserviceaccount.com"
# Cloud Build runs `gcloud run deploy --source` as the Compute Engine default service account.
BUILD_SA="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

# Only these two repos, and only from main (pushes and manual runs alike), can get a token.
condition="assertion.repository in ['$AIRFLOW_REPO', '$GET_DATA_REPO'] && assertion.ref == 'refs/heads/main'"
mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner,attribute.ref=assertion.ref"

if ! gcloud iam workload-identity-pools describe "$POOL" --location=global --project "$PROJECT" >/dev/null 2>&1; then
  gcloud iam workload-identity-pools create "$POOL" --location=global --project "$PROJECT" \
    --display-name="GitHub Actions"
fi
provider_args=(--workload-identity-pool="$POOL" --location=global --project "$PROJECT"
  --attribute-mapping="$mapping" --attribute-condition="$condition")
if gcloud iam workload-identity-pools providers describe "$PROVIDER" --workload-identity-pool="$POOL" \
    --location=global --project "$PROJECT" >/dev/null 2>&1; then
  gcloud iam workload-identity-pools providers update-oidc "$PROVIDER" "${provider_args[@]}" --quiet
else
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" "${provider_args[@]}" \
    --display-name="GitHub" --issuer-uri=https://token.actions.githubusercontent.com
fi

sa_ensure() {  # email, display name
  gcloud iam service-accounts describe "$1" --project "$PROJECT" >/dev/null 2>&1 && return 0
  gcloud iam service-accounts create "${1%%@*}" --project "$PROJECT" --display-name "$2"
  sleep 15  # a new service account takes a moment before IAM accepts it in bindings
}
project_role() {
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$1" --role "$2" \
    --condition=None --quiet >/dev/null
}
act_as() {  # deployer, service account it attaches (VM login, Cloud Run revision, Cloud Build)
  gcloud iam service-accounts add-iam-policy-binding "$2" --project "$PROJECT" \
    --member "serviceAccount:$1" --role roles/iam.serviceAccountUser >/dev/null
}
trust_repo() {  # service account, owner/repo
  gcloud iam service-accounts add-iam-policy-binding "$1" --project "$PROJECT" \
    --member "principalSet://iam.googleapis.com/$POOL_NAME/attribute.repository/$2" \
    --role roles/iam.workloadIdentityUser >/dev/null
}

# basecast-airflow: SSH to the VM through IAP with sudo (OS Login), nothing else.
sa_ensure "$AIRFLOW_DEPLOY_SA" "GitHub Actions: basecast-airflow deploy"
for role in roles/compute.osAdminLogin roles/iap.tunnelResourceAccessor; do
  project_role "$AIRFLOW_DEPLOY_SA" "$role"
done
act_as "$AIRFLOW_DEPLOY_SA" "$VM_SA"  # OS Login asks for it on a VM that runs as a service account
trust_repo "$AIRFLOW_DEPLOY_SA" "$AIRFLOW_REPO"

# basecast-get-data: build from source with Cloud Build and deploy the Cloud Run service.
sa_ensure "$GET_DATA_DEPLOY_SA" "GitHub Actions: basecast-get-data deploy"
for role in roles/run.admin roles/cloudbuild.builds.editor roles/artifactregistry.writer roles/storage.admin; do
  project_role "$GET_DATA_DEPLOY_SA" "$role"
done
act_as "$GET_DATA_DEPLOY_SA" "$API_SA"
act_as "$GET_DATA_DEPLOY_SA" "$BUILD_SA"
trust_repo "$GET_DATA_DEPLOY_SA" "$GET_DATA_REPO"
# The repository `gcloud run deploy --source` pushes to; created here so the deployer needs only writer.
if ! gcloud artifacts repositories describe cloud-run-source-deploy --location "$REGION" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud artifacts repositories create cloud-run-source-deploy --location "$REGION" --project "$PROJECT" \
    --repository-format=docker --description="Cloud Run source deployments"
fi

gh_secret() {  # repo, name, value
  gh secret set "$2" --repo "$1" --body "$3" >/dev/null
}
gh_has_secret() {  # repo, name
  gh secret list --repo "$1" --json name --jq '.[].name' | grep -qx "$2"
}

for repo in "$AIRFLOW_REPO" "$GET_DATA_REPO"; do
  gh_secret "$repo" GCP_WORKLOAD_IDENTITY_PROVIDER "$POOL_NAME/providers/$PROVIDER"
  gh_secret "$repo" GCP_PROJECT_ID "$PROJECT"
done
gh_secret "$AIRFLOW_REPO" GCP_SERVICE_ACCOUNT "$AIRFLOW_DEPLOY_SA"
gh_secret "$AIRFLOW_REPO" GCP_VM_NAME "$VM_NAME"
gh_secret "$AIRFLOW_REPO" GCP_VM_ZONE "$ZONE"
gh_secret "$GET_DATA_REPO" GCP_SERVICE_ACCOUNT "$GET_DATA_DEPLOY_SA"

if ! gh_has_secret "$AIRFLOW_REPO" AIRFLOW_ENV_FILE; then
  env_file=$(gcloud compute ssh "$VM_NAME" --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
    --command "sudo cat /opt/basecast/airflow/.env" 2>/dev/null)
  grep -q '^FERNET_KEY=.' <<<"$env_file" || { echo "the VM's .env has no FERNET_KEY; not seeding" >&2; exit 1; }
  gh secret set AIRFLOW_ENV_FILE --repo "$AIRFLOW_REPO" <<<"$env_file" >/dev/null
  echo "AIRFLOW_ENV_FILE seeded from the VM's .env"
fi
if ! gh_has_secret "$GET_DATA_REPO" ENV_YAML; then
  gh secret set ENV_YAML --repo "$GET_DATA_REPO" >/dev/null <<EOF
API_TOKEN: "$(secret_get get-data-api-token)"
DATA_MODE: fixtures
GCP_PROJECT: $PROJECT
BQ_DATASET: $BQ_DATASET
EOF
  echo "ENV_YAML seeded (API_TOKEN from secret get-data-api-token)"
fi

echo "GitHub Actions ready: $AIRFLOW_DEPLOY_SA ($AIRFLOW_REPO), $GET_DATA_DEPLOY_SA ($GET_DATA_REPO)"
