#!/usr/bin/env bash
# The Airflow VM. e2-medium because Airflow asks for at least 4 GB; no port open to the internet
# (SSH only through IAP, the Airflow UI through an SSH tunnel). Idempotent.
set -euo pipefail
source "$(dirname "$0")/env.sh"

if ! gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1; then
  # The ephemeral external IP is only for outbound calls (ERCOT, NOAA...); Cloud NAT costs more.
  gcloud compute instances create "$VM_NAME" --project "$PROJECT" --zone "$ZONE" \
    --machine-type=e2-medium \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size=30GB --boot-disk-type=pd-standard \
    --service-account="$VM_SA" --scopes=cloud-platform \
    --tags="$VM_TAG" \
    --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
    --metadata=enable-oslogin=TRUE \
    --metadata-from-file=startup-script="$(dirname "$0")/vm-startup.sh"
fi
