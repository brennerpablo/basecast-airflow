#!/usr/bin/env bash
# Deploys the committed HEAD of this repo to the VM (/opt/basecast/src: DAGs, pipelines, config/), then
# copies deploy/airflow there and (re)starts Airflow. Safe to re-run after any change.
# Uncommitted changes are not deployed: commit first.
set -euo pipefail
source "$(dirname "$0")/env.sh"
ssh_opts=(--project "$PROJECT" --zone "$ZONE" --tunnel-through-iap)
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
revision=$(git -C "$repo" rev-parse --short HEAD)
if [ -n "$(git -C "$repo" status --porcelain --untracked-files=no)" ]; then
  echo "warning: uncommitted changes are not deployed (deploying $revision)" >&2
fi

archive=$(mktemp -d)/basecast-src.tar.gz
git -C "$repo" archive --format=tar.gz -o "$archive" HEAD

gcloud compute ssh "$VM_NAME" "${ssh_opts[@]}" --command "rm -rf /tmp/airflow-deploy /tmp/basecast-src.tar.gz"
gcloud compute scp "$archive" "$VM_NAME":/tmp/basecast-src.tar.gz "${ssh_opts[@]}"
gcloud compute scp --recurse "$repo/deploy/airflow" "$VM_NAME":/tmp/airflow-deploy "${ssh_opts[@]}"
# rsync into place (not a directory swap): the running containers keep their bind mount.
gcloud compute ssh "$VM_NAME" "${ssh_opts[@]}" --command "sudo bash -c '
  set -euo pipefail
  rm -rf /tmp/basecast-src && mkdir -p /tmp/basecast-src /opt/basecast/src
  tar -xzf /tmp/basecast-src.tar.gz -C /tmp/basecast-src
  echo $revision > /tmp/basecast-src/REVISION
  rsync -a --delete /tmp/basecast-src/ /opt/basecast/src/
  chown -R 50000:0 /opt/basecast/src
  SQL_CONNECTION=$SQL_CONNECTION bash /tmp/airflow-deploy/up.sh
'"
echo "deployed $revision"
