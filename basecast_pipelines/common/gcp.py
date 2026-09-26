"""Google Cloud clients. Credentials come from Application Default Credentials (the VM's service account,
or ``gcloud auth application-default login`` on a laptop); quota is always charged to the basecast
project, never to whatever project the local gcloud defaults to."""

from __future__ import annotations

from functools import lru_cache

import google.auth

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def _credentials(project: str | None):
    credentials, default_project = google.auth.default(scopes=SCOPES, quota_project_id=project)
    return credentials, project or default_project


@lru_cache(maxsize=4)
def storage_client(project: str | None = None):
    from google.cloud import storage

    from basecast_pipelines.config import load_settings

    project = project or load_settings().gcp_project
    credentials, project = _credentials(project)
    return storage.Client(project=project, credentials=credentials)


@lru_cache(maxsize=4)
def bigquery_client(project: str, location: str):
    from google.cloud import bigquery

    credentials, project = _credentials(project)
    return bigquery.Client(project=project, credentials=credentials, location=location)
