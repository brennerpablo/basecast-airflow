"""Google Cloud clients. Credentials come from Application Default Credentials (the VM's service account,
or ``gcloud auth application-default login`` on a laptop); on a laptop the quota goes to the basecast
project, never to whatever project the local gcloud defaults to."""

from __future__ import annotations

from functools import lru_cache

import google.auth

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def credentials(project: str | None):
    """ADC with the basecast project as quota project only for user credentials (gcloud on a laptop, whose
    default quota project may be another one). A service account (the VM) already bills its own project,
    and a quota project there would need ``serviceusage.services.use``."""
    from google.oauth2.credentials import Credentials as UserCredentials

    creds, default_project = google.auth.default(scopes=SCOPES)
    if project and isinstance(creds, UserCredentials):
        creds = creds.with_quota_project(project)
    return creds, project or default_project


_credentials = credentials


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
