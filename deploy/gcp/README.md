# GCP setup

Everything basecast runs on GCP, in project `basecast-509812`, region `us-central1` (the ERCOT API
blocks requests from outside the US). Each script is idempotent: re-running it fixes drift and never
duplicates anything. Every command passes `--project`, so the local gcloud default project does not matter.

| Script | Creates |
|---|---|
| `01-bootstrap.sh` | APIs, budget alert (R$ 250/month, 50/90/100%), lake bucket `gs://basecast-509812-lake`, BigQuery dataset `basecast`, firewall (SSH only from IAP) |
| `02-cloudsql.sh` | Cloud SQL `basecast-pg` (Postgres 17, db-f1-micro), databases `airflow` and `basecast`, roles, passwords in Secret Manager |
| `03-service-accounts.sh` | `airflow-vm` and `get-data-run` service accounts with least-privilege roles (BigQuery: jobs + dataset write for the VM, read for the API) |
| `04-vm.sh` | VM `basecast-airflow` (e2-medium, Debian 12, Docker via `vm-startup.sh`) |
| `05-airflow-up.sh` | Deploys the repo's committed `HEAD` and `deploy/airflow/` to the VM and (re)starts Airflow (re-run after any change) |

`02-cloudsql.sh` needs `psql` and the [Cloud SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/connect-auth-proxy)
(`cloud-sql-proxy` on PATH, or `CLOUD_SQL_PROXY=/path/to/binary`).

## Postgres

One Cloud SQL instance, two databases:

- `airflow`: Airflow's metadata, owned by role `airflow`.
- `basecast`: operational data that does not need Parquet/BigQuery. `basecast_writer` (pipelines)
  creates tables; `basecast_reader` (the API) gets `SELECT` on them automatically.

Connections go only through the Cloud SQL connectors (connector enforcement is on, no authorized
networks). db-f1-micro allows 25 connections, so every client keeps a small pool.

From the Mac:

```bash
cloud-sql-proxy --port 5439 --quota-project basecast-509812 basecast-509812:us-central1:basecast-pg
PGPASSWORD=$(gcloud secrets versions access latest --secret pg-basecast-reader-password --project basecast-509812) \
  psql -h 127.0.0.1 -p 5439 -U basecast_reader basecast
```

## Airflow

Runs with Docker Compose on the VM, in `/opt/basecast/airflow` (LocalExecutor, metadata in Cloud SQL
through a proxy sidecar). No port is open to the internet; open the UI through an SSH tunnel:

```bash
gcloud compute ssh basecast-airflow --project basecast-509812 --zone us-central1-a \
  --tunnel-through-iap -- -N -L 8080:localhost:8080
# http://localhost:8080, user admin, password:
gcloud secrets versions access latest --secret airflow-admin-password --project basecast-509812
```

The image is `apache/airflow:3.3.2-python3.12` plus the pipeline libraries in
`deploy/airflow/requirements.txt` (built on the VM). `05-airflow-up.sh` deploys the committed `HEAD`
of this repo to `/opt/basecast/src` (mounted read-only; `REVISION` holds the commit), which is the DAGs
folder and the `PYTHONPATH`, then rebuilds and restarts. The pipelines write straight to the lake
bucket (`STORAGE_ROOT=gs://basecast-509812-lake`), to Postgres as `basecast_writer` and to the
BigQuery dataset `basecast`. Pools (`deploy/airflow/config/pools.json`): `ercot_http` (1 slot: every
ercot.com fetch shares the 20 req/min budget), `open_meteo` (1), `heavy_process` (1: large parses run
alone on the 4 GB VM).

### Deploys

As in fundsys, a push to `main` deploys (`.github/workflows/deploy.yml`; also runnable by hand from the
Actions tab). The workflow signs in to GCP through Workload Identity Federation, reaches the VM over
SSH through IAP, `git reset --hard`s `/opt/basecast/src` (a clone of this public repo) to the pushed
commit, copies `deploy/airflow/` to `/opt/basecast/airflow` and writes `.env` there from the
`AIRFLOW_ENV_FILE` repo secret. Then one of three paths:

- `Dockerfile`, `requirements.txt` or `docker-compose.yml` changed: rebuild the image and `up -d`.
- only `.env` changed: `up -d --force-recreate` (compose reads `.env` only when a container starts).
- anything else: restart the scheduler and the DAG processor, because tasks fork from the scheduler
  and keep the modules it imported at start.

Each path waits for the API server's health check; only then are old images and the build cache
pruned. Rollback is `git revert` and a push. `05-airflow-up.sh` still works for the first install and
to try a branch on the VM; the next push to `main` puts `main` back.

`AIRFLOW_ENV_FILE` is the source of truth for the VM's `.env` (seed it from the current file so the
Fernet key survives). The workflow also reads `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT`,
`GCP_PROJECT_ID`, `GCP_VM_NAME` and `GCP_VM_ZONE`. The deploy service account needs OS Login with sudo
and IAP tunnel access, as `airflow-deploy` has in fundsys. The pool, the service account and these
secrets are not set up yet.

DAGs are paused when created. After a deploy, unpause them (all or one by one):

```bash
gcloud compute ssh basecast-airflow --project basecast-509812 --zone us-central1-a --tunnel-through-iap \
  --command "sudo docker exec airflow-airflow-scheduler-1 airflow dags unpause dag_ercot_gis_incremental"
```

## Secrets (Secret Manager)

`pg-postgres-password`, `pg-airflow-password`, `pg-basecast-writer-password`,
`pg-basecast-reader-password`, `airflow-admin-password`, `get-data-api-token`. The Airflow Fernet key
and JWT secret are generated on the VM and stay in `/opt/basecast/airflow/.env`, which the deploy
workflow overwrites from the `AIRFLOW_ENV_FILE` repo secret (see Deploys).

## Cost

Prices from the Cloud Billing catalog on 2026-09-26, running 24/7:

| Item | ~US$/month |
|---|---|
| VM e2-medium (US$ 0.0335/h) | 24.46 |
| VM disk, 30 GB pd-standard | 1.20 |
| VM external IPv4 (first 720 h/month per billing account free, then US$ 0.005/h) | 0 to 3.65 |
| Cloud SQL db-f1-micro (US$ 0.0105/h) | 7.67 |
| Cloud SQL 10 GB SSD + backups | ~1.75 |
| Bucket, Secret Manager, Cloud Run | ~0 (free tiers) |

Stopping saves money, but read the fine print:

```bash
gcloud compute instances stop basecast-airflow --project basecast-509812 --zone us-central1-a   # disk still billed
gcloud sql instances patch basecast-pg --project basecast-509812 --activation-policy=NEVER     # IP reservation then costs US$ 0.01/h
```

The API reads Postgres, so stopping Cloud SQL takes those endpoints down.

Upgrade paths: Cloud SQL `db-g1-small` (1.7 GB, US$ 0.035/h) if 25 connections or 614 MB get tight;
VM `e2-standard-2` (8 GB) if Airflow runs out of memory. Both are a stop, change and start.
