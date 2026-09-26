# GCP setup

Everything basecast runs on GCP, in project `basecast-509812`, region `us-central1` (the ERCOT API
blocks requests from outside the US). Each script is idempotent: re-running it fixes drift and never
duplicates anything. Every command passes `--project`, so the local gcloud default project does not matter.

| Script | Creates |
|---|---|
| `01-bootstrap.sh` | APIs, budget alert (R$ 250/month, 50/90/100%), lake bucket `gs://basecast-509812-lake`, firewall (SSH only from IAP) |
| `02-cloudsql.sh` | Cloud SQL `basecast-pg` (Postgres 17, db-f1-micro), databases `airflow` and `basecast`, roles, passwords in Secret Manager |
| `03-service-accounts.sh` | `airflow-vm` and `get-data-run` service accounts with least-privilege roles |
| `04-vm.sh` | VM `basecast-airflow` (e2-medium, Debian 12, Docker via `vm-startup.sh`) |
| `05-airflow-up.sh` | Copies `deploy/airflow/` to the VM and starts Airflow (re-run after any change there) |

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

The local lake lives in `/data` on the VM (same layout as the bucket) until the pipelines write to
GCS directly.

## Secrets (Secret Manager)

`pg-postgres-password`, `pg-airflow-password`, `pg-basecast-writer-password`,
`pg-basecast-reader-password`, `airflow-admin-password`, `get-data-api-token`. The Airflow Fernet key
and JWT secret are generated on the VM and stay in `/opt/basecast/airflow/.env`.

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
