# CLAUDE.md — basecast-airflow

**basecast** was built for the Base Power × AITX Hackathon (Austin, Sep 25–27, 2026). This repo is
**front A: data mining**. It downloads public sources into the lake (`gs://basecast-509812-lake`, same
layout as the local `data/`), parses them into typed tables (Postgres `basecast`; Parquet + BigQuery for the
large series) and runs both stages from thin Airflow DAGs on a GCP VM. Models come later.

**Since 2026-09-29 the services are gone.** To bring the GCP bill to zero, project `basecast-509812` was deleted
(Cloud Run, Cloud SQL, the Airflow VM, the lake's bucket, BigQuery) and the deploy workflows of this repo and its
sibling are disabled, so a push to `main` no longer deploys. basecast-app is a static public demo that answers
from a recorded snapshot of get-data. Local backups of the bucket and both databases are in
`~/Documents/repos/basecast/backups/2026-09-29/` (see its README). Read what follows about GCP as history.

This file carries the stable parts of `docs/KICKOFF.md` (in Portuguese): sections 1, 2 and 7, this repo's
part of section 3, the working rules from section 0 and the front A principles from section 4. The tasks
and their done criteria (A0–A9) stay only in `docs/KICKOFF.md` §4. What to download, from where and in
which order is in `docs/SCRAPING_RUNBOOK.md`.

## Working agreements

- Code, comments, README and names in English. Talk to the user in Portuguese.
- Plan each piece of work and show the plan to the user; implement only after approval.
- Never use "Novi" in code, packages or branding: Novi Labs is a real Austin company, and
  "Novi for Energy" is only the pitch analogy.
- ERCOT file formats: read headers dynamically, never assume column names. Anything not confirmed stays
  marked "not verified".
- Log decisions in `docs/decisions.md`, one line each: date, decision, reason. Decisions that affect more
  than one repo go to `basecast-get-data`, which owns the contract.
- Work on `main` only, in all three repos: no feature branches, no worktrees. Small commits pushed straight
  to `main`. A push to `main` deploys to the Airflow VM (`.github/workflows/deploy.yml`), so the checks pass
  before every push: `uv run pytest`, plus `tests/test_dags.py` with Airflow installed.

## Product context (KICKOFF §1)

Base Power is an Austin energy company. It installs batteries in homes (and keeps owning them), sells
retail power and runs the fleet as a virtual power plant. It makes money from three sources: homeowners,
the ERCOT wholesale market, and utilities (co-ops and munis that buy capacity).

**Problem:** Texas plans its grid around inflated interconnection queues. In Jan 2026 ERCOT was tracking
~232.5 GW of large loads, only 3.8% of them approved to energize. The demand record was 85,508 MW
(2023-08-10) until 2026-07-22, when the peak reached 91.1 GW (preliminary). ERCOT's own preliminary
long-term forecast for 2026 (~112 GW) missed that same year's peak by about 21 GW.

**Product:** forecast how much of the queues (large loads and generation) actually gets built, where and
when; turn that into a **peak MW** forecast by region and year (P10/P50/P90); and translate it into
decisions for Base, mainly for the partnerships team: which co-ops to approach, when, and with what offer.

**Modules:**
1. **Explorer:** Texas map by county; raw vs. adjusted queue; priority acquisition zones.
2. **Forecast:** per-project survival in the generation queue; aggregate flow by stage for large loads;
   weather-normalized load time series; backtest.
3. **Commercial intelligence (core of the demo):** prioritized accounts, triggers, per-co-op diagnosis,
   rule-based next action. Read-only; CSV/webhook export.
4. **Private data adapters:** `FleetDataSource` (Base's fleet, simulated) and `UtilityDataSource`
   (large-load requests the co-op itself received). Public data gives a zone-level view; private data
   takes the diagnosis down to the territory.

**Judging:** 5-minute video + code. Completeness without crashes, technical depth, track fit (Open Grid
Data is the main track), non-obvious insight, usability, performance.

## Architecture decisions, closed (KICKOFF §2)

- **Three repos, same design as Fundsys:** `basecast-airflow` (ingestion and models), `basecast-get-data`
  (API) and `basecast-app` (frontend). No monorepo.
- **GCP project `basecast-509812`, `us-central1`** (the ERCOT API blocks access from outside the US):
  Airflow VM, Cloud SQL, the lake bucket and BigQuery; setup in `deploy/gcp/`. The Mac mini still runs
  backfills and dry runs against the same lake.
- **Lake:** immutable raw `raw/source=<id>/dt=<snapshot date>/<original file>` plus typed Parquet
  `parquet/<dataset>/dt=<date>/part-*.parquet`. The local layout is identical to the GCS bucket's, so
  moving up is `gcloud storage rsync` plus a BigQuery load, with no code rewrite.
- **Warehouse:** Postgres (Cloud SQL, database `basecast`, PostGIS) for what the API reads; BigQuery
  (`basecast` dataset, tables partitioned by MONTH because of the 4,000-partition limit) for the large
  series, whose Parquet in the lake is the source of truth. Both reverse the kickoff's "BigQuery by day,
  no Cloud SQL" (see `docs/decisions.md`).
- **Pipelines (`basecast-airflow`):** each source is a pure Python module with
  `run(*, storage, http, since=None, until=None)` that runs on its own from the CLI. The Airflow DAGs
  (later, on a VM with Docker Compose and LocalExecutor) will be thin and only call these `run()`
  functions. Patterns inherited from Fundsys: a `full` / `incremental` DAG factory, `etl_run` (one row per
  run with status, duration and events), idempotency (reprocessing never duplicates).
- **API (`basecast-get-data`):** FastAPI, later on Cloud Run. Same service name as Fundsys's, but
  **one typed endpoint per resource** (no dispatch by `process`), Pydantic, and an OpenAPI spec that
  generates the app's TypeScript client. Small marts loaded in memory (Polars) so the sliders respond in
  milliseconds.
- **Frontend (`basecast-app`):** Next.js on Vercel, built on the Fundsys base. The browser only talks to
  the BFF (route handlers); the API token stays on the server. TanStack Query. MapLibre for the map.
- **Time:** store everything in UTC; keep the sources' local hour and DST flag (ERCOT repeats an hour in
  November and uses "hour ending"); crons and display in `America/Chicago`.
- **Out of the MVP:** ancillary service prices (the post-RTC+B series is under a year old), 60-day
  disclosures, outages, short-term price forecasting, multi-tenancy, user writes.

## Repo layout (KICKOFF §3)

```
basecast-airflow/
├── CLAUDE.md
├── README.md
├── .env.example              # never commit .env
├── pyproject.toml            # Python 3.12, uv
├── catalog.yaml              # extracted from the ERCOT catalog (task A0)
├── config/                   # weather_points.yaml, manual_official_figures.yaml (see SCRAPING_RUNBOOK)
├── data/                     # local lake (gitignored): raw/, parquet/, _runs/
├── basecast_pipelines/
│   ├── common/               # storage (local|gcs), http client, manifest, etl_run, schemas
│   ├── sources/              # one module per source, each with run()
│   │   ├── ercot/            # gis.py, load_archive.py, ltlf.py, cdr.py, large_load.py, ...
│   │   ├── census/           # permits.py, housing.py, geo.py
│   │   ├── weather/          # open_meteo.py
│   │   └── territories/      # eia_atlas.py
│   ├── processing/           # parser contract (Dataset, SqlDataset), runner, tabular helpers
│   ├── parsers/              # one module per source, same path as sources/ (docs/processing.md)
│   ├── adapters/             # fleet_data_source.py, utility_data_source.py (interfaces + simulated)
│   ├── marts/                # core.py (Mart, build, checks, write), one module per screen group, catalog.py
│   └── cli.py                # `basecast run|process|datasets|runs|audit|inventory|marts|export-geo`
├── dags/                     # dag_<source>_incremental.py (thin), dag_source_full.py, basecast_dags/ (factory, cadences)
├── deploy/                   # gcp/ (setup scripts, 05 deploys HEAD to the VM), airflow/ (compose, image, pools)
├── tests/                    # pytest with small fixtures (trimmed real samples)
└── docs/
    ├── KICKOFF.md
    ├── SCRAPING_RUNBOOK.md
    ├── ercot-data-catalog.md
    ├── data-inventory.md     # generated by `basecast inventory`
    └── decisions.md
```

Suggested dependencies (pin versions): `polars`, `pyarrow`, `httpx`, `tenacity`, `pydantic`, `openpyxl`,
`pyxlsb`, `xlrd`, `pdfplumber`, `geopandas`, `shapely`, `python-dotenv`, `typer`, `pytest`. `gridstatus`
as support (pin the version; its methods vary between versions).

## Front A principles (KICKOFF §4)

- **Raw is immutable.** Never overwrite. Every download writes a `_manifest.json` next to it: URL,
  document id / Report Type ID, `fetched_at` (UTC), sha256, bytes, HTTP status. Running again with the
  same sha256 means skip (idempotency).
- **Typed Parquet:** numbers as numbers, dates as dates, timestamps in UTC, plus `source_file` and
  `ingested_at` columns. Never store numbers as text (a known Fundsys pain).
- **Polite HTTP:** one shared client with a token bucket (the ERCOT API allows 30 req/min; use at most
  ~20), exponential backoff with jitter, retry on 429/5xx, token renewal without assuming a fixed 1h, an
  identifiable User-Agent. If requests keep failing, **stop and tell the user**: ERCOT has suspended keys
  for high failure rates.
- **Credentials** in `.env`: `ERCOT_API_USERNAME`, `ERCOT_API_PASSWORD`, `ERCOT_SUBSCRIPTION_KEY` (free
  signup on ERCOT's developer portal). Many files also download straight from ercot.com without login:
  prefer that path when it exists.
- **Discover links on the pages the runbook points to; never construct URLs.** Raw source ids (the
  `raw/source=<id>/` prefix) come from the table in `docs/SCRAPING_RUNBOOK.md` §1.
- **Local etl_run:** every run writes one row to `data/_runs/etl_run.parquet` (source, started_at,
  finished_at, status, rows, files, error, events).
- **ops.log:** with `BASECAST_DB_URL` set, `common/ops_log.py` also writes each run's start, end and events
  (`etl_run.*`, `etl.<kind>`, with `run_id`) and every WARNING+ of the `basecast_pipelines`/`basecast_dags`
  loggers to Postgres `ops.log`, the log the app's /ops screen reads (format: basecast-get-data data contract
  §7). Batched on a thread, flushed when a run ends, never raises; `install()` is called by the CLI and the
  DAG runtime.
- Downloads above ~1 GB in total for one source: ask the user first.

## General rules (KICKOFF §7)

- Plan before coding; small, descriptive commits; never commit `data/` or `.env`.
- Tests use small fixtures trimmed from real files, especially for the parsers.
- Label as **simulated** everything that comes from the private-data adapters and from fixtures.
- Don't invent URLs, IDs or columns; confirm them at the source or mark them "not verified".
- Contract changed? Update `data-contract.md`, the Pydantic models and `openapi.json` together, and
  regenerate the client in the app.
- Out of scope for now: models, Fleet API mock, commercial-module logic, real mart reads in the API.

## Sibling repos

All three live in `~/Documents/repos/basecast/`:

- `basecast-airflow` (this repo, front A): produces the data. The county TopoJSON from task A1 goes to
  `basecast-app/public/geo/`.
- `basecast-get-data` (front C): serves the data; owns `docs/data-contract.md` and the cross-repo
  decisions.
- `basecast-app` (front B): Next.js frontend; consumes the API through its BFF.

Reference only, never copy Fundsys data: `~/Documents/repos/fundsys/fundsys-airflow` has the `etl_run`
and full/incremental DAG patterns. When DAGs arrive, start each DAG file's docstring with
`"""Airflow DAG: ..."""`: Airflow 3's safe-mode discovery silently skips files that never mention both
"airflow" and "dag", which is exactly what thin DAG files look like.

## Docs

- `docs/KICKOFF.md`: the full kickoff, including tasks A0–A9 and the open questions (§8).
- `docs/SCRAPING_RUNBOOK.md`: per-source runbook, execution order and tracking table.
- `catalog.yaml` and `docs/ercot-data-catalog.md`: the source catalog. Check each entry's `verified` field
  before trusting a URL, ID or column.
