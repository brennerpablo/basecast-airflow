# basecast-airflow

Data ingestion, processing and orchestration for **basecast**, which forecasts how much of ERCOT's
interconnection queues (large loads and generation) actually gets built, where and when, and turns that
into peak-demand forecasts by region.

Every source goes through two stages:

1. **raw:** discover and download public ERCOT, PUCT, Census, EIA, BLS, TCEQ, Comptroller and weather
   files into an immutable lake (`raw/source=<id>/dt=<date>/`, one `_manifest.json` per folder).
2. **process:** parse them into typed tables. Postgres (`basecast` database, PostGIS on) for the datasets
   the API reads; typed Parquet in the lake plus BigQuery for the large series.

Airflow runs both stages on a schedule on a GCP VM: one thin DAG per source, built by a factory.

## Repos

| Repo | Role |
|---|---|
| `basecast-airflow` | Ingestion, processing and orchestration (this repo) |
| `basecast-get-data` | FastAPI service; owns the data contract |
| `basecast-app` | Next.js frontend |

## Setup

```bash
uv sync                # Python 3.12 + pinned dependencies
cp .env.example .env   # optional; never commit .env
```

| Variable | Default | What |
|---|---|---|
| `STORAGE_ROOT` | `file://./data` | Lake root: `file://...` locally, `gs://basecast-509812-lake` on the VM |
| `BASECAST_DB_URL` | unset | Postgres `basecast` database (processed tables, `etl_run`, `lake_processed`); needed to process |
| `GCP_PROJECT` | unset | Needed for the BigQuery datasets and Gemini |
| `BQ_DATASET` / `BQ_LOCATION` | `basecast` / `us-central1` | BigQuery destination |
| `GEMINI_MODEL` | `gemini-3.8-flash` | Chart values and image-only pages of documents (Vertex AI, location `global`) |
| `BASECAST_USER_AGENT` | project UA | HTTP User-Agent |

No credentials are needed for the raw stage.

## Usage

```bash
uv run basecast sources                          # source ids, in runbook order
uv run basecast run ercot_gis --dry-run          # list what would be downloaded
uv run basecast run ercot_gis                    # download new/changed files into the lake
uv run basecast datasets                         # every dataset, its target and write mode
uv run basecast process ercot_gis --dry-run      # parse and print schema and a sample; write nothing
uv run basecast process ercot_gis                # parse new raw files into the tables
uv run basecast process ercot_gis --reprocess    # re-parse everything (add --rebuild after schema changes)
uv run basecast runs                             # recent etl_run rows (local Parquet)
uv run basecast audit                            # raw integrity: manifests, sha256, file signatures
uv run pytest                                    # tests (offline, small real fixtures)
```

`docs/processing.md` explains the parser contract, write modes and rules.

## Marts

The marts are the small typed tables the API serves (`public.mart_*`, plus `mart_meta`), built from the processed
tables by `basecast_pipelines/marts/` (plan and golden checks in `docs/build/BUILD_A_airflow_marts.md`; switches in
`config/marts.yaml`). They need the `analysis` group (numpy, rapidfuzz):

```bash
uv run --group analysis basecast marts list                               # marts in build order
uv run --group analysis basecast marts build --all --as-of 2026-09-26 --dry-run   # Parquet to data/marts_dry/
uv run --group analysis basecast marts build --all                        # write Postgres (BASECAST_DB_URL, writer)
uv run --group analysis basecast marts check                              # re-run the checks on what is in Postgres
uv run --group analysis basecast export-geo                               # county / weather-zone GeoJSON for the app
```

A real build writes only from committed code: run it from a clean checkout of HEAD with `BASECAST_GIT_SHA` set
(or `--allow-dirty` for a scratch database). A mart whose golden check fails is not written; the previous version
keeps being served and the `etl_run` row (source `marts`, stage `model`) ends `partial`.

## Airflow

- `dags/dag_<source>_incremental.py`: one per source, scheduled (cadences in
  `dags/basecast_dags/config.py`), `fetch_raw` then `process`.
- `dag_config_facts_incremental`: loads the YAML files in `config/`.
- `dag_source_full`: manual backfill or reprocessing (params: source, stages, window, options).

Deployment (VM, Cloud SQL, bucket, BigQuery, pools) is in `deploy/gcp/README.md`. The DagBag test needs
Airflow installed (it skips otherwise); see `tests/test_dags.py`.

## Docs

- `docs/KICKOFF.md`: project kickoff (Portuguese)
- `docs/SCRAPING_RUNBOOK.md`: what to download, from where, and what each source produces
- `docs/processing.md`: raw → typed tables
- `docs/decisions.md`: decision log
- `catalog.yaml` and `docs/ercot-data-catalog.md`: source catalog with verification status
- `CLAUDE.md`: working context for Claude Code
