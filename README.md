# basecast-airflow

Data ingestion and models for **basecast**, which forecasts how much of ERCOT's interconnection queues
(large loads and generation) actually gets built, where and when, and turns that into peak-demand
forecasts by region.

This repo pulls public ERCOT, Census, EIA and weather data into a local lake: immutable raw files plus
typed Parquet. Each source is a plain Python module with a `run()` entry point; Airflow DAGs will be thin
wrappers added later.

> **Status:** bootstrap. Nothing runs yet; the foundation (task A0) comes next.

## Repos

| Repo | Role |
|---|---|
| `basecast-airflow` | Ingestion and models (this repo) |
| `basecast-get-data` | FastAPI service; owns the data contract |
| `basecast-app` | Next.js frontend |

## Setup

```bash
cp .env.example .env   # ERCOT Public API credentials; never commit .env
```

## Docs

- `docs/KICKOFF.md`: project kickoff (Portuguese)
- `docs/SCRAPING_RUNBOOK.md`: what to download, from where, and what each source produces
- `catalog.yaml` and `docs/ercot-data-catalog.md`: source catalog with verification status
- `CLAUDE.md`: working context for Claude Code
