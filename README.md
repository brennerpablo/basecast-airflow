# basecast-airflow

Data ingestion and models for **basecast**, which forecasts how much of ERCOT's interconnection queues
(large loads and generation) actually gets built, where and when, and turns that into peak-demand
forecasts by region.

This repo pulls public ERCOT, Census, EIA and weather data into a local lake: immutable raw files plus
typed Parquet. Each source is a plain Python module with a `run()` entry point; Airflow DAGs will be thin
wrappers added later.

> **Status:** raw ingestion. Every source in `docs/SCRAPING_RUNBOOK.md` downloads into `data/raw/`;
> parsers into typed Parquet come next.

## Repos

| Repo | Role |
|---|---|
| `basecast-airflow` | Ingestion and models (this repo) |
| `basecast-get-data` | FastAPI service; owns the data contract |
| `basecast-app` | Next.js frontend |

## Setup

```bash
uv sync                # Python 3.12 + pinned dependencies
cp .env.example .env   # optional; never commit .env
```

No credentials are needed for the raw backfill: ERCOT files come from the public product pages and their
document listings, Census and EIA from their public file servers. `STORAGE_ROOT` (default
`file://./data`) sets the lake root; `BASECAST_USER_AGENT` overrides the default User-Agent.

## Usage

```bash
uv run basecast sources                          # source ids, in runbook order
uv run basecast run ercot_gis --dry-run          # list what would be downloaded
uv run basecast run ercot_gis                    # download new/changed files into data/raw/
uv run basecast run ercot_ltlf --opt include_weather_scenarios=true
uv run basecast runs                             # recent etl_run rows
uv run basecast audit                            # raw integrity: manifests, sha256, file signatures
uv run basecast inventory                        # write docs/data-inventory.md
uv run pytest                                    # tests (offline, small real fixtures)
```

Lake layout (identical to the future GCS bucket):

```
data/
├── raw/source=<id>/dt=<YYYY-MM-DD>/<original file>   # immutable, one _manifest.json per dt folder
├── _runs/etl_run.parquet                             # one row per run: status, counts, events
└── _logs/                                            # run logs (local only)
```

Re-running a source is idempotent: known ERCOT document ids are skipped, other URLs are re-checked with
conditional requests, and identical content (same sha256) is never stored twice. See `docs/decisions.md`.

## Docs

- `docs/KICKOFF.md`: project kickoff (Portuguese)
- `docs/SCRAPING_RUNBOOK.md`: what to download, from where, and what each source produces
- `catalog.yaml` and `docs/ercot-data-catalog.md`: source catalog with verification status
- `CLAUDE.md`: working context for Claude Code
