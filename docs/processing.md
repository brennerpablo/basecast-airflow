# Processing: raw → typed tables

Every source goes through two stages, each recorded in `etl_run`:

1. **raw** (`basecast run <source>`): discover and download into `raw/source=<id>/dt=<date>/`, immutable,
   with a `_manifest.json` per folder.
2. **process** (`basecast process <source>`): parse the raw files into typed tables.

Where the tables go:

| Target | For | Read by |
|---|---|---|
| Postgres (`basecast` database, PostGIS on) | everything small or medium | the API (`basecast_reader`) |
| Parquet in the lake (`parquet/<dataset>/dt=<date>/part-*.parquet`) + BigQuery (`basecast` dataset) | the large datasets only | BigQuery |

## Writing a parser

A source's parser lives at the path of its raw module with `sources` replaced by `parsers`
(`sources/ercot/gis.py` → `parsers/ercot/gis.py`). The module declares `DATASETS`, and optionally
`SQL_DATASETS`. See `parsers/ercot/ziptozone.py` for a complete example.

```python
DATASETS = [
    Dataset(
        name="ercot_zip_weather_zone",   # table name (Postgres or BigQuery)
        target="postgres",               # or "bigquery" (large datasets only)
        mode="replace",                  # replace | by_file | by_key
        description="...",
        parse=parse_zip_zone,            # RawFile -> DataFrame | None, one file at a time
        inputs=lambda f: f.suffix in {".xlsx", ".xls"},   # which raw files feed it
        select=latest_dt,                # optional: pick among the inputs (latest_dt, latest_by_url, ...)
    ),
]
```

- **`parse`** reads one `RawFile` and returns a DataFrame (or `None` for a file that has nothing for this
  dataset). Use `f.local_path()` for readers that need a path, `f.read_bytes()` otherwise; `f.meta`
  holds what discovery recorded (family, friendly name, publish date...). **`build`** instead receives
  every selected file at once; it only works with `mode="replace"`.
- The runner adds `source_file` (the raw key) and `ingested_at` (UTC). Do not add them yourself unless a
  `build` combines files (then set `source_file` per row).
- `finalize` (replace mode) runs once on the concatenated frame, e.g. to deduplicate across files.
- `version`: bump it when the parser's output changes; every file is reprocessed on the next run.

### Write modes (idempotency)

| Mode | Scope replaced on each write | Use for |
|---|---|---|
| `replace` | the whole table | reference data; "latest snapshot wins" |
| `by_file` | rows with the same `source_file` | report vintages, snapshots, documents |
| `by_key` | rows with the same `key` (never null) | time series whose files overlap (current-year files overwritten by the source) |

`lake_processed` records (dataset, raw key, sha256, version), so scheduled runs parse only new raw files.
`--reprocess` re-parses everything selected; `--rebuild` also drops the tables first (schema changes).

- `by_file` prunes: when a run sees every raw file (no `--since`/`--until`/`--max-files`), rows of files
  the dataset no longer selects (a corrected report, a newer copy of the same URL) are deleted.
- `replace` rebuilds the table when the columns change (added, removed or retyped).
- Two sources feeding one table: give each source its own table and combine them in a SQL dataset
  declared by both modules (see `ercot_load_hourly_wz`), so neither can overwrite or drop the other's rows.
- Identifiers longer than 63 bytes are refused (Postgres would silently truncate them).

### BigQuery datasets

Use `by_file` or `by_key` and set `partition=("<date or timestamp column>", "MONTH")` (a table may have at
most 4,000 partitions, so long hourly series cannot be partitioned by day) and `cluster=(...)` on the
main filter columns.

### SQL datasets

```python
SQL_DATASETS = [
    SqlDataset(name="county_weather_zone", sql="SELECT ... FROM census_zcta_county JOIN ...",
               description="...", indexes=(("county_fips",),)),
]
```

Rebuilt after the module's datasets on every non-dry run, swapped in atomically. If an input table does
not exist yet, the dataset is skipped with a warning.

### PostGIS

Return geometries as GeoJSON text and declare them: `geometry={"geom": 4326}` (the SRID of the input
coordinates; the column is stored as `geometry(Geometry, 4326)`, transformed if needed, with a GiST index).

## Rules

- **Read headers dynamically** (`processing/tabular.py`: `read_grid`, `find_header_row`, `with_header`).
  Never assume a column position; layouts change between vintages. Raise when a file that should parse
  does not; return `None` only for files that are known to be irrelevant, and say why in a comment.
- **Types:** numbers as numbers (`num()`, `integer()`), dates as `Date` (`excel_date()`), timestamps as
  `Datetime("us", "UTC")`. Keep the source's local hour and DST flag where the source is in local time
  (ERCOT uses "hour ending", writes `24:00`, and repeats an hour in November).
- **Names:** snake_case columns. Keys follow the data contract: `county_fips` 5-char string,
  `weather_zone` in `COAST, EAST, FWEST, NORTH, NCENT, SOUTH, SCENT, WEST`, `load_zone` like `LZ_NORTH`,
  `utility_id` (EIA id) as string, `inr` as string.
- **No personal data:** drop names, emails and phone numbers of individuals at parse (see
  `docs/decisions.md`).
- **No nested columns:** encode lists and structs as JSON text.
- **Numbers read from PDFs or charts** carry `extraction_method` and `verified` (false until a human
  checks them).
- **Memory:** the Airflow VM leaves ~2.5 GB to tasks. Parse per file; use `build` when the inputs are
  small, or when collecting pages one by one keeps memory lower than concatenating parsed files.

## Checking a parser

```bash
uv run basecast process <source> --dry-run [--max-files 5]   # parse, print schema and a sample
BASECAST_DB_URL=postgresql://postgres:dev@127.0.0.1:55432/basecast uv run basecast process <source>
```

The second line writes to a local PostGIS (`docker run -p 55432:5432 -e POSTGRES_PASSWORD=dev
-e POSTGRES_DB=basecast imresamu/postgis:17-3.5`, then `CREATE EXTENSION postgis`). Tests use small
fixtures trimmed from real files (`tests/fixtures/<source_id>/`, helper `raw_file` in
`tests/parsers/conftest.py`).
