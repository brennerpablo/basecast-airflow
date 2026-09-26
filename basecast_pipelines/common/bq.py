"""BigQuery loads, for the large datasets only. Their typed Parquet in the lake
(``parquet/<dataset>/dt=<date>/part-*.parquet``) is the source of truth; BigQuery gets a copy through load
jobs (free), and a multi-statement transaction replaces the write's scope, so re-running never duplicates."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from uuid import uuid4

from basecast_pipelines.common.db import WriteMode

log = logging.getLogger(__name__)

STAGE_EXPIRATION_MS = 24 * 3600 * 1000


def _job_config(*, write: str, partition: tuple[str, str] | None, cluster: Sequence[str]):
    from google.cloud import bigquery

    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, write_disposition=write)
    if partition:
        config.time_partitioning = bigquery.TimePartitioning(type_=partition[1], field=partition[0])
    if cluster:
        config.clustering_fields = list(cluster)
    return config


def _load(client, sources: Sequence[str], table: str, config) -> None:
    """Load Parquet from ``gs://`` URIs in one job, or from local paths one file at a time."""
    from google.cloud import bigquery

    if all(s.startswith("gs://") for s in sources):
        client.load_table_from_uri(list(sources), table, job_config=config).result()
        return
    for i, path in enumerate(sources):
        if i:
            config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        with open(path.removeprefix("file://"), "rb") as fh:
            client.load_table_from_file(fh, table, job_config=config).result()


def ensure_dataset(client, dataset: str, location: str) -> None:
    from google.cloud import bigquery

    ref = bigquery.Dataset(f"{client.project}.{dataset}")
    ref.location = location
    client.create_dataset(ref, exists_ok=True)


def load_parquet(
    client,
    *,
    dataset: str,
    table: str,
    sources: Sequence[str],
    mode: WriteMode,
    key: Sequence[str] = (),
    partition: tuple[str, str] | None = None,
    cluster: Sequence[str] = (),
    recreate: bool = False,
) -> None:
    """Replace the write's scope in ``dataset.table`` with the rows of ``sources`` (Parquet URIs or paths).

    ``replace`` truncates the table in the load job itself; ``by_file`` and ``by_key`` load into a staging
    table and swap the scope (same ``source_file`` values, or same key) inside one transaction."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    target = f"{client.project}.{dataset}.{table}"
    if recreate:
        client.delete_table(target, not_found_ok=True)
    if mode == "replace":
        config = _job_config(write=bigquery.WriteDisposition.WRITE_TRUNCATE, partition=partition, cluster=cluster)
        _load(client, sources, target, config)
        log.info("%s: replaced from %d files", target, len(sources))
        return

    stage = f"{client.project}.{dataset}._stage_{table}_{uuid4().hex[:8]}"
    _load(client, sources, stage, _job_config(write=bigquery.WriteDisposition.WRITE_TRUNCATE, partition=None, cluster=()))
    try:
        stage_table = client.get_table(stage)
        stage_table.expires = None
        try:
            current = client.get_table(target)
        except NotFound:
            new = bigquery.Table(target, schema=stage_table.schema)
            if partition:
                new.time_partitioning = bigquery.TimePartitioning(type_=partition[1], field=partition[0])
            if cluster:
                new.clustering_fields = list(cluster)
            current = client.create_table(new)
        known = {f.name for f in current.schema}
        added = [f for f in stage_table.schema if f.name not in known]
        if added:
            current.schema = [*current.schema, *added]
            client.update_table(current, ["schema"])
        columns = ", ".join(f"`{f.name}`" for f in stage_table.schema)
        if mode == "by_file":
            scope = f"DELETE FROM `{target}` WHERE source_file IN (SELECT DISTINCT source_file FROM `{stage}`);"
        else:
            if not key:
                raise ValueError(f"{table}: by_key needs key columns")
            match = " AND ".join(f"t.`{c}` = s.`{c}`" for c in key)
            scope = f"DELETE FROM `{target}` t WHERE EXISTS (SELECT 1 FROM `{stage}` s WHERE {match});"
        client.query(
            f"""
            BEGIN TRANSACTION;
            {scope}
            INSERT INTO `{target}` ({columns}) SELECT {columns} FROM `{stage}`;
            COMMIT TRANSACTION;
            """
        ).result()
        log.info("%s: %s from %d files", target, mode, len(sources))
    finally:
        client.delete_table(stage, not_found_ok=True)
