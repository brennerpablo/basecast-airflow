"""``basecast`` command line: run a source, list sources, write the data inventory."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.inventory import render_markdown, runs_table
from basecast_pipelines.common.raw import register_local_file
from basecast_pipelines.common.storage import storage_from_uri
from basecast_pipelines.common.validate import audit_raw
from basecast_pipelines.config import PROJECT_ROOT, load_settings, local_today
from basecast_pipelines.sources import SOURCE_MODULES, UnknownSourceError, describe, get_source

app = typer.Typer(no_args_is_help=True, help="basecast data pipelines (raw ingestion).")


def _parse_option(raw: str) -> tuple[str, object]:
    key, sep, value = raw.partition("=")
    if not sep:
        raise typer.BadParameter(f"expected key=value, got {raw!r}")
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return key, lowered == "true"
    if value.lstrip("-").isdigit():
        return key, int(value)
    return key, value


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


@app.command()
def sources() -> None:
    """List the source ids in runbook order."""
    for source_id in SOURCE_MODULES:
        typer.echo(f"{source_id:26} {describe(source_id)[:110]}")


@app.command()
def run(
    source: Annotated[str, typer.Argument(help="Source id (see `basecast sources`).")],
    since: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Only dt >= this date.")] = None,
    until: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Only dt <= this date.")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="List what would be downloaded; write nothing.")] = False,
    opt: Annotated[list[str] | None, typer.Option("--opt", help="Source option key=value (repeatable).")] = None,
) -> None:
    """Discover and download a source's raw files into the lake."""
    try:
        module = get_source(source)
    except UnknownSourceError:
        raise typer.BadParameter(f"unknown source {source!r}; see `basecast sources`") from None
    settings = load_settings()
    storage = storage_from_uri(settings.storage_root)
    options = dict(_parse_option(o) for o in opt or [])
    with HttpClient(user_agent=settings.user_agent) as http:
        try:
            files = module.run(
                storage=storage,
                http=http,
                since=since.date() if since else None,
                until=until.date() if until else None,
                dry_run=dry_run,
                **options,
            )
        except Exception as exc:
            typer.secho(f"{source} failed: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1) from exc
    if dry_run:
        for f in files:
            typer.echo(f"{f.dt}  {f.filename or f.url.rsplit('/', 1)[-1]}  <- {f.url}")
        typer.echo(f"{len(files)} files would be considered (already-fetched ones are skipped at run time)")


@app.command()
def inventory(
    out: Annotated[Path, typer.Option(help="Markdown output path.")] = PROJECT_ROOT / "docs" / "data-inventory.md",
) -> None:
    """Summarize the lake (raw files per source, last run status) into docs/data-inventory.md."""
    storage = storage_from_uri(load_settings().storage_root)
    out.write_text(render_markdown(storage))
    typer.echo(f"wrote {out}")


@app.command()
def runs(last: Annotated[int, typer.Option(help="How many runs to show.")] = 20) -> None:
    """Show the most recent etl_run rows."""
    storage = storage_from_uri(load_settings().storage_root)
    table = runs_table(storage).sort("started_at", descending=True).head(last)
    with pl.Config(tbl_rows=last, tbl_width_chars=160, fmt_str_lengths=60):
        typer.echo(table)


@app.command()
def audit() -> None:
    """Check raw integrity: every manifest entry present, sha256/size match, file signature fits its type."""
    storage = storage_from_uri(load_settings().storage_root)
    checked, problems = audit_raw(storage)
    for p in problems:
        typer.echo(f"{p.key}: {p.problem}")
    typer.echo(f"{checked} files checked, {len(problems)} problems")
    if problems:
        raise typer.Exit(1)


@app.command()
def register(
    source: Annotated[str, typer.Argument(help="Raw source id, e.g. lbnl_queued_up.")],
    path: Annotated[Path, typer.Argument(exists=True, dir_okay=False, help="File downloaded by hand.")],
    dt: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Snapshot date (default: today).")] = None,
    origin_url: Annotated[str | None, typer.Option(help="Page or URL it was downloaded from.")] = None,
    note: Annotated[str | None, typer.Option(help="Free-text provenance note.")] = None,
) -> None:
    """Register a manually downloaded file as a raw snapshot (copied to raw/source=<id>/dt=<date>/, manifest origin: manual)."""
    storage = storage_from_uri(load_settings().storage_root)
    key = register_local_file(
        storage, source, path, dt=dt.date() if dt else local_today(), origin_url=origin_url, note=note
    )
    typer.echo(f"stored {key}" if key else "same content already registered; nothing written")
