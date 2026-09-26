"""Parser registry: source id → parser module declaring ``DATASETS`` (see ``processing/core.py``).

A source's parser lives at the path of its raw module with ``sources`` replaced by ``parsers``
(``sources/ercot/gis.py`` → ``parsers/ercot/gis.py``), so adding a parser never edits this file.
``PARSER_ONLY`` holds datasets that have no raw source (files versioned in this repo)."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

from basecast_pipelines.sources import SOURCE_MODULES

PARSER_ONLY: dict[str, str] = {
    "config_facts": "basecast_pipelines.parsers.config_facts",
}


class NoParserError(LookupError):
    pass


def parser_path(source_id: str) -> str:
    if source_id in PARSER_ONLY:
        return PARSER_ONLY[source_id]
    if source_id not in SOURCE_MODULES:
        raise NoParserError(source_id)
    return SOURCE_MODULES[source_id].replace(".sources.", ".parsers.", 1)


def get_parser(source_id: str) -> ModuleType:
    path = parser_path(source_id)
    try:
        return import_module(path)
    except ModuleNotFoundError as exc:
        if exc.name == path:
            raise NoParserError(source_id) from None
        raise


def has_parser(source_id: str) -> bool:
    try:
        get_parser(source_id)
    except NoParserError:
        return False
    return True


def processable_sources() -> list[str]:
    return [s for s in [*SOURCE_MODULES, *PARSER_ONLY] if has_parser(s)]
