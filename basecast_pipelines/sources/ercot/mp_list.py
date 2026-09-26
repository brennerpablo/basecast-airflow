"""List of Market Participants in the ERCOT Region (EMIL NP12-215-ER): daily workbook with TDSP, LSE, QSE,
RE, IMRE and CRRAH registrations; only the latest file is listed, so each run keeps a snapshot. Contact
names and emails are personal data: drop them at parse. ``dt`` is the publish date."""

from __future__ import annotations

from basecast_pipelines.common.ercot_mis import listing_files
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner

SOURCE_ID = "ercot_mp_list"
EMIL_ID = "NP12-215-ER"


def discover(http: HttpClient) -> list[RemoteFile]:
    return listing_files(http, EMIL_ID)


run = source_runner(SOURCE_ID, discover)
