"""Settlement Points List and Electrical Buses Mapping (EMIL NP4-160-SG), posted with each weekly network
model load: buses to settlement points, load zones and hubs, plus the NOIE mapping that ties LZ_AEN, LZ_CPS,
LZ_LCRA and LZ_RAYBN to physical loads. The listing keeps 31 days, so snapshot at least weekly.
``dt`` is the publish date."""

from __future__ import annotations

from basecast_pipelines.common.ercot_mis import listing_files
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner

SOURCE_ID = "ercot_settlement_points"
EMIL_ID = "NP4-160-SG"


def discover(http: HttpClient) -> list[RemoteFile]:
    return listing_files(http, EMIL_ID)


run = source_runner(SOURCE_ID, discover)
