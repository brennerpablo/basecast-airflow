"""Settlement Points List and Electrical Buses Mapping (NP4-160-SG) → the settlement point mapping tables, one
per csv in the zip, rebuilt from the newest snapshot (``replace`` + ``latest_dt``):

- ``ercot_settlement_point_map`` (``Settlement_Points``): electrical bus → PSS/E bus, substation, voltage, load
  zone (``LZ_HOUSTON/NORTH/SOUTH/WEST`` only), resource node and hub bus / hub where the bus is one. The NOIE
  load zone (``LZ_AEN``, ``LZ_CPS``, ``LZ_LCRA``, ``LZ_RAYBN``) of the bus is added from ``NOIE_Mapping``
  (every NOIE bus is in the list; no bus maps to two NOIE zones);
- ``ercot_noie_load_map`` (``NOIE_Mapping``): physical load → NOIE load zone, substation, electrical bus;
- ``ercot_resource_node_unit`` (``Resource_Node_to_Unit``): resource node → generating units;
- ``ercot_ccp_resource`` (``CCP_Resource_Names``): combined-cycle plant → logical resource node;
- ``ercot_hub_dc_tie`` (``Hub_Name_AND_DC_Ties``): hub and DC-tie settlement point names.

Each zip is one weekly network model; ``network_model`` keeps its name (e.g. ``CIM_Sep_ML3_1_09302026``, as
in the document's friendly name). The csv headers are read by name; ``voltage_kv`` and ``psse_bus_number``
are typed, ``PSSE_BUS_NUMBER`` joins the TPIT SSWG bus numbers. The listing keeps 31 days, so older models are
only in the raw snapshots.
"""

from __future__ import annotations

import io
import re
import zipfile
from collections.abc import Callable
from pathlib import PurePosixPath

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_dt
from basecast_pipelines.processing.tabular import num, snake

SOURCE_ID = "ercot_settlement_points"

_MODEL = re.compile(r"(CIM_[A-Za-z]+_ML\d+_\d+_\d{8})")


def _network_model(f: RawFile) -> str | None:
    for text in (f.meta.get("friendly_name") or "", f.name):
        if m := _MODEL.search(text):
            return m.group(1)
    return None


def _csv(f: RawFile, prefix: str) -> pl.DataFrame:
    """The zip member whose name starts with ``prefix`` (case-insensitive), all columns as text, snake_case."""
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        members = [n for n in z.namelist()
                   if n.lower().endswith(".csv") and PurePosixPath(n).name.lower().startswith(prefix.lower())]
        if len(members) != 1:
            raise ValueError(f"{f.key}: expected one {prefix}*.csv, found {members}")
        df = pl.read_csv(z.read(members[0]), infer_schema=False)
    df = df.rename({c: snake(c) for c in df.columns})
    return df.with_columns(pl.col(c).str.strip_chars().replace("", None) for c in df.columns)


def _require(f: RawFile, df: pl.DataFrame, columns: set[str]) -> None:
    missing = columns - set(df.columns)
    if missing:
        raise ValueError(f"{f.key}: missing columns {sorted(missing)} in {df.columns}")


def _with_model(f: RawFile, df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.lit(_network_model(f), pl.String).alias("network_model"))


def parse_settlement_points(f: RawFile) -> pl.DataFrame:
    sp = _csv(f, "Settlement_Points")
    _require(f, sp, {"electrical_bus", "node_name", "psse_bus_name", "voltage_level", "substation",
                     "settlement_load_zone", "resource_node", "hub_bus_name", "hub", "psse_bus_number"})
    noie = _noie(f).select("electrical_bus", "noie_load_zone").unique("electrical_bus", keep="first")
    out = (
        sp.select(
            "electrical_bus", "node_name", "psse_bus_name",
            num("voltage_level").alias("voltage_kv"),
            "substation",
            pl.col("settlement_load_zone").str.to_uppercase().alias("load_zone"),
            "resource_node", "hub_bus_name", "hub",
            pl.col("psse_bus_number").cast(pl.Int64, strict=False).alias("psse_bus_number"),
        )
        .join(noie, on="electrical_bus", how="left")
    )
    if out["electrical_bus"].is_duplicated().any():
        raise ValueError(f"{f.key}: duplicated electrical buses")
    return _with_model(f, out)


def _noie(f: RawFile) -> pl.DataFrame:
    df = _csv(f, "NOIE_Mapping")
    _require(f, df, {"physical_load", "noie", "voltage_name", "substation", "electrical_bus"})
    return df.select(
        "physical_load",
        pl.col("noie").str.to_uppercase().alias("noie_load_zone"),
        num("voltage_name").alias("voltage_kv"),
        "substation",
        "electrical_bus",
    )


def parse_noie(f: RawFile) -> pl.DataFrame:
    return _with_model(f, _noie(f))


def parse_resource_node_unit(f: RawFile) -> pl.DataFrame:
    df = _csv(f, "Resource_Node_to_Unit")
    _require(f, df, {"resource_node", "unit_substation", "unit_name"})
    return _with_model(f, df.select("resource_node", "unit_substation", "unit_name"))


def parse_ccp(f: RawFile) -> pl.DataFrame:
    df = _csv(f, "CCP_Resource_Names")
    logical = next((c for c in df.columns if c.startswith("logical")), None)  # LOGICALREOURCENODENAME (sic)
    _require(f, df, {"ccp_name"})
    if logical is None:
        raise ValueError(f"{f.key}: no logical resource node column in {df.columns}")
    return _with_model(f, df.select("ccp_name", pl.col(logical).alias("logical_resource_node")))


def parse_hub_dc_tie(f: RawFile) -> pl.DataFrame:
    df = _csv(f, "Hub_Name")
    _require(f, df, {"name"})
    return _with_model(
        f,
        df.select(
            pl.col("name").alias("settlement_point"),
            pl.when(pl.col("name").str.starts_with("HB_")).then(pl.lit("hub"))
            .when(pl.col("name").str.starts_with("DC_")).then(pl.lit("dc_tie"))
            .otherwise(None).alias("kind"),
        ),
    )


def _dataset(name: str, description: str, parse: Callable[[RawFile], pl.DataFrame]) -> Dataset:
    return Dataset(
        name=name,
        target="postgres",
        mode="replace",
        description=description,
        parse=parse,
        inputs=lambda f: f.suffix == ".zip",
        select=latest_dt,
    )


DATASETS = [
    _dataset(
        "ercot_settlement_point_map",
        "Electrical bus → PSS/E bus, substation, voltage, load zone, NOIE load zone, resource node and hub "
        "(NP4-160-SG Settlement_Points + NOIE_Mapping, newest network model).",
        parse_settlement_points,
    ),
    _dataset(
        "ercot_noie_load_map",
        "Physical load → NOIE load zone (LZ_AEN, LZ_CPS, LZ_LCRA, LZ_RAYBN), substation and electrical bus "
        "(NP4-160-SG NOIE_Mapping, newest network model).",
        parse_noie,
    ),
    _dataset(
        "ercot_resource_node_unit",
        "Resource node settlement point → generating units (NP4-160-SG Resource_Node_to_Unit).",
        parse_resource_node_unit,
    ),
    _dataset(
        "ercot_ccp_resource",
        "Combined-cycle plant → logical resource node (NP4-160-SG CCP_Resource_Names).",
        parse_ccp,
    ),
    _dataset(
        "ercot_hub_dc_tie",
        "Hub and DC-tie settlement point names (NP4-160-SG Hub_Name_AND_DC_Ties).",
        parse_hub_dc_tie,
    ),
]
