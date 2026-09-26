"""Helpers shared by the EIA parsers and the other utility-directory parsers: zip members, header blocks
with group rows, string cleanup, Y/N flags and Texas county names → ``county_fips``."""

from __future__ import annotations

import re
import zipfile
from collections.abc import Sequence
from io import BytesIO

import polars as pl

from basecast_pipelines.processing.tabular import clean_label, integer, num, snake

# Texas counties (state FIPS 48), county FIPS → name, from Census PEP ``co-est2025-alldata.csv``
# (raw ``census_pep``, 254 counties), embedded so parsers of other sources can map county names without it.
_TX_COUNTIES = {
    "001": "Anderson", "003": "Andrews", "005": "Angelina", "007": "Aransas", "009": "Archer",
    "011": "Armstrong", "013": "Atascosa", "015": "Austin", "017": "Bailey", "019": "Bandera", "021": "Bastrop",
    "023": "Baylor", "025": "Bee", "027": "Bell", "029": "Bexar", "031": "Blanco", "033": "Borden",
    "035": "Bosque", "037": "Bowie", "039": "Brazoria", "041": "Brazos", "043": "Brewster", "045": "Briscoe",
    "047": "Brooks", "049": "Brown", "051": "Burleson", "053": "Burnet", "055": "Caldwell", "057": "Calhoun",
    "059": "Callahan", "061": "Cameron", "063": "Camp", "065": "Carson", "067": "Cass", "069": "Castro",
    "071": "Chambers", "073": "Cherokee", "075": "Childress", "077": "Clay", "079": "Cochran", "081": "Coke",
    "083": "Coleman", "085": "Collin", "087": "Collingsworth", "089": "Colorado", "091": "Comal",
    "093": "Comanche", "095": "Concho", "097": "Cooke", "099": "Coryell", "101": "Cottle", "103": "Crane",
    "105": "Crockett", "107": "Crosby", "109": "Culberson", "111": "Dallam", "113": "Dallas", "115": "Dawson",
    "117": "Deaf Smith", "119": "Delta", "121": "Denton", "123": "DeWitt", "125": "Dickens", "127": "Dimmit",
    "129": "Donley", "131": "Duval", "133": "Eastland", "135": "Ector", "137": "Edwards", "139": "Ellis",
    "141": "El Paso", "143": "Erath", "145": "Falls", "147": "Fannin", "149": "Fayette", "151": "Fisher",
    "153": "Floyd", "155": "Foard", "157": "Fort Bend", "159": "Franklin", "161": "Freestone", "163": "Frio",
    "165": "Gaines", "167": "Galveston", "169": "Garza", "171": "Gillespie", "173": "Glasscock",
    "175": "Goliad", "177": "Gonzales", "179": "Gray", "181": "Grayson", "183": "Gregg", "185": "Grimes",
    "187": "Guadalupe", "189": "Hale", "191": "Hall", "193": "Hamilton", "195": "Hansford", "197": "Hardeman",
    "199": "Hardin", "201": "Harris", "203": "Harrison", "205": "Hartley", "207": "Haskell", "209": "Hays",
    "211": "Hemphill", "213": "Henderson", "215": "Hidalgo", "217": "Hill", "219": "Hockley", "221": "Hood",
    "223": "Hopkins", "225": "Houston", "227": "Howard", "229": "Hudspeth", "231": "Hunt", "233": "Hutchinson",
    "235": "Irion", "237": "Jack", "239": "Jackson", "241": "Jasper", "243": "Jeff Davis", "245": "Jefferson",
    "247": "Jim Hogg", "249": "Jim Wells", "251": "Johnson", "253": "Jones", "255": "Karnes", "257": "Kaufman",
    "259": "Kendall", "261": "Kenedy", "263": "Kent", "265": "Kerr", "267": "Kimble", "269": "King",
    "271": "Kinney", "273": "Kleberg", "275": "Knox", "277": "Lamar", "279": "Lamb", "281": "Lampasas",
    "283": "La Salle", "285": "Lavaca", "287": "Lee", "289": "Leon", "291": "Liberty", "293": "Limestone",
    "295": "Lipscomb", "297": "Live Oak", "299": "Llano", "301": "Loving", "303": "Lubbock", "305": "Lynn",
    "307": "McCulloch", "309": "McLennan", "311": "McMullen", "313": "Madison", "315": "Marion",
    "317": "Martin", "319": "Mason", "321": "Matagorda", "323": "Maverick", "325": "Medina", "327": "Menard",
    "329": "Midland", "331": "Milam", "333": "Mills", "335": "Mitchell", "337": "Montague", "339": "Montgomery",
    "341": "Moore", "343": "Morris", "345": "Motley", "347": "Nacogdoches", "349": "Navarro", "351": "Newton",
    "353": "Nolan", "355": "Nueces", "357": "Ochiltree", "359": "Oldham", "361": "Orange", "363": "Palo Pinto",
    "365": "Panola", "367": "Parker", "369": "Parmer", "371": "Pecos", "373": "Polk", "375": "Potter",
    "377": "Presidio", "379": "Rains", "381": "Randall", "383": "Reagan", "385": "Real", "387": "Red River",
    "389": "Reeves", "391": "Refugio", "393": "Roberts", "395": "Robertson", "397": "Rockwall",
    "399": "Runnels", "401": "Rusk", "403": "Sabine", "405": "San Augustine", "407": "San Jacinto",
    "409": "San Patricio", "411": "San Saba", "413": "Schleicher", "415": "Scurry", "417": "Shackelford",
    "419": "Shelby", "421": "Sherman", "423": "Smith", "425": "Somervell", "427": "Starr", "429": "Stephens",
    "431": "Sterling", "433": "Stonewall", "435": "Sutton", "437": "Swisher", "439": "Tarrant", "441": "Taylor",
    "443": "Terrell", "445": "Terry", "447": "Throckmorton", "449": "Titus", "451": "Tom Green",
    "453": "Travis", "455": "Trinity", "457": "Tyler", "459": "Upshur", "461": "Upton", "463": "Uvalde",
    "465": "Val Verde", "467": "Van Zandt", "469": "Victoria", "471": "Walker", "473": "Waller", "475": "Ward",
    "477": "Washington", "479": "Webb", "481": "Wharton", "483": "Wheeler", "485": "Wichita",
    "487": "Wilbarger", "489": "Willacy", "491": "Williamson", "493": "Wilson", "495": "Winkler", "497": "Wise",
    "499": "Wood", "501": "Yoakum", "503": "Young", "505": "Zapata", "507": "Zavala",
}


def county_key(name: object) -> str:
    """``'De Witt County'`` → ``'dewitt'``: lowercase letters only, without a trailing "County"."""
    text = clean_label(name).lower()
    text = re.sub(r"\s+(county|parish)$", "", text)
    return re.sub(r"[^a-z]", "", text)


TX_COUNTY_FIPS: dict[str, str] = {county_key(name): "48" + code for code, name in _TX_COUNTIES.items()}


def tx_county_fips(county: pl.Expr | str, state: pl.Expr | str | None = None) -> pl.Expr:
    """County name → 5-char Texas ``county_fips`` (null when the name is not a Texas county, or the row's
    ``state`` is not TX)."""
    c = pl.col(county) if isinstance(county, str) else county
    key = (
        c.cast(pl.String).str.replace_all(r"\s+", " ").str.strip_chars().str.to_lowercase()
        .str.replace(r"\s+(county|parish)$", "").str.replace_all(r"[^a-z]", "")
    )
    fips = key.replace_strict(TX_COUNTY_FIPS, default=None, return_dtype=pl.String)
    if state is None:
        return fips
    s = pl.col(state) if isinstance(state, str) else state
    return pl.when(s == "TX").then(fips).otherwise(None)


def zip_members(data: bytes, pattern: str) -> list[tuple[str, bytes]]:
    """Members of a zip whose file name (without folders) matches ``pattern`` (case-insensitive)."""
    rx = re.compile(pattern, re.IGNORECASE)
    with zipfile.ZipFile(BytesIO(data)) as zf:
        return [
            (info.filename, zf.read(info))
            for info in zf.infolist()
            if not info.is_dir() and rx.search(info.filename.rsplit("/", 1)[-1])
        ]


def zip_member(data: bytes, pattern: str) -> tuple[str, bytes] | None:
    found = zip_members(data, pattern)
    if len(found) > 1:
        raise ValueError(f"several zip members match {pattern!r}: {[n for n, _ in found]}")
    return found[0] if found else None


def group_header(grid: pl.DataFrame, header_row: int, prefixes: Sequence[tuple[str, str]], *,
                 rename=snake) -> list[str]:
    """Column names for a header row that sits under a row of group labels (merged cells): each label is
    forward-filled across its columns and, when it matches one of ``prefixes`` (regex, prefix), prefixes
    the column name. Duplicates get a numeric suffix."""
    labels = list(grid.row(header_row))
    groups = list(grid.row(header_row - 1)) if header_row else [None] * len(labels)
    compiled = [(re.compile(p, re.IGNORECASE), prefix) for p, prefix in prefixes]
    names, seen, current = [], {}, ""
    for i, raw in enumerate(labels):
        group = clean_label(groups[i])
        if group:
            current = group
        prefix = next((pre for rx, pre in compiled if current and rx.search(current)), "")
        name = rename(raw) if clean_label(raw) else f"col_{i}"
        name = f"{prefix}{name}" if prefix and clean_label(raw) else name
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        names.append(name)
    return names


def body_with_names(grid: pl.DataFrame, header_row: int, names: list[str]) -> pl.DataFrame:
    """Rows below ``header_row`` with ``names`` as columns; entirely empty rows are dropped."""
    body = grid.slice(header_row + 1)
    body.columns = names
    return body.filter(~pl.all_horizontal(pl.all().is_null() | (pl.all().str.strip_chars() == "")))


def clean_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Strip every text column; empty strings become null."""
    return df.with_columns(
        pl.when(pl.col(c).str.strip_chars() == "").then(None).otherwise(pl.col(c).str.strip_chars()).alias(c)
        for c, t in df.schema.items()
        if t == pl.String
    )


def yes_no(expr: pl.Expr | str) -> pl.Expr:
    """``Y``/``Yes``/``True`` → true, ``N``/``No``/``False`` → false, anything else (blank, ``X`` = not
    applicable, ``U`` = unknown) → null."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    s = e.cast(pl.String).str.strip_chars().str.to_lowercase()
    return (
        pl.when(s.is_in(["y", "yes", "true"])).then(True)
        .when(s.is_in(["n", "no", "false"])).then(False)
        .otherwise(None)
        .cast(pl.Boolean)
    )


def id_text(expr: pl.Expr | str) -> pl.Expr:
    """An EIA id read as text (``'1015'``, ``'1015.0'``) → ``'1015'``; blanks → null."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    s = e.cast(pl.String).str.strip_chars().str.replace(r"\.0+$", "")
    return pl.when(s == "").then(None).otherwise(s)


_NULLISH = ["", ".", "-", "--", "n/a", "N/A", "NA", "TBD", "X", "U"]


def _check_lossless(df: pl.DataFrame, column: str, typed: pl.Expr, what: str) -> None:
    text = pl.col(column).cast(pl.String).str.strip_chars()
    bad = df.filter(text.is_not_null() & ~text.is_in(_NULLISH) & typed.is_null())
    if bad.height:
        sample = bad[column].unique().head(5).to_list()
        raise ValueError(f"{column}: {bad.height} values are not {what}, e.g. {sample}")


def type_columns(df: pl.DataFrame, *, floats: Sequence[str] = (), ints: Sequence[str] = (),
                 flags: Sequence[str] = ()) -> pl.DataFrame:
    """Cast text columns (those present) to Float64, Int64 or Boolean (Y/N), raising when a non-blank
    value would be lost, so a layout change that moves text into a numeric column fails loudly."""
    exprs = []
    for names, fn, what in ((floats, num, "numbers"), (ints, integer, "integers"), (flags, yes_no, "Y/N flags")):
        for c in names:
            if c not in df.columns:
                continue
            typed = fn(pl.col(c))
            if df.schema[c] == pl.String:
                _check_lossless(df, c, typed, what)
            exprs.append(typed.alias(c))
    return df.with_columns(exprs) if exprs else df


def check_identifiers(df: pl.DataFrame, table: str) -> pl.DataFrame:
    """Postgres truncates identifiers past 63 bytes; fail early instead of writing a broken table."""
    long = [c for c in df.columns if len(c.encode()) > 63]
    if long:
        raise ValueError(f"{table}: column names longer than 63 bytes: {long}")
    return df
