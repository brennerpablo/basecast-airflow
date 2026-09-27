"""Discovery of the census sources against small mocked pages and directory listings (Apache-style relative links,
as www2.census.gov serves them), including the place files ``marts/muni_places.py`` reads from the lake."""

from __future__ import annotations

from datetime import date

import httpx

from basecast_pipelines.sources.census import counties_geo, permits, population

WWW2 = "https://www2.census.gov"


def _listing(children: list[str]) -> str:
    sort = '<a href="?C=N;O=D">Name</a><a href="?C=M;O=A">Last modified</a><a href="/econ/">Parent Directory</a>'
    return "<html><body>" + sort + "".join(f'<a href="{c}">{c}</a>' for c in children) + "</body></html>"


def _serve(pages: dict[str, str]):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=pages.get(request.url.raw_path.decode(), "<html></html>"))

    return handler


def _name(f) -> str:
    return f.url.rsplit("/", 1)[-1]


def test_counties_geo_takes_newest_county_and_texas_place_shapefiles(make_http) -> None:
    geo = f"{WWW2}/geo/tiger"
    links = [
        ("shapefile", f"{geo}/GENZ2025/shp/cb_2025_us_county_500k.zip"),
        ("kml", f"{geo}/GENZ2025/kml/cb_2025_us_county_500k.zip"),
        ("shapefile", f"{geo}/GENZ2024/shp/cb_2024_us_county_500k.zip"),
        ("Oklahoma", f"{geo}/GENZ2025/shp/cb_2025_40_place_500k.zip"),
        ("Texas", f"{geo}/GENZ2025/kml/cb_2025_48_place_500k.zip"),
        ("Texas", f"{geo}/GENZ2025/shp/cb_2025_48_place_500k.zip"),
        ("Texas", f"{geo}/GENZ2024/shp/cb_2024_48_place_500k.zip"),
        ("Texas", f"{geo}/GENZ2025/shp/cb_2025_48_sldu_500k.zip"),
    ]
    page = "".join(f'<h3>Places</h3><a href="{url}">{text}</a>' for text, url in links)
    http = make_http(lambda request: httpx.Response(200, text=page))

    files = counties_geo.discover(http)
    assert [f.url for f in files] == [links[0][1], links[5][1]]
    county, place = files
    assert county.meta == {"vintage": 2025, "link_text": "shapefile"}  # unchanged from before the place file
    assert place.meta == {"kind": "place", "state_fips": "48", "vintage": 2025, "link_text": "Texas"}
    assert [f.url for f in counties_geo.discover(http, include_places=False)] == [links[0][1]]


def _bps_pages(region: list[str]) -> dict[str, str]:
    bps = "/econ/bps/"
    return {
        "/construction/bps/": f'<a href="{WWW2}{bps}">Data directory</a>',
        bps: _listing(["County/", "Metro%20(ending%202023)/", "Place/", "State/"]),
        f"{bps}County/": _listing(["co2024a.txt", "co2025a.txt", "co2608c.txt", "cty2025a.txt"]),
        f"{bps}Place/": _listing(["Midwest%20Region/", "South%20Region/", "West%20Region/", "foot1609.txt"]),
        f"{bps}Place/South%20Region/": _listing(region),
    }


def test_bps_adds_south_region_place_files(make_http, monkeypatch) -> None:
    monkeypatch.setattr(permits, "local_today", lambda: date(2026, 9, 26))
    region = [
        *(f"so{y}a.txt" for y in range(2019, 2026)),
        "so8801y.txt", "so9912y.txt", "so2507y.txt", "so2508y.txt", "so2607y.txt", "so2608y.txt",
        "so2608c.txt", "so2608r.txt",
    ]  # fmt: skip
    http = make_http(_serve(_bps_pages(region)))

    files = permits.discover(http)
    county = [f for f in files if f.meta["kind"] in {"annual", "monthly"}]
    assert [_name(f) for f in county] == ["co2024a.txt", "co2025a.txt", "co2608c.txt"]
    assert county[0].source_page == f"{WWW2}/econ/bps/County/"
    places = [f for f in files if f.meta["kind"].startswith("place")]
    assert [_name(f) for f in places] == [
        "so2022a.txt", "so2023a.txt", "so2024a.txt", "so2025a.txt", "so2508y.txt", "so2608y.txt",
    ]  # fmt: skip
    assert places[0].url == f"{WWW2}/econ/bps/Place/South%20Region/so2022a.txt"
    assert places[-1].meta == {"kind": "place_ytd", "region": "South Region", "year": 2026, "month": 8}
    assert places[-2].meta["year"] == 2025 and places[0].meta == {
        "kind": "place_annual", "region": "South Region", "year": 2022,
    }  # fmt: skip

    fewer = permits.discover(http, place_annual_years=2, include_monthly=False)
    assert [_name(f) for f in fewer] == ["co2024a.txt", "co2025a.txt", "so2024a.txt", "so2025a.txt", "so2508y.txt",
                                         "so2608y.txt"]
    assert [_name(f) for f in permits.discover(http, include_places=False)] == [_name(f) for f in county]


def test_bps_place_ytd_century_and_missing_prior_year() -> None:
    today = date(2026, 9, 26)
    # 1999 is older than 2026, whatever "99" > "26" says; without Aug 2025, only Aug 2026 is taken.
    picked = permits.place_files(["so9912y.txt", "so2608y.txt", "so2607y.txt", "so2507y.txt"], today=today)
    assert [n for n, _ in picked] == ["so2608y.txt"]
    assert permits.place_files(["so2608c.txt", "sofoot.txt"], today=today) == []


def test_pep_adds_texas_place_totals_and_their_layout(make_http) -> None:
    popest = "/programs-surveys/popest/"
    datasets, layouts = f"{popest}datasets/", f"{popest}technical-documentation/file-layouts/"
    pages = {
        datasets: _listing(["2010-2020/", "2020-2025/"]),
        f"{datasets}2020-2025/": _listing(["cities/", "counties/", "state/"]),
        f"{datasets}2020-2025/counties/": _listing(["asrh/", "totals/"]),
        f"{datasets}2020-2025/counties/totals/": _listing(["co-est2025-alldata.csv", "co-est2025-pop.xlsx"]),
        f"{datasets}2020-2025/cities/": _listing(["totals/"]),
        f"{datasets}2020-2025/cities/totals/": _listing(
            ["sub-est2025.csv", "sub-est2025_4.csv", "sub-est2025_48.csv", "sub-est2025_480.csv"]
        ),
        layouts: _listing(["2010-2020/", "2020-2025/"]),
        f"{layouts}2020-2025/": _listing(["CC-EST2025-ALLDATA.pdf", "CO-EST2025-ALLDATA.pdf", "SUB-EST2025.pdf"]),
    }
    files = population.discover(make_http(_serve(pages)))
    by_series = {(f.meta["series"], f.meta["kind"]): _name(f) for f in files}
    assert by_series == {
        ("postcensal_latest", "data"): "co-est2025-alldata.csv",
        ("postcensal_latest", "layout"): "CO-EST2025-ALLDATA.pdf",
        ("places_latest", "data"): "sub-est2025_48.csv",
        ("places_latest", "layout"): "SUB-EST2025.pdf",
    }
    place = next(f for f in files if f.meta["series"] == "places_latest" and f.meta["kind"] == "data")
    assert place.meta["vintage"] == "Vintage 2025"
    assert place.url == f"{WWW2}{datasets}2020-2025/cities/totals/sub-est2025_48.csv"
