"""Discovery of the sources added after the first backfill, against small mocked responses."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import httpx

from basecast_pipelines.common.arcgis import layer_files
from basecast_pipelines.common.s3 import list_objects
from basecast_pipelines.config import local_today
from basecast_pipelines.sources.eia import eia_860m
from basecast_pipelines.sources.texas import comptroller

S3_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><IsTruncated>{truncated}</IsTruncated>
{token}<Contents><Key>{key}</Key><LastModified>2026-07-09T21:44:08.765Z</LastModified>
<ETag>&quot;abc&quot;</ETag><Size>1150865</Size></Contents></ListBucketResult>"""


def test_s3_listing_follows_continuation(make_http) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "continuation-token" in request.url.params:
            return httpx.Response(200, text=S3_PAGE.format(truncated="false", token="", key="p/b.parquet"))
        token = "<NextContinuationToken>t1</NextContinuationToken>"
        return httpx.Response(200, text=S3_PAGE.format(truncated="true", token=token, key="p/a.parquet"))

    objects = list_objects(make_http(handler), "https://bucket.example", "p/")
    assert [o.key for o in objects] == ["p/a.parquet", "p/b.parquet"]
    assert objects[0].size == 1150865 and objects[0].etag == "abc"


def test_arcgis_layer_pages(make_http) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["returnCountOnly"] == "true"
        return httpx.Response(200, json={"count": 60})

    files = layer_files(make_http(handler), "https://svc/FeatureServer/310", dt=local_today(), prefix="coop_",
                        page_size=25)
    assert [f.filename for f in files] == [
        "coop_layer.json", "coop_features_00000.geojson", "coop_features_00025.geojson", "coop_features_00050.geojson",
    ]
    query = parse_qs(urlsplit(files[-1].url).query)
    assert query["resultOffset"] == ["50"] and query["f"] == ["geojson"] and query["orderByFields"] == ["OBJECTID"]


def test_comptroller_reads_download_links_from_api(make_http) -> None:
    page = (
        'const api = "https://api.comptroller.texas.gov/open-data/v1";'
        '{"endpointPath":"/tables/ch380"},{"endpointPath":"/tables/ch312-abatement"}'
    )
    links = {
        "/open-data/v1/tables/ch380": "https://assets.comptroller.texas.gov/open-data-files/ch380.csv",
        "/open-data/v1/tables/ch312-abatement": [
            "https://assets.comptroller.texas.gov/open-data-files/ch312-abatement.csv",
            "https://assets.comptroller.texas.gov/open-data-files/ch312-abatement-detail.csv",
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "comptroller.texas.gov":
            return httpx.Response(200, text=page)
        assert request.url.params["columns[0][data]"] == "id"
        return httpx.Response(200, json={"success": True, "downloadLink": links[request.url.path]})

    names = [f.filename or f.url.rsplit("/", 1)[-1] for f in comptroller.discover(make_http(handler))]
    assert names[:3] == ["ch380.csv", "ch312-abatement.csv", "ch312-abatement-detail.csv"]
    assert "data-center-lists.html" in names


def test_eia_860m_takes_latest_and_decembers(make_http) -> None:
    html = "".join(
        f'<a href="archive/xls/{m}_generator{y}.xlsx">XLS</a>'
        for y, m in [(2024, "november"), (2024, "december"), (2025, "december"), (2026, "july")]
    ) + '<!-- <a href="xls/december_generator2026.xlsx">future</a> -->' + '<a href="xls/august_generator2026.xlsx">XLS</a>'
    http = make_http(lambda request: httpx.Response(200, text=html))
    names = [f.url.rsplit("/", 1)[-1] for f in eia_860m.discover(http)]
    assert names == ["december_generator2024.xlsx", "december_generator2025.xlsx", "august_generator2026.xlsx"]
    assert len(eia_860m.discover(http, all_months=True)) == 5


def test_puct_filings_takes_every_item_once(make_http) -> None:
    from basecast_pipelines.sources.puct import interchange

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/search/filings/" in url:
            control = request.url.params["ControlNumber"]
            items = "".join(
                f'<a href="/search/documents/?controlNumber={control}&itemNumber={i}">{i}</a>' for i in (1, 2, 2)
            )
            return httpx.Response(200, text=f'<a href="/search/exportfilings/?ControlNumber={control}">Export</a>{items}')
        control, item = request.url.params["controlNumber"], request.url.params["itemNumber"]
        return httpx.Response(200, text=f'<a href="/Documents/{control}_{item}_99.PDF">doc</a>')

    files = interchange.discover(make_http(handler))
    docs = [f for f in files if f.meta["kind"] == "document"]
    assert len(docs) == 2 * len(interchange.DOCKETS) and all(f.immutable for f in docs)
    assert len([f for f in files if f.meta["kind"] == "filings_index"]) == len(interchange.DOCKETS)
    only = interchange.discover(make_http(handler), items="2")
    assert {f.meta["item"] for f in only if f.meta["kind"] == "document"} == {2}
