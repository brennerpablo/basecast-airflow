"""Discovery against small fixtures trimmed from real ercot.com pages (fetched 2026-09-25)."""

from __future__ import annotations

import json
from datetime import date

import httpx

from basecast_pipelines.common.discovery import extract_links, file_links, parse_month_year
from basecast_pipelines.common.ercot_mis import parse_doc_list, parse_product_page
from basecast_pipelines.sources.ercot import gis, ltlf
from tests.conftest import FIXTURES


def test_product_page_declares_listing_services() -> None:
    html = (FIXTURES / "ercot_product_pg7-200-er.html").read_text()
    product = parse_product_page(html, "pg7-200-er", "https://www.ercot.com/p")
    assert product.report_type_id == "15933"
    assert product.list_url == "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId="
    assert product.download_url == "https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId="


def test_doc_list_parsing() -> None:
    docs = parse_doc_list(json.loads((FIXTURES / "ercot_doclist_15933.json").read_text()))
    assert [d.friendly_name for d in docs] == [
        "Co-located_Battery_Identification_Report_August_2026",
        "GIS_Report_August2026",
        "GIS_Report_June_2019",
    ]
    assert docs[1].filename.startswith("RPT.00015933.") and docs[1].filename.endswith("GIS_Report_August2026.xlsx")
    assert docs[1].content_size > 0 and docs[1].publish_date.tzinfo is not None


def test_parse_month_year_variants() -> None:
    assert parse_month_year("GIS_Report_August2026") == date(2026, 8, 1)
    assert parse_month_year("GIS_Report_June_2019") == date(2019, 6, 1)
    assert parse_month_year("GIS REPORT August 2016, corrected wind chart") == date(2016, 8, 1)
    assert parse_month_year("gis_report__september_2014_final.xls") == date(2014, 9, 1)
    assert parse_month_year("GIS_Report_Sept2021") == date(2021, 9, 1)
    assert parse_month_year("Appendix_D_Profile_Decision_Tree_050124.xlsx") is None


def test_links_carry_their_section() -> None:
    html = (FIXTURES / "ercot_load_forecast.html").read_text()
    links = file_links(extract_links(html, "https://www.ercot.com/gridinfo/load/forecast"), (".xlsx", ".xlsb", ".docx"))
    by_name = {link.filename: link for link in links}
    assert by_name["ErcotAdjustedForecast.xlsb"].section.startswith("Long-Term Load Forecast")
    assert by_name["ErcotAdjustedForecast.xlsb"].url.startswith("https://www.ercot.com/files/docs/2025/")
    assert "Weather Year Scenario" in by_name["Coast.xlsx"].section
    assert any("Mid-Term" in link.section for link in links)


def _serve(pages: dict[str, str]):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=pages.get(request.url.path, "<html></html>"))

    return handler


def test_ltlf_skips_mid_term_and_weather_scenarios(make_http) -> None:
    html = (FIXTURES / "ercot_load_forecast.html").read_text()
    http = make_http(_serve({"/gridinfo/load/forecast": html}))
    names = {f.url.rsplit("/", 1)[-1] for f in ltlf.discover(http)}
    assert "ErcotAdjustedForecast.xlsb" in names and "Summer-and-Winter-Peaks.xlsx" in names
    assert "Coast.xlsx" not in names
    assert not any("Metrics" in n or "Backcast" in n for n in names)
    with_scenarios = {f.url.rsplit("/", 1)[-1] for f in ltlf.discover(http, include_weather_scenarios=True)}
    assert "Coast.xlsx" in with_scenarios


def test_gis_combines_listing_and_year_pages(make_http) -> None:
    product_html = (FIXTURES / "ercot_product_pg7-200-er.html").read_text()
    doclist = (FIXTURES / "ercot_doclist_15933.json").read_text()
    year_html = (FIXTURES / "ercot_resource_2014.html").read_text()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/mp/data-products/data-product-details":
            return httpx.Response(200, text=product_html)
        if path == "/misapp/servlets/IceDocListJsonWS":
            return httpx.Response(200, text=doclist)
        if path == "/gridinfo/resource":
            return httpx.Response(200, text='<a href="/gridinfo/resource/2014">2014</a>')
        if path == "/gridinfo/resource/2014":
            return httpx.Response(200, text=year_html)
        return httpx.Response(404)

    files = gis.discover(make_http(handler))
    listed = [f for f in files if f.doc_id]
    assert {f.dt for f in listed} == {date(2026, 8, 1), date(2019, 6, 1)}
    assert {f.meta["family"] for f in listed} == {"gis_report", "co_located_battery"}
    older = [f for f in files if not f.doc_id]
    assert len(older) == 8 and min(f.dt for f in older) == date(2014, 5, 1)
    assert gis.discover(make_http(handler), include_colocated_battery=False, include_year_pages=False)[0].meta[
        "family"
    ] == "gis_report"
