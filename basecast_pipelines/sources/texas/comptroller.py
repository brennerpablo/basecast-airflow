"""Texas Comptroller large-project signals by county:
- Local Development Agreement database (Ch. 312 tax abatements and zones, Ch. 380/381 agreements): the
  daily CSVs whose links the open-data API returns in ``downloadLink``; the API base and table endpoints are
  read from the SB 1340 results page script;
- the Qualifying Data Center / Large Data Center Project registry (HTML tables, no county);
- current JETI (Ch. 403) agreements (HTML table; the API's CSV link is dead).
No license stated; free text is inconsistent. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "tx_comptroller"
RESULTS_PAGE = "https://comptroller.texas.gov/economy/development/search-tools/sb1340/results.php"
HTML_PAGES = {
    "data-center-lists.html": "https://comptroller.texas.gov/taxes/data-centers/data-center-lists.php",
    "jeti-current-agreements.html": "https://comptroller.texas.gov/economy/development/prop-tax/jeti/current-agreements.php",
}

_API_BASE = re.compile(r"https://api\.comptroller\.texas\.gov/open-data/v\d+")
_ENDPOINT = re.compile(r'"endpointPath"\s*:\s*"(/tables/[a-z0-9-]+)"')


def _datatables_params() -> dict[str, str]:
    """The minimal DataTables query the API accepts (one column spec is mandatory)."""
    return {
        "draw": "1", "start": "0", "length": "1", "search[value]": "", "search[regex]": "false",
        "order[0][column]": "0", "order[0][dir]": "asc", "columns[0][data]": "id", "columns[0][name]": "",
        "columns[0][searchable]": "true", "columns[0][orderable]": "true", "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",
    }  # fmt: skip


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    page = http.get(RESULTS_PAGE).text
    base = _API_BASE.search(page)
    if base is None:
        raise RuntimeError(f"open-data API base not found on {RESULTS_PAGE}")
    files: list[RemoteFile] = []
    seen: set[str] = set()
    for endpoint in dict.fromkeys(_ENDPOINT.findall(page)):
        payload = http.get(base.group(0) + endpoint, params=_datatables_params()).json()
        if not payload.get("success"):
            raise RuntimeError(f"{endpoint}: {payload.get('error')}")
        links = payload.get("downloadLink") or []
        for url in [links] if isinstance(links, str) else links:
            if url not in seen:
                seen.add(url)
                files.append(RemoteFile(url=url, dt=today, source_page=RESULTS_PAGE,
                                        meta={"endpoint": endpoint, "last_updated": payload.get("lastUpdated"),
                                              "records_total": payload.get("recordsTotal")}))
    for filename, url in HTML_PAGES.items():
        files.append(RemoteFile(url=url, dt=today, filename=filename, source_page=url))
    return files


run = source_runner(SOURCE_ID, discover)
