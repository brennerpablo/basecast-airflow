"""Public S3-compatible bucket listings (ListObjectsV2 over plain HTTP, no credentials)."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass

from basecast_pipelines.common.http import HttpClient

_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


@dataclass(frozen=True)
class S3Object:
    key: str
    size: int
    last_modified: str
    etag: str


def parse_listing(xml_text: str) -> tuple[list[S3Object], str | None]:
    root = ET.fromstring(xml_text)
    objects = [
        S3Object(
            key=c.findtext("s3:Key", "", _NS),
            size=int(c.findtext("s3:Size", "0", _NS)),
            last_modified=c.findtext("s3:LastModified", "", _NS),
            etag=c.findtext("s3:ETag", "", _NS).strip('"'),
        )
        for c in root.findall("s3:Contents", _NS)
    ]
    token = root.findtext("s3:NextContinuationToken", None, _NS)
    truncated = root.findtext("s3:IsTruncated", "false", _NS) == "true"
    return objects, token if truncated else None


def list_objects(http: HttpClient, endpoint: str, prefix: str) -> list[S3Object]:
    """Every object under ``prefix`` (follows continuation tokens)."""
    objects: list[S3Object] = []
    params = {"list-type": "2", "prefix": prefix}
    while True:
        page, token = parse_listing(http.get(endpoint, params=params).text)
        objects += page
        if token is None:
            return objects
        params = {**params, "continuation-token": token}
