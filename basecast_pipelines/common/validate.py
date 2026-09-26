"""Content checks for downloaded files: catch error pages served with HTTP 200 before they reach raw, and
audit what raw already holds (manifest sha256/size and file signatures)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from basecast_pipelines.common.manifest import MANIFEST_NAME
from basecast_pipelines.common.storage import Storage

_SIGNATURES: dict[str, tuple[bytes, ...]] = {
    "xlsx": (b"PK\x03\x04",),
    "xlsb": (b"PK\x03\x04",),
    "xlsm": (b"PK\x03\x04",),
    "docx": (b"PK\x03\x04",),
    "zip": (b"PK\x03\x04",),
    "xls": (b"\xd0\xcf\x11\xe0",),
    "doc": (b"\xd0\xcf\x11\xe0",),
    "pdf": (b"%PDF",),
}
_HTML_STARTS = (b"<!doctype html", b"<html")


def _extension(filename: str) -> str:
    return filename.rsplit(".", 1)[-1].lower() if "." in filename else ""


def error_payload(head: bytes, filename: str, full_text: str | None = None) -> str | None:
    """Why this looks like an error response rather than the file, or None."""
    ext = _extension(filename)
    if ext not in {"html", "htm"} and head.lstrip()[:20].lower().startswith(_HTML_STARTS):
        return "got an HTML page instead of a file"
    if ext in {"json", "geojson"} and full_text is not None:
        try:
            payload = json.loads(full_text)
        except json.JSONDecodeError as exc:
            return f"invalid JSON: {exc}"
        if isinstance(payload, dict) and payload.get("error"):
            return f"error payload: {str(payload['error'])[:200]}"
    return None


def check_download(path: Path, filename: str) -> str | None:
    head = path.open("rb").read(2048)
    full_text = path.read_text(errors="replace") if _extension(filename) in {"json", "geojson"} else None
    return error_payload(head, filename, full_text)


def signature_mismatch(head: bytes, filename: str) -> str | None:
    expected = _SIGNATURES.get(_extension(filename))
    if expected and not head.startswith(expected):
        return f"content does not look like .{_extension(filename)} (starts with {head[:8]!r})"
    return None


@dataclass
class AuditProblem:
    key: str
    problem: str


def audit_raw(storage: Storage) -> tuple[int, list[AuditProblem]]:
    """Check every manifest entry: file present, size and sha256 match, signature fits the extension."""
    checked, problems = 0, []
    listed = set()
    for manifest_key in [k for k in storage.list("raw/") if k.endswith("/" + MANIFEST_NAME)]:
        prefix = manifest_key.removesuffix(MANIFEST_NAME)
        for entry in json.loads(storage.read_bytes(manifest_key))["entries"]:
            key = prefix + entry["file"]
            listed.add(key)
            checked += 1
            if not storage.exists(key):
                problems.append(AuditProblem(key, "missing"))
                continue
            data = storage.read_bytes(key)
            if len(data) != entry["bytes"]:
                problems.append(AuditProblem(key, f"size {len(data)} != manifest {entry['bytes']}"))
            if hashlib.sha256(data).hexdigest() != entry["sha256"]:
                problems.append(AuditProblem(key, "sha256 differs from manifest"))
            issue = error_payload(data[:2048], entry["file"]) or signature_mismatch(data[:2048], entry["file"])
            if issue:
                problems.append(AuditProblem(key, issue))
    for key in storage.list("raw/"):
        if not key.endswith(MANIFEST_NAME) and key not in listed:
            problems.append(AuditProblem(key, "file not in any manifest"))
    return checked, problems
