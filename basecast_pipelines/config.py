"""Runtime settings, read from the environment (a local .env is loaded when present)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_TZ = ZoneInfo("America/Chicago")
DEFAULT_USER_AGENT = "basecast-pipelines/0.1 (Base Power x AITX hackathon; public data research)"


@dataclass(frozen=True)
class Settings:
    storage_root: str
    user_agent: str


def load_settings() -> Settings:
    load_dotenv(PROJECT_ROOT / ".env")
    return Settings(
        storage_root=os.environ.get("STORAGE_ROOT", "file://./data"),
        user_agent=os.environ.get("BASECAST_USER_AGENT", DEFAULT_USER_AGENT),
    )


def local_today() -> date:
    """Today in America/Chicago, the date used for fetch-dated raw snapshots."""
    return datetime.now(LOCAL_TZ).date()
