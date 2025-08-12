from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class StationsConfig(BaseModel):
    names: List[str] = Field(default_factory=list)
    ids: List[str] = Field(default_factory=list)


class Settings(BaseModel):
    db_url: str = "sqlite:///./data/reliability.db"
    polling_interval_seconds: int = 300
    stations: StationsConfig = Field(default_factory=StationsConfig)
    log_level: str = "INFO"

    @field_validator("log_level")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()


def _load_yaml(path: Optional[Path]) -> dict:
    if not path:
        return {}
    if not Path(path).exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with Path(path).open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping")
    return raw


def _env_override(config: dict) -> dict:
    # Environment variables take precedence; prefix TTR_
    # Supported:
    # TTR_DB_URL, TTR_POLLING_INTERVAL_SECONDS, TTR_LOG_LEVEL,
    # TTR_STATION_NAMES (comma), TTR_STATION_IDS (comma)
    out = dict(config)
    db_url = os.environ.get("TTR_DB_URL")
    if db_url:
        out["db_url"] = db_url
    poll = os.environ.get("TTR_POLLING_INTERVAL_SECONDS")
    if poll and poll.isdigit():
        out["polling_interval_seconds"] = int(poll)
    log = os.environ.get("TTR_LOG_LEVEL")
    if log:
        out["log_level"] = log
    names = os.environ.get("TTR_STATION_NAMES")
    ids = os.environ.get("TTR_STATION_IDS")
    if names or ids:
        stations = dict(out.get("stations") or {})
        if names:
            stations["names"] = [s.strip() for s in names.split(",") if s.strip()]
        if ids:
            stations["ids"] = [s.strip() for s in ids.split(",") if s.strip()]
        out["stations"] = stations
    return out


def load_settings(config_path: Optional[Path] = None) -> Settings:
    base = _load_yaml(config_path)
    merged = _env_override(base)
    return Settings(**merged)
