from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List

from .http import create_session
from .models import Station

STATIONS_URL = "https://www.mvg.de/.rest/zdm/stations"


def fetch_stations() -> List[Station]:
    """Fetch all stations from MVG API and parse into models."""
    sess = create_session()
    resp = sess.get(STATIONS_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    stations: List[Station] = []
    for item in data:
        # Map possible fields with fallbacks
        station = Station(
            id=item.get("id") or item.get("globalId"),
            name=item.get("name"),
            place=item.get("place"),
            latitude=item.get("latitude"),
            longitude=item.get("longitude"),
            diva_id=item.get("diva_id") or item.get("divaId"),
            tariff_zones=item.get("tariff_zones") or item.get("tariffZones"),
            products=item.get("products"),
        )
        stations.append(station)
    return stations


def write_cache(stations: Iterable[Station], cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump([s.model_dump() for s in stations], f, ensure_ascii=False, indent=2)


def read_cache(cache_path: Path) -> List[Station]:
    with cache_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
        return [Station(**item) for item in raw]


# Default cache path at project root's data/ directory
DEFAULT_CACHE = Path(__file__).resolve().parent.parent.parent / "data" / "stations.json"


def refresh_stations_cache(cache_path: Path | None = None) -> List[Station]:
    cache_path = cache_path or DEFAULT_CACHE
    stations = fetch_stations()
    write_cache(stations, cache_path)
    return stations
