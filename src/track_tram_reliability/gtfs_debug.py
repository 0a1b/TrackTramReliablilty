from __future__ import annotations

import io
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from .http import create_session
from .stations import read_cache, DEFAULT_CACHE
from .gtfs_index import _open_zip_from_source, _read_csv_from_zip  # type: ignore


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    from math import radians, sin, cos, atan2, sqrt

    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


@dataclass
class GtfsStop:
    stop_id: str
    stop_name: str
    lat: float
    lon: float
    location_type: str | None
    parent_station: str | None


def load_gtfs_stops(gtfs_source: str | Path) -> List[GtfsStop]:
    with _open_zip_from_source(gtfs_source) as zf:
        stops_rows = _read_csv_from_zip(zf, "stops.txt")
    out: List[GtfsStop] = []
    for r in stops_rows:
        sid = (r.get("stop_id") or "").strip()
        name = (r.get("stop_name") or "").strip()
        lat = r.get("stop_lat")
        lon = r.get("stop_lon")
        if not sid or not name or not lat or not lon:
            continue
        try:
            out.append(
                GtfsStop(
                    stop_id=sid,
                    stop_name=name,
                    lat=float(lat),
                    lon=float(lon),
                    location_type=(r.get("location_type") or "").strip() or None,
                    parent_station=(r.get("parent_station") or "").strip() or None,
                )
            )
        except Exception:
            continue
    return out


def debug_link_for_stop_name(
    stop_query: str,
    gtfs_source: str | Path,
    stations_cache: Path | None = None,
    radius_m: float = 300.0,
) -> Dict:
    stops = load_gtfs_stops(gtfs_source)
    stations = read_cache(stations_cache or DEFAULT_CACHE)
    station_ids = {s.id for s in stations}
    station_points: List[Tuple[str, float, float, str]] = []
    for s in stations:
        if s.latitude is None or s.longitude is None:
            continue
        station_points.append((s.id, s.latitude, s.longitude, s.name or ""))

    q = stop_query.strip().lower()
    matches = [s for s in stops if q in s.stop_name.lower()]

    results = []
    for st in matches:
        direct_id_match = st.stop_id in station_ids
        parent_id_match = st.parent_station in station_ids if st.parent_station else False
        # Nearest MVG stations by distance
        nearest = []
        for mv_id, mv_lat, mv_lon, mv_name in station_points:
            d = _haversine_m(st.lat, st.lon, mv_lat, mv_lon)
            if d <= radius_m:
                nearest.append((d, mv_id, mv_name))
        nearest.sort(key=lambda x: x[0])
        results.append(
            {
                "gtfs_stop_id": st.stop_id,
                "gtfs_stop_name": st.stop_name,
                "gtfs_parent_station": st.parent_station,
                "direct_id_in_cache": direct_id_match,
                "parent_id_in_cache": parent_id_match,
                "nearest_mvg_within_radius": [
                    {"distance_m": round(d, 1), "station_id": sid, "station_name": name}
                    for d, sid, name in nearest[:10]
                ],
            }
        )

    return {
        "query": stop_query,
        "radius_m": radius_m,
        "matches": results,
    }
