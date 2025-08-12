from __future__ import annotations

import csv
import io
import json
import math
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .http import create_session
from .stations import read_cache, DEFAULT_CACHE
from .models import Station

GTFS_DEFAULT_URL = "https://www.mvg.de/static/gtfs/google_transit.zip"

ROUTE_TYPE_TO_PRODUCT = {
    "0": "TRAM",
    "1": "UBAHN",
    "2": "SBAHN",
    "3": "BUS",
    "900": "TRAM",  # Munich-specific: regular tram routes
}


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


@dataclass
class GtfsIndex:
    # mapping: product -> label -> list of station_ids
    mapping: Dict[str, Dict[str, List[str]]]
    source: str

    def to_json(self) -> str:
        return json.dumps({"mapping": self.mapping, "source": self.source}, ensure_ascii=False, indent=2)

    @staticmethod
    def from_json(text: str) -> "GtfsIndex":
        obj = json.loads(text)
        return GtfsIndex(mapping=obj.get("mapping", {}), source=obj.get("source", ""))


def _read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
    with zf.open(name) as f:
        data = f.read()
    # Use utf-8-sig to strip BOM if present
    sio = io.StringIO(data.decode("utf-8-sig"))
    reader = csv.DictReader(sio)
    rows = []
    for row in reader:
        # Normalize keys to strip BOM and whitespace
        norm = {str(k).lstrip("\ufeff").strip(): v for k, v in row.items()}
        rows.append(norm)
    return rows


def _resp_bytes(resp) -> bytes:
    if hasattr(resp, "content"):
        return resp.content  # requests.Response
    if hasattr(resp, "_body"):
        return resp._body  # our shim
    # Last resort: try .read()
    if hasattr(resp, "read"):
        return resp.read()
    raise TypeError("Unknown response object; cannot extract bytes")


def _download_bytes(url: str) -> bytes:
    sess = create_session()
    resp = sess.get(url, timeout=60)
    resp.raise_for_status()
    return _resp_bytes(resp)


def _open_zip_from_source(source: str | Path) -> zipfile.ZipFile:
    p = Path(str(source))
    if p.exists():
        return zipfile.ZipFile(p)
    # Otherwise treat as URL
    sess = create_session()
    resp = sess.get(str(source), timeout=60)
    resp.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(_resp_bytes(resp)))


def build_label_index(
    gtfs_source: str | Path = GTFS_DEFAULT_URL,
    products: Optional[Set[str]] = None,
    labels: Optional[Set[str]] = None,
    stations_cache: Path | None = None,
    distance_threshold_m: float = 150.0,
) -> GtfsIndex:
    """Build a mapping from (product, label) -> list of MVG station_ids using GTFS + stations cache.

    - products: set like {"TRAM", "BUS"}. If None, include all.
    - labels: route_short_name values to include (normalize to upper()). If None, include all.
    """
    products = {p.upper() for p in products} if products else None
    labels = {str(l).strip().upper() for l in labels} if labels else None

    with _open_zip_from_source(gtfs_source) as zf:
        routes = _read_csv_from_zip(zf, "routes.txt")
        trips = _read_csv_from_zip(zf, "trips.txt")
        stop_times = _read_csv_from_zip(zf, "stop_times.txt")
        stops = _read_csv_from_zip(zf, "stops.txt")

    # Filter routes by products & labels
    route_ids: Set[str] = set()
    route_product: Dict[str, str] = {}
    for r in routes:
        r_type = ROUTE_TYPE_TO_PRODUCT.get(r.get("route_type", ""))
        r_label = (r.get("route_short_name") or "").strip().upper()
        if products and (r_type not in products):
            continue
        if labels and (r_label not in labels):
            continue
        rid = r.get("route_id")
        if not rid:
            continue
        route_ids.add(rid)
        if r_type:
            route_product[rid] = r_type

    # Trips for those routes
    route_trips: Dict[str, Set[str]] = {}
    for t in trips:
        rid = t.get("route_id")
        if rid not in route_ids:
            continue
        trip_id = t.get("trip_id")
        if not trip_id:
            continue
        route_trips.setdefault(rid, set()).add(trip_id)

    # Stops used by those trips
    trip_stops: Dict[str, Set[str]] = {}
    for st in stop_times:
        trip_id = st.get("trip_id")
        stop_id = st.get("stop_id")
        if trip_id and stop_id:
            trip_stops.setdefault(trip_id, set()).add(stop_id)

    route_stops: Dict[str, Set[str]] = {}
    for rid, tids in route_trips.items():
        all_stops = set()
        for tid in tids:
            all_stops |= trip_stops.get(tid, set())
        route_stops[rid] = all_stops

    # Build mapping product -> label -> station_ids by matching first 3 colon-separated parts (base3)
    stations_cache = stations_cache or DEFAULT_CACHE
    stations = read_cache(stations_cache)

    def base3(x: str) -> str:
        parts = x.split(":")
        return ":".join(parts[:3]) if len(parts) >= 3 else x

    # Map base3 -> set of full station_ids in cache
    station_base3_map: Dict[str, Set[str]] = {}
    for s in stations:
        key = base3(s.id)
        station_base3_map.setdefault(key, set()).add(s.id)

    mapping: Dict[str, Dict[str, List[str]]] = {}

    # Build stop lookup including parent_station mapping
    stop_lookup: Dict[str, Dict[str, str]] = {s.get("stop_id"): s for s in stops if s.get("stop_id")}

    for rid, sids in route_stops.items():
        prod = route_product.get(rid)
        # Fetch label
        r = next((x for x in routes if x.get("route_id") == rid), None)
        if r is None:
            continue
        label = (r.get("route_short_name") or "").strip().upper()
        if not label or not prod:
            continue
        base3_keys: Set[str] = set()
        for sid in sids:
            sinfo = stop_lookup.get(sid)
            if not sinfo:
                continue
            parent = (sinfo.get("parent_station") or "").strip()
            use_id = parent if parent else sid
            base3_keys.add(base3(use_id))
        # Expand base3 keys into actual station_ids present in the cache
        selected_ids: Set[str] = set()
        for k in base3_keys:
            selected_ids |= station_base3_map.get(k, set())
        # Store under specific product (if available) and under ALL, unioning across routes
        # Specific product
        if prod:
            curr = set(mapping.setdefault(prod, {}).get(label, []))
            mapping[prod][label] = sorted(curr | set(selected_ids))
        # ALL bucket
        curr_all = set(mapping.setdefault("ALL", {}).get(label, []))
        mapping["ALL"][label] = sorted(curr_all | set(selected_ids))

    return GtfsIndex(mapping=mapping, source=str(gtfs_source))


def write_label_index(index: GtfsIndex, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(index.to_json(), encoding="utf-8")


def load_label_index(path: Path) -> GtfsIndex:
    return GtfsIndex.from_json(Path(path).read_text(encoding="utf-8"))
