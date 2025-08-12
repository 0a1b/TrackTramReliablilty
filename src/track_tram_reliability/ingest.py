from __future__ import annotations

from typing import Iterable, List, Optional, Set, Tuple
from sqlalchemy.exc import IntegrityError
from concurrent.futures import ThreadPoolExecutor, as_completed

from .stations import read_cache, DEFAULT_CACHE
from .models import Station, Departure
from .departures import fetch_departures
from .db import create_session_maker, StationOrm, DepartureRawOrm, init_db

ALLOWED_PRODUCTS: Set[str] = {"UBAHN", "SBAHN", "BUS", "TRAM"}


def _norm_label(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    return str(val).strip().upper()


def _norm_name(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    return str(val).strip().lower()


def filter_stations_by_products(stations: Iterable[Station], products: Optional[Set[str]]) -> List[Station]:
    if not products or "ALL" in products:
        return list(stations)
    products = {p.upper() for p in products}
    out: List[Station] = []
    for s in stations:
        if not s.products:
            continue
        s_prods = {p.upper() for p in s.products}
        if s_prods & products:
            out.append(s)
    return out


def sync_stations_from_cache_to_db(db_url: str, cache_path=DEFAULT_CACHE) -> int:
    """Upsert stations from cache into DB. Returns number of stations processed."""
    stations = read_cache(cache_path)
    init_db(db_url)  # ensure schema exists
    Session = create_session_maker(db_url)
    count = 0
    with Session() as session:
        for s in stations:
            orm = session.get(StationOrm, s.id)
            if orm is None:
                orm = StationOrm(station_id=s.id)
                session.add(orm)
            orm.name = s.name
            orm.place = s.place
            orm.latitude = s.latitude
            orm.longitude = s.longitude
            orm.diva_id = s.diva_id
            orm.tariff_zones = s.tariff_zones
            orm.products = s.products  # JSON column supports list
            count += 1
        session.commit()
    return count


def insert_departures(session, departures: List[Departure]) -> Tuple[int, int]:
    """Insert departures; returns (inserted_count, skipped_duplicates)."""
    inserted = 0
    skipped = 0
    for d in departures:
        rec = DepartureRawOrm(
            station_id=d.station_id,
            transport_type=d.transport_type,
            label=d.label,
            destination=d.destination,
            planned_departure_time=d.planned_departure_time,
            realtime_departure_time=d.realtime_departure_time,
            delay_in_minutes=d.delay_in_minutes,
            cancelled=d.cancelled,
            platform=d.platform,
            realtime=d.realtime,
            fetched_at=d.fetched_at,
        )
        session.add(rec)
        try:
            session.flush()
            inserted += 1
        except IntegrityError:
            session.rollback()
            skipped += 1
    return inserted, skipped


def ingest_departures_for_products(
    db_url: str,
    cache_path=DEFAULT_CACHE,
    products: Optional[Set[str]] = None,
    labels: Optional[Set[str]] = None,
    station_names: Optional[Set[str]] = None,
    station_ids: Optional[Set[str]] = None,
    max_workers: int = 8,
) -> Tuple[int, int, int]:
    """Ingest departures for all stations filtered by products, optionally filter by labels.

    Args:
        db_url: Database URL
        cache_path: Path to stations cache
        products: Transport product types to include (e.g., {"BUS", "TRAM"}) or {"ALL"}
        labels: Optional set of normalized line labels to include (e.g., {"53", "164", "X30", "T17"}).
                Note: labels are compared case-insensitively after stripping; numbers are matched as strings.

    Returns:
        (stations_processed, rows_inserted, rows_skipped)
    """
    stations = read_cache(cache_path)
    filtered = filter_stations_by_products(stations, products)

    # Apply station filters if provided
    if station_names:
        name_set = {_norm_name(x) for x in station_names}
        filtered = [s for s in filtered if _norm_name(s.name) in name_set]
    if station_ids:
        id_set = {x.strip() for x in station_ids}
        filtered = [s for s in filtered if s.id in id_set]

    init_db(db_url)
    Session = create_session_maker(db_url)

    # Ensure stations exist in DB
    sync_stations_from_cache_to_db(db_url, cache_path)

    stations_processed = 0
    rows_inserted = 0
    rows_skipped = 0

    norm_labels = {_norm_label(x) for x in labels} if labels else None

    # Fetch departures concurrently to speed up ingestion
    def _fetch_for_station(station_id: str):
        deps = fetch_departures(station_id)
        if norm_labels is not None:
            deps = [d for d in deps if _norm_label(d.label) in norm_labels]
        return station_id, deps

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_for_station, s.id) for s in filtered]
        results = []
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception:
                # Skip failures; continue others
                continue

    with Session() as session:
        for station_id, deps in results:
            ins, skip = insert_departures(session, deps)
            rows_inserted += ins
            rows_skipped += skip
            stations_processed += 1
        session.commit()

    return stations_processed, rows_inserted, rows_skipped
