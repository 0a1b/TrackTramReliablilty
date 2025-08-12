from __future__ import annotations

from typing import List, Optional
from datetime import datetime, timezone

from .http import create_session
from .models import Departure

DEPARTURES_URL = "https://www.mvg.de/api/bgw-pt/v3/departures"


def _normalize_epoch_seconds(value: Optional[int | float | str]) -> Optional[int]:
    if value is None:
        return None
    # Convert to int if string/float
    try:
        ivalue = int(float(value))
    except Exception:
        return None
    # Handle milliseconds vs seconds
    if ivalue > 10_000_000_000:  # larger than year 2286 in seconds, likely ms
        ivalue //= 1000
    return ivalue


def fetch_departures(station_id: str) -> List[Departure]:
    """Fetch departures for a given MVG global station id.

    Args:
        station_id: e.g., "de:09162:1"

    Returns:
        List of normalized Departure models.
    """
    sess = create_session()
    params = {"globalId": station_id}
    resp = sess.get(DEPARTURES_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    departures: List[Departure] = []
    fetched_at = int(datetime.now(tz=timezone.utc).timestamp())

    for item in data:
        planned = _normalize_epoch_seconds(
            item.get("planned_departure_time") or item.get("plannedDepartureTime")
        )
        realtime = _normalize_epoch_seconds(
            item.get("realtime_departure_time") or item.get("realtimeDepartureTime")
        )
        delay = item.get("delay_in_minutes") or item.get("delayInMinutes")
        if delay is None and planned is not None and realtime is not None:
            delay = int(round((realtime - planned) / 60))

        raw_platform = item.get("platform")
        platform_str = None if raw_platform is None else str(raw_platform)
        dep = Departure(
            station_id=station_id,
            planned_departure_time=planned,
            realtime_departure_time=realtime,
            delay_in_minutes=delay,
            transport_type=item.get("transport_type") or item.get("transportType"),
            label=item.get("label"),
            destination=item.get("destination"),
            cancelled=bool(item.get("cancelled", False)),
            platform=platform_str,
            realtime=bool(item.get("realtime", False)),
            fetched_at=fetched_at,
        )
        departures.append(dep)

    return departures
