from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel


class Station(BaseModel):
    id: str
    name: str
    place: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    diva_id: Optional[int] = None
    tariff_zones: Optional[str] = None
    products: Optional[List[str]] = None


class Departure(BaseModel):
    station_id: str
    planned_departure_time: Optional[int]
    realtime_departure_time: Optional[int]
    delay_in_minutes: Optional[int]
    transport_type: Optional[str]
    label: Optional[str]
    destination: Optional[str]
    cancelled: bool = False
    platform: Optional[str]
    realtime: bool = False
    fetched_at: int  # unix epoch (UTC)
