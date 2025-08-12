from __future__ import annotations

from pathlib import Path
from typing import Optional

from sqlalchemy import (
    Boolean,
    Integer,
    Float,
    String,
    JSON,
    UniqueConstraint,
    create_engine,
    ForeignKey,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from datetime import datetime, timezone


class Base(DeclarativeBase):
    pass


class StationOrm(Base):
    __tablename__ = "stations"

    station_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String)
    place: Mapped[Optional[str]] = mapped_column(String)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    diva_id: Mapped[Optional[int]] = mapped_column(Integer)
    tariff_zones: Mapped[Optional[str]] = mapped_column(String)
    products: Mapped[Optional[dict]] = mapped_column(JSON)
    last_seen_at: Mapped[Optional[int]] = mapped_column(Integer)


class DepartureRawOrm(Base):
    __tablename__ = "departures_raw"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    station_id: Mapped[str] = mapped_column(String, ForeignKey("stations.station_id"), index=True)
    transport_type: Mapped[Optional[str]] = mapped_column(String(16))
    label: Mapped[Optional[str]] = mapped_column(String(32))
    destination: Mapped[Optional[str]] = mapped_column(String)
    planned_departure_time: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    realtime_departure_time: Mapped[Optional[int]] = mapped_column(Integer)
    delay_in_minutes: Mapped[Optional[int]] = mapped_column(Integer)
    cancelled: Mapped[bool] = mapped_column(Boolean, default=False)
    platform: Mapped[Optional[str]] = mapped_column(String(32))
    realtime: Mapped[bool] = mapped_column(Boolean, default=False)
    fetched_at: Mapped[int] = mapped_column(Integer, index=True)

    __table_args__ = (
        UniqueConstraint(
            "station_id",
            "transport_type",
            "label",
            "destination",
            "planned_departure_time",
            name="uq_departure_identity",
        ),
    )


def _ensure_sqlite_path(db_url: str) -> None:
    if db_url.startswith("sqlite:///") and ":memory:" not in db_url:
        path_str = db_url.replace("sqlite:///", "", 1)
        # Expand relative paths and user (~)
        p = Path(path_str).expanduser()
        parent = p.parent
        parent.mkdir(parents=True, exist_ok=True)


def create_engine_for_url(db_url: str):
    _ensure_sqlite_path(db_url)
    return create_engine(db_url, future=True)


def create_session_maker(db_url: str):
    engine = create_engine_for_url(db_url)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db(db_url: str) -> None:
    engine = create_engine_for_url(db_url)
    Base.metadata.create_all(engine)


# Aggregation helpers
from sqlalchemy import select, func

def epoch_to_date(epoch_col):
    """Convert UNIX epoch seconds to date (UTC) using SQLAlchemy functions where possible.
    Fallback: divide and cast to date where dialect supports.
    """
    # For SQLite, use datetime(epoch, 'unixepoch') then date()
    return func.date(func.datetime(epoch_col, 'unixepoch'))
