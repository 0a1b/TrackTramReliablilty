from __future__ import annotations

from typing import List

from sqlalchemy import select, func, case

from .db import create_session_maker, DepartureRawOrm, epoch_to_date


def compute_line_metrics(db_url: str, days: int = 1) -> List[dict]:
    """Compute reliability metrics per (date, transport_type, label, destination).

    Metrics:
    - count_total: number of rows
    - count_cancelled
    - cancellation_rate
    - avg_delay
    """
    Session = create_session_maker(db_url)
    out: List[dict] = []
    with Session() as session:
        date_col = epoch_to_date(DepartureRawOrm.fetched_at)
        stmt = (
            select(
                date_col.label("date"),
                DepartureRawOrm.transport_type,
                DepartureRawOrm.label,
                DepartureRawOrm.destination,
                func.count().label("count_total"),
                func.sum(case((DepartureRawOrm.cancelled == True, 1), else_=0)).label(
                    "count_cancelled"
                ),
                func.avg(DepartureRawOrm.delay_in_minutes).label("avg_delay"),
            )
            .group_by(
                date_col,
                DepartureRawOrm.transport_type,
                DepartureRawOrm.label,
                DepartureRawOrm.destination,
            )
            .order_by(date_col)
        )
        rows = session.execute(stmt).all()
        for r in rows:
            date, transport_type, label, destination, count_total, count_cancelled, avg_delay = r
            cancellation_rate = (count_cancelled or 0) / count_total if count_total else 0.0
            out.append(
                {
                    "date": date,
                    "transport_type": transport_type,
                    "label": label,
                    "destination": destination,
                    "count_total": int(count_total or 0),
                    "count_cancelled": int(count_cancelled or 0),
                    "cancellation_rate": float(cancellation_rate),
                    "avg_delay": float(avg_delay or 0.0),
                }
            )
    return out


def compute_station_metrics(db_url: str) -> List[dict]:
    """Compute reliability metrics per (date, station_id)."""
    Session = create_session_maker(db_url)
    out: List[dict] = []
    with Session() as session:
        date_col = epoch_to_date(DepartureRawOrm.fetched_at)
        stmt = (
            select(
                date_col.label("date"),
                DepartureRawOrm.station_id,
                func.count().label("count_total"),
                func.sum(case((DepartureRawOrm.cancelled == True, 1), else_=0)).label(
                    "count_cancelled"
                ),
                func.avg(DepartureRawOrm.delay_in_minutes).label("avg_delay"),
            )
            .group_by(date_col, DepartureRawOrm.station_id)
            .order_by(date_col)
        )
        rows = session.execute(stmt).all()
        for r in rows:
            date, station_id, count_total, count_cancelled, avg_delay = r
            cancellation_rate = (count_cancelled or 0) / count_total if count_total else 0.0
            out.append(
                {
                    "date": date,
                    "station_id": station_id,
                    "count_total": int(count_total or 0),
                    "count_cancelled": int(count_cancelled or 0),
                    "cancellation_rate": float(cancellation_rate),
                    "avg_delay": float(avg_delay or 0.0),
                }
            )
    return out
