from __future__ import annotations

import signal
import time
from dataclasses import dataclass
from typing import Optional, Set

from .config import load_settings
from .ingest import ingest_departures_for_products
from .stations import DEFAULT_CACHE


@dataclass
class PollerConfig:
    db_url: str
    polling_interval_seconds: int
    products: Set[str]
    cache_path: str


def run_poller(
    db_url: str,
    polling_interval_seconds: int,
    products: Set[str],
    cache_path: str = str(DEFAULT_CACHE),
):
    stop_flag = {"stop": False}

    def _handle_sig(signum, frame):
        stop_flag["stop"] = True

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    backoff = 1
    while not stop_flag["stop"]:
        t0 = time.time()
        try:
            stations_processed, rows_inserted, rows_skipped = ingest_departures_for_products(
                db_url=db_url, cache_path=cache_path, products=products
            )
            print(
                f"Ingest ok: stations={stations_processed}, inserted={rows_inserted}, skipped={rows_skipped}"
            )
            backoff = 1  # reset on success
        except Exception as e:
            print(f"Error during ingest: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

        elapsed = time.time() - t0
        sleep_time = max(0.0, polling_interval_seconds - elapsed)
        # Early exit if stop requested
        if stop_flag["stop"]:
            break
        time.sleep(sleep_time)
