import unittest
import sys
import json
from pathlib import Path

# Ensure src/ is importable
SYS_PATH_ADDED = str(Path(__file__).resolve().parents[1] / "src")
if SYS_PATH_ADDED not in sys.path:
    sys.path.insert(0, SYS_PATH_ADDED)

from track_tram_reliability.stations import write_cache, read_cache  # noqa: E402
from track_tram_reliability.ingest import filter_stations_by_products, insert_departures  # noqa: E402
from track_tram_reliability.models import Station, Departure  # noqa: E402
from track_tram_reliability.db import create_session_maker, init_db  # noqa: E402


class StationsAndIngestTests(unittest.TestCase):
    def setUp(self):
        # Use temporary sqlite DB in data/ for tests
        self.tmp_db = f"sqlite:///{Path(__file__).parent / 'tmp_rovodev_test.db'}"
        init_db(self.tmp_db)

    def tearDown(self):
        # Clean up DB file
        db_path = Path(__file__).parent / 'tmp_rovodev_test.db'
        if db_path.exists():
            db_path.unlink()

    def test_write_and_read_cache(self):
        stations = [
            Station(id="s1", name="Alpha", products=["TRAM"]),
            Station(id="s2", name="Beta", products=["BUS", "TRAM"]),
        ]
        cache_path = Path(__file__).parent / 'tmp_rovodev_stations.json'
        try:
            write_cache(stations, cache_path)
            loaded = read_cache(cache_path)
            self.assertEqual(len(loaded), 2)
            self.assertEqual(loaded[0].id, "s1")
            self.assertEqual(loaded[1].products, ["BUS", "TRAM"])
        finally:
            if cache_path.exists():
                cache_path.unlink()

    def test_filter_stations_by_products(self):
        stations = [
            Station(id="s1", name="Alpha", products=["TRAM"]),
            Station(id="s2", name="Beta", products=["BUS", "TRAM"]),
            Station(id="s3", name="Gamma", products=["UBAHN"]),
            Station(id="s4", name="Delta", products=None),
        ]
        # Only TRAM
        filtered = filter_stations_by_products(stations, {"TRAM"})
        self.assertEqual({s.id for s in filtered}, {"s1", "s2"})
        # BUS or TRAM
        filtered = filter_stations_by_products(stations, {"BUS", "TRAM"})
        self.assertEqual({s.id for s in filtered}, {"s1", "s2"})
        # ALL
        filtered = filter_stations_by_products(stations, {"ALL"})
        self.assertEqual({s.id for s in filtered}, {"s1", "s2", "s3", "s4"})

    def test_insert_departures_dedup(self):
        Session = create_session_maker(self.tmp_db)
        deps = [
            Departure(
                station_id="s1",
                planned_departure_time=1700000000,
                realtime_departure_time=1700000300,
                delay_in_minutes=5,
                transport_type="TRAM",
                label="T17",
                destination="Central",
                cancelled=False,
                platform=None,
                realtime=True,
                fetched_at=1700000000,
            ),
            # Duplicate key
            Departure(
                station_id="s1",
                planned_departure_time=1700000000,
                realtime_departure_time=1700000300,
                delay_in_minutes=5,
                transport_type="TRAM",
                label="T17",
                destination="Central",
                cancelled=False,
                platform=None,
                realtime=True,
                fetched_at=1700000060,
            ),
        ]
        with Session() as session:
            ins, skip = insert_departures(session, deps)
            session.commit()
        self.assertEqual(ins, 1)
        self.assertEqual(skip, 1)


if __name__ == "__main__":
    unittest.main()
