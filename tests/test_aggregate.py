import unittest
import sys
from pathlib import Path

# Ensure src/ is importable
SYS_PATH_ADDED = str(Path(__file__).resolve().parents[1] / "src")
if SYS_PATH_ADDED not in sys.path:
    sys.path.insert(0, SYS_PATH_ADDED)

from track_tram_reliability.db import create_session_maker, init_db, DepartureRawOrm  # noqa: E402
from track_tram_reliability.aggregate import compute_line_metrics, compute_station_metrics  # noqa: E402


class AggregateTests(unittest.TestCase):
    def setUp(self):
        self.tmp_db = f"sqlite:///{Path(__file__).parent / 'tmp_rovodev_agg.db'}"
        init_db(self.tmp_db)
        Session = create_session_maker(self.tmp_db)
        with Session() as session:
            # Insert sample rows for two dates
            def add(station_id, transport_type, label, dest, planned, real, delay, cancelled, fetched_at):
                r = DepartureRawOrm(
                    station_id=station_id,
                    transport_type=transport_type,
                    label=label,
                    destination=dest,
                    planned_departure_time=planned,
                    realtime_departure_time=real,
                    delay_in_minutes=delay,
                    cancelled=cancelled,
                    platform=None,
                    realtime=True,
                    fetched_at=fetched_at,
                )
                session.add(r)

            # Day 1 (2023-11-14 UTC ~ 1700000000)
            add("s1", "TRAM", "T1", "A", 1700000000, 1700000300, 5, False, 1700000000)
            add("s1", "TRAM", "T1", "A", 1700000600, 1700000660, 1, True, 1700000600)
            add("s2", "BUS", "B2", "B", 1700001200, 1700001800, 10, False, 1700001200)
            # Day 2
            add("s1", "TRAM", "T1", "A", 1700086400, 1700086700, 5, False, 1700086400)
            session.commit()

    def tearDown(self):
        db_path = Path(__file__).parent / 'tmp_rovodev_agg.db'
        if db_path.exists():
            db_path.unlink()

    def test_compute_line_metrics(self):
        rows = compute_line_metrics(self.tmp_db)
        # Expect entries for TRAM T1 A for two dates and BUS B2 B
        keys = {(r['date'], r['transport_type'], r['label'], r['destination']) for r in rows}
        self.assertTrue(any(k[1] == 'TRAM' and k[2] == 'T1' for k in keys))
        self.assertTrue(any(k[1] == 'BUS' and k[2] == 'B2' for k in keys))
        # Check cancellation rate for day 1 TRAM T1 A: 1 cancelled out of 2 => 0.5
        day1 = [r for r in rows if r['transport_type']=='TRAM' and r['label']=='T1' and r['date']=='2023-11-14']
        if day1:
            self.assertAlmostEqual(day1[0]['cancellation_rate'], 0.5, places=3)

    def test_compute_station_metrics(self):
        rows = compute_station_metrics(self.tmp_db)
        s1_rows = [r for r in rows if r['station_id']=='s1']
        self.assertGreaterEqual(len(s1_rows), 2)


if __name__ == "__main__":
    unittest.main()
