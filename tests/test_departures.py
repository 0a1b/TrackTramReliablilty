import unittest
import sys
from pathlib import Path

# Ensure src/ is importable when running tests without installation
SYS_PATH_ADDED = str(Path(__file__).resolve().parents[1] / "src")
if SYS_PATH_ADDED not in sys.path:
    sys.path.insert(0, SYS_PATH_ADDED)

from track_tram_reliability import models  # noqa: E402
from track_tram_reliability import departures  # noqa: E402


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload):
        self._payload = payload

    def get(self, url, params=None, timeout=10):
        return FakeResponse(self._payload)


class DeparturesTests(unittest.TestCase):
    def test_normalize_epoch_seconds(self):
        self.assertIsNone(departures._normalize_epoch_seconds(None))
        self.assertEqual(departures._normalize_epoch_seconds(1700000000), 1700000000)
        # Milliseconds converted to seconds
        self.assertEqual(departures._normalize_epoch_seconds(1700000000000), 1700000000)
        # Strings converted
        self.assertEqual(departures._normalize_epoch_seconds("1700000000"), 1700000000)

    def test_fetch_departures_computes_delay(self):
        planned = 1_700_000_000
        realtime = planned + 5 * 60  # +5 minutes
        payload = [
            {
                "planned_departure_time": planned,
                "realtime_departure_time": realtime,
                "delay_in_minutes": None,
                "transport_type": "TRAM",
                "label": "T17",
                "destination": "Central",
                "cancelled": False,
                "platform": None,
                "realtime": True,
            }
        ]

        # Monkeypatch create_session to return our fake session
        orig_create_session = departures.create_session
        try:
            departures.create_session = lambda: FakeSession(payload)
            res = departures.fetch_departures("de:fake:1")
        finally:
            departures.create_session = orig_create_session

        self.assertEqual(len(res), 1)
        d = res[0]
        self.assertEqual(d.delay_in_minutes, 5)
        self.assertEqual(d.transport_type, "TRAM")
        self.assertEqual(d.label, "T17")
        self.assertEqual(d.destination, "Central")


if __name__ == "__main__":
    unittest.main()
