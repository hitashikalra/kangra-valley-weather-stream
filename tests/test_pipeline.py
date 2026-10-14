import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "producer"))
sys.path.insert(0, str(ROOT / "streaming"))

from openmeteo import LOCATIONS, build_events, event_key  # noqa: E402
from rules import triggered  # noqa: E402

FETCHED = datetime(2026, 10, 14, 13, 0, tzinfo=timezone.utc)


def weather_payload(temp=14.2, time="2026-10-14T18:30"):
    return {"elevation": 1457.0, "current": {
        "time": time, "temperature_2m": temp, "relative_humidity_2m": 71,
        "precipitation": 0.4, "wind_speed_10m": 9.7, "weather_code": 61}}


def air_payload(aqi=58):
    return {"current": {"time": "2026-10-14T18:30", "pm2_5": 14.1, "pm10": 22.0, "us_aqi": aqi}}


def test_build_events_merges_weather_and_air_per_town():
    n = len(LOCATIONS)
    events = build_events([weather_payload()] * n, [air_payload()] * n, FETCHED)

    assert len(events) == n
    e = events[0]
    assert e["location_id"] == LOCATIONS[0]["id"]
    assert e["temperature_c"] == 14.2
    assert e["us_aqi"] == 58
    assert e["observed_at"] == "2026-10-14T18:30"


def test_missing_values_stay_none_not_zero():
    n = len(LOCATIONS)
    air = [{"current": {"time": "2026-10-14T18:30"}}] * n
    events = build_events([weather_payload()] * n, air, FETCHED)
    assert events[0]["us_aqi"] is None


def test_rows_without_observation_time_are_skipped():
    n = len(LOCATIONS)
    weather = [weather_payload(time=None)] + [weather_payload()] * (n - 1)
    events = build_events(weather, [air_payload()] * n, FETCHED)
    assert len(events) == n - 1


def test_location_count_mismatch_raises():
    with pytest.raises(ValueError):
        build_events([weather_payload()], [air_payload()], FETCHED)


def test_event_key_is_stable_for_replays():
    n = len(LOCATIONS)
    a = build_events([weather_payload()] * n, [air_payload()] * n, FETCHED)
    b = build_events([weather_payload()] * n, [air_payload()] * n,
                     datetime(2026, 10, 14, 13, 5, tzinfo=timezone.utc))
    assert event_key(a[0]) == event_key(b[0])


@pytest.mark.parametrize("window, expected", [
    ({"max_us_aqi": 60, "total_precip_mm": 1.0, "min_temperature_c": 12, "max_wind_kmh": 10}, set()),
    ({"max_us_aqi": 120}, {"poor_air"}),
    ({"max_us_aqi": 180}, {"unhealthy_air"}),          # poor_air suppressed
    ({"total_precip_mm": 12.3}, {"heavy_rain"}),
    ({"min_temperature_c": -1.5}, {"cold_snap"}),
    ({"max_wind_kmh": 55, "max_us_aqi": None}, {"high_wind"}),
])
def test_alert_rules(window, expected):
    assert {a for a, _ in triggered(window)} == expected
