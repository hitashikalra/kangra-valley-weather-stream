"""
Open-Meteo client and event shaping.

Kept separate from the Kafka loop so the parsing logic can be unit tested
without a broker or network.
"""

from datetime import datetime

import requests

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

WEATHER_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]
AIR_VARS = ["pm2_5", "pm10", "us_aqi"]

# Towns across Kangra district, lowest to highest elevation
LOCATIONS = [
    {"id": "kangra",     "name": "Kangra",     "lat": 32.0998, "lon": 76.2691},
    {"id": "dharamshala", "name": "Dharamshala", "lat": 32.2190, "lon": 76.3234},
    {"id": "palampur",   "name": "Palampur",   "lat": 32.1109, "lon": 76.5363},
    {"id": "mcleod_ganj", "name": "McLeod Ganj", "lat": 32.2426, "lon": 76.3213},
]

EVENT_VERSION = 1


def _params(variables: list[str]) -> dict:
    return {
        "latitude": ",".join(str(l["lat"]) for l in LOCATIONS),
        "longitude": ",".join(str(l["lon"]) for l in LOCATIONS),
        "current": ",".join(variables),
        "timezone": "Asia/Kolkata",
    }


def fetch(session: requests.Session) -> tuple[list[dict], list[dict]]:
    """One call per API covers every location. Returns raw per-location lists."""
    weather = session.get(WEATHER_URL, params=_params(WEATHER_VARS), timeout=20)
    weather.raise_for_status()
    air = session.get(AIR_URL, params=_params(AIR_VARS), timeout=20)
    air.raise_for_status()
    return _as_list(weather.json()), _as_list(air.json())


def _as_list(payload) -> list[dict]:
    # Open-Meteo returns an object for one location and a list for several
    return payload if isinstance(payload, list) else [payload]


def build_events(weather: list[dict], air: list[dict], fetched_at: datetime) -> list[dict]:
    """
    Merge weather + air quality per location into one flat event.
    Locations come back in the same order they were requested.
    Missing values stay None rather than becoming 0, so downstream
    averages aren't dragged toward zero by gaps.
    """
    if len(weather) != len(LOCATIONS) or len(air) != len(LOCATIONS):
        raise ValueError(
            f"expected {len(LOCATIONS)} locations, got weather={len(weather)} air={len(air)}"
        )

    events = []
    for loc, w, a in zip(LOCATIONS, weather, air):
        wc, ac = w.get("current") or {}, a.get("current") or {}
        observed_at = wc.get("time")
        if not observed_at:
            continue
        events.append({
            "event_version": EVENT_VERSION,
            "location_id": loc["id"],
            "location_name": loc["name"],
            "lat": loc["lat"],
            "lon": loc["lon"],
            "elevation_m": w.get("elevation"),
            # local IST time from the API, e.g. "2026-10-14T18:45"
            "observed_at": observed_at,
            "fetched_at": fetched_at.isoformat(timespec="seconds"),
            "temperature_c": wc.get("temperature_2m"),
            "humidity_pct": wc.get("relative_humidity_2m"),
            "precipitation_mm": wc.get("precipitation"),
            "wind_kmh": wc.get("wind_speed_10m"),
            "weather_code": wc.get("weather_code"),
            "pm2_5": ac.get("pm2_5"),
            "pm10": ac.get("pm10"),
            "us_aqi": ac.get("us_aqi"),
        })
    return events


def event_key(event: dict) -> str:
    """Kafka key: same location + observation time = same event (idempotent replays)."""
    return f"{event['location_id']}|{event['observed_at']}"
