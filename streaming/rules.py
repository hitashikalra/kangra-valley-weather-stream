"""
Alert thresholds, defined once and used by both the Spark job and the tests.

AQI bands follow the US EPA scale. The rain threshold is roughly where
hourly rainfall starts causing trouble on hill roads during monsoon.
"""

ALERT_RULES = [
    # (alert_type, column, threshold, severity)
    ("unhealthy_air", "max_us_aqi",       150, "high"),
    ("poor_air",      "max_us_aqi",       100, "medium"),
    ("heavy_rain",    "total_precip_mm",  7.5, "high"),
    ("cold_snap",     "min_temperature_c", 2,  "medium"),   # fires when BELOW
    ("high_wind",     "max_wind_kmh",     40,  "medium"),
]

BELOW_RULES = {"cold_snap"}


def triggered(window: dict) -> list[tuple[str, str]]:
    """
    Plain-Python version of the alert check for one aggregated window.
    Returns [(alert_type, severity)]. Only the most severe air alert fires.
    """
    hits = []
    for alert_type, col, threshold, severity in ALERT_RULES:
        value = window.get(col)
        if value is None:
            continue
        fired = value < threshold if alert_type in BELOW_RULES else value > threshold
        if fired:
            hits.append((alert_type, severity))

    if any(a == "unhealthy_air" for a, _ in hits):
        hits = [h for h in hits if h[0] != "poor_air"]
    return hits
