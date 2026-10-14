-- Runs automatically when the Postgres container first starts.

CREATE TABLE IF NOT EXISTS readings (
    location_id       TEXT        NOT NULL,
    location_name     TEXT        NOT NULL,
    elevation_m       REAL,
    event_time        TIMESTAMP   NOT NULL,   -- UTC
    temperature_c     REAL,
    humidity_pct      REAL,
    precipitation_mm  REAL,
    wind_kmh          REAL,
    weather_code      INTEGER,
    pm2_5             REAL,
    pm10              REAL,
    us_aqi            REAL,
    ingested_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (location_id, event_time)
);

CREATE TABLE IF NOT EXISTS window_stats (
    location_id        TEXT      NOT NULL,
    window_start       TIMESTAMP NOT NULL,
    window_end         TIMESTAMP NOT NULL,
    readings           INTEGER   NOT NULL,
    avg_temperature_c  REAL,
    min_temperature_c  REAL,
    total_precip_mm    REAL,
    max_wind_kmh       REAL,
    avg_pm2_5          REAL,
    max_us_aqi         REAL,
    updated_at         TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (location_id, window_start)
);

CREATE TABLE IF NOT EXISTS alerts (
    location_id   TEXT      NOT NULL,
    window_start  TIMESTAMP NOT NULL,
    window_end    TIMESTAMP NOT NULL,
    alert_type    TEXT      NOT NULL,
    severity      TEXT      NOT NULL,
    raised_at     TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (location_id, window_start, alert_type)
);

-- Handy for Grafana: latest reading per town
CREATE OR REPLACE VIEW latest_readings AS
SELECT DISTINCT ON (location_id) *
FROM readings
ORDER BY location_id, event_time DESC;

-- Does air get worse as you go down the valley?
CREATE OR REPLACE VIEW aqi_by_elevation_daily AS
SELECT
    date_trunc('day', event_time) AS day,
    location_name,
    elevation_m,
    round(avg(us_aqi)::numeric, 1) AS avg_aqi,
    max(us_aqi)                    AS peak_aqi
FROM readings
GROUP BY 1, 2, 3
ORDER BY 1, 3;
