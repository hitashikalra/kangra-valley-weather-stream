# Kangra Valley Weather Stream

I'm from Kangra in Himachal Pradesh. The valley climbs from about 700 m at Kangra town to over 2,000 m at McLeod Ganj in a few dozen kilometers, so the weather and air can be completely different from one town to the next. This project streams live weather and air quality for four towns through Kafka and Spark, keeps rolling hourly stats, and raises alerts for bad air, heavy rain, cold snaps, and high wind.

## Architecture

```
Open-Meteo weather + air quality APIs
        |  every 15 min, one call per API for all four towns
        v
producer/producer.py ---------> Kafka topic: kangra.weather.raw
                                 key = town|observation time
        |
        v
streaming/stream_job.py   (Spark Structured Streaming)
  - parse JSON against a fixed schema, drop malformed events
  - convert IST to UTC, 2 hour watermark for late data
  - dedupe on (town, observation time)
  - 1 hour windows sliding every 15 min
        |
        +--> Postgres readings       every reading, idempotent insert
        +--> Postgres window_stats   rolling hourly stats, upserted
        +--> Postgres alerts         threshold breaches per window
        |
        v
Grafana dashboard (provisioned automatically)
```

## Towns

| Town | Approx. elevation |
|---|---|
| Kangra | 730 m |
| Dharamshala | 1,450 m |
| Palampur | 1,470 m |
| McLeod Ganj | 2,080 m |

## Alerts

Defined once in `streaming/rules.py` and evaluated on each hourly window:

| Alert | Fires when |
|---|---|
| unhealthy_air | US AQI above 150 |
| poor_air | US AQI above 100 (suppressed if unhealthy_air fires) |
| heavy_rain | more than 7.5 mm in the hour |
| cold_snap | temperature drops below 2°C |
| high_wind | wind above 40 km/h |

## Design choices

- **Idempotent end to end.** Kafka keys are `town|observation time`, Spark dedupes on the same pair, and every Postgres write uses `ON CONFLICT`. Restarting the producer or replaying the topic doesn't double count anything.
- **Missing values stay null.** If the air quality API skips a value it stays `None` instead of becoming 0, so hourly averages aren't dragged down by gaps.
- **Polling matches the source.** Open-Meteo updates current conditions every 15 minutes, so the producer polls on that cadence and backs off exponentially when the API fails.
- **Small batches, simple sinks.** Each micro-batch is a handful of rows, so `foreachBatch` upserts with psycopg2 instead of JDBC. That's what makes `ON CONFLICT` possible.

## Run it

```bash
docker compose up -d                    # Kafka, Postgres (schema auto-loads), Grafana
pip install -r requirements.txt

python producer/producer.py --once      # one poll to check everything connects
python producer/producer.py &           # then leave it running

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/stream_job.py

pytest                                  # producer and alert rule tests
```

Grafana runs at http://localhost:3000 with a "Kangra Valley Weather" dashboard showing temperature and AQI by town, hourly rainfall, and recent alerts.

## Useful queries

```sql
-- Latest conditions in each town
SELECT location_name, event_time, temperature_c, us_aqi FROM latest_readings;

-- Is the air worse lower in the valley?
SELECT * FROM aqi_by_elevation_daily ORDER BY day DESC, elevation_m;
```

## Repo layout

```
producer/     Open-Meteo client, event shaping, Kafka producer
streaming/    Spark Structured Streaming job and alert rules
sql/          Postgres tables and views
grafana/      datasource and dashboard provisioning
tests/        unit tests for event building and alert rules
```
