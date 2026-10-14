"""
Spark Structured Streaming job:

  Kafka (kangra.weather.raw)
    -> parse + validate JSON
    -> dedupe on (location, observed_at) with a watermark
    -> raw readings          -> Postgres readings
    -> 1h windows, 15m slide -> Postgres window_stats
    -> threshold checks      -> Postgres alerts

Run:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    streaming/stream_job.py
"""

import os
import sys

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

sys.path.insert(0, os.path.dirname(__file__))
from rules import triggered  # noqa: E402

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "kangra.weather.raw")
CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "/tmp/kangra-checkpoints")
PG = dict(
    host=os.environ.get("PGHOST", "localhost"),
    port=int(os.environ.get("PGPORT", 5432)),
    dbname=os.environ.get("PGDATABASE", "kangra"),
    user=os.environ.get("PGUSER", "kangra"),
    password=os.environ.get("PGPASSWORD", "kangra"),
)

EVENT_SCHEMA = StructType([
    StructField("event_version", IntegerType()),
    StructField("location_id", StringType()),
    StructField("location_name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("elevation_m", DoubleType()),
    StructField("observed_at", StringType()),
    StructField("fetched_at", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("wind_kmh", DoubleType()),
    StructField("weather_code", IntegerType()),
    StructField("pm2_5", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("us_aqi", DoubleType()),
])


def read_events(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")
    ).select("e.*")

    # API times are local IST with no offset; store everything in UTC
    return (
        parsed
        .where(F.col("location_id").isNotNull() & F.col("observed_at").isNotNull())
        .withColumn(
            "event_time",
            F.to_utc_timestamp(F.to_timestamp("observed_at", "yyyy-MM-dd'T'HH:mm"),
                               "Asia/Kolkata"),
        )
        .where(F.col("event_time").isNotNull())
        # Late data up to 2 hours is still accepted; older state is dropped
        .withWatermark("event_time", "2 hours")
        .dropDuplicates(["location_id", "event_time"])
    )


def window_stats(events: DataFrame) -> DataFrame:
    return (
        events
        .groupBy(F.window("event_time", "1 hour", "15 minutes"), "location_id")
        .agg(
            F.count("*").alias("readings"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
            F.min("temperature_c").alias("min_temperature_c"),
            F.round(F.sum("precipitation_mm"), 2).alias("total_precip_mm"),
            F.max("wind_kmh").alias("max_wind_kmh"),
            F.round(F.avg("pm2_5"), 2).alias("avg_pm2_5"),
            F.max("us_aqi").alias("max_us_aqi"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "*",
        )
        .drop("window")
    )


# ---- sinks ---------------------------------------------------------------
# Batches here are small (a few towns x a few windows), so collecting to the
# driver and upserting with psycopg2 is simpler than JDBC and lets us use
# ON CONFLICT for idempotent writes after restarts.

READING_COLS = ["location_id", "location_name", "elevation_m", "event_time",
                "temperature_c", "humidity_pct", "precipitation_mm", "wind_kmh",
                "weather_code", "pm2_5", "pm10", "us_aqi"]

STAT_COLS = ["location_id", "window_start", "window_end", "readings",
             "avg_temperature_c", "min_temperature_c", "total_precip_mm",
             "max_wind_kmh", "avg_pm2_5", "max_us_aqi"]


def write_readings(batch: DataFrame, batch_id: int):
    rows = [tuple(r[c] for c in READING_COLS) for r in batch.select(READING_COLS).collect()]
    if not rows:
        return
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO readings ({", ".join(READING_COLS)}) VALUES %s
            ON CONFLICT (location_id, event_time) DO NOTHING
        """, rows)


def write_stats_and_alerts(batch: DataFrame, batch_id: int):
    records = [r.asDict() for r in batch.select(STAT_COLS).collect()]
    if not records:
        return

    alerts = []
    for rec in records:
        for alert_type, severity in triggered(rec):
            alerts.append((rec["location_id"], rec["window_start"], rec["window_end"],
                           alert_type, severity))

    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in STAT_COLS[3:])
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO window_stats ({", ".join(STAT_COLS)}) VALUES %s
            ON CONFLICT (location_id, window_start) DO UPDATE SET {updates}
        """, [tuple(rec[c] for c in STAT_COLS) for rec in records])
        if alerts:
            execute_values(cur, """
                INSERT INTO alerts (location_id, window_start, window_end, alert_type, severity)
                VALUES %s
                ON CONFLICT (location_id, window_start, alert_type) DO NOTHING
            """, alerts)


def main():
    spark = (
        SparkSession.builder.appName("kangra-weather-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = read_events(spark)

    readings_q = (
        events.writeStream.foreachBatch(write_readings)
        .option("checkpointLocation", f"{CHECKPOINT}/readings")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )

    stats_q = (
        window_stats(events).writeStream.foreachBatch(write_stats_and_alerts)
        .option("checkpointLocation", f"{CHECKPOINT}/window_stats")
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )

    spark.streams.awaitAnyTermination()
    readings_q.stop()
    stats_q.stop()


if __name__ == "__main__":
    main()
