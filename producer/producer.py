"""
Poll Open-Meteo every 15 minutes and publish one event per town to Kafka.

Open-Meteo refreshes "current" values every 15 minutes, so polling faster
only produces duplicates. Duplicates that do slip through (restarts, retries)
share a Kafka key and get dropped by the streaming job.

Usage:
  python producer/producer.py                 # runs forever
  python producer/producer.py --once          # one poll, handy for testing
"""

import argparse
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

from openmeteo import build_events, event_key, fetch

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "kangra.weather.raw")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", 900))

log = logging.getLogger("producer")
running = True


def stop(*_):
    global running
    running = False
    log.info("shutting down after current poll")


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",          # wait for the broker to persist before moving on
        retries=5,
        linger_ms=50,
    )


def poll_once(session: requests.Session, producer: KafkaProducer) -> int:
    weather, air = fetch(session)
    events = build_events(weather, air, datetime.now(timezone.utc))
    for e in events:
        producer.send(TOPIC, key=event_key(e), value=e)
    producer.flush()
    return len(events)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    session = requests.Session()
    producer = make_producer()
    failures = 0

    while running:
        try:
            n = poll_once(session, producer)
            failures = 0
            log.info("published %s events to %s", n, TOPIC)
        except Exception as exc:  # noqa: BLE001
            failures += 1
            log.error("poll failed (%s in a row): %s", failures, exc)

        if args.once:
            break
        # back off on repeated failures, capped at an hour
        wait = POLL_SECONDS if failures == 0 else min(60 * 2 ** failures, 3600)
        for _ in range(wait):
            if not running:
                break
            time.sleep(1)

    producer.close()


if __name__ == "__main__":
    main()
