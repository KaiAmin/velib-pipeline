"""
streaming/producer.py
─────────────────────────────────────────────────────────────────
Kafka Producer — polls Paris Open Data every 60 seconds and
publishes each station snapshot as a JSON message to the
'velib-status' topic.
"""

import os
import json
import time
import logging
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC          = "velib-status"
POLL_INTERVAL  = 60   # seconds
API_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-disponibilite-en-temps-reel/exports/json"
    "?limit=2000&timezone=Europe%2FParis"
)


def wait_for_kafka(servers: str, retries: int = 20, delay: int = 5):
    for i in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            log.info("Kafka is ready ✓")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retry {i+1}/{retries} …")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries")


def fetch_stations() -> list[dict]:
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def build_message(record: dict) -> dict:
    """Flatten and sanitise one station record for Kafka."""
    geo = record.get("coordonnees_geo") or {}
    return {
        "station_code":        str(record.get("stationcode", "")),
        "name":                record.get("name", ""),
        "num_bikes_available": int(record.get("numbikesavailable") or 0),
        "num_docks_available": int(record.get("numdocksavailable") or 0),
        "num_ebikes":          int(record.get("ebike") or 0),
        "num_mechanical":      int(record.get("mechanical") or 0),
        "capacity":            int(record.get("capacity") or 0),
        "is_renting":          bool(record.get("is_renting", True)),
        "lat":                 geo.get("lat"),
        "lon":                 geo.get("lon"),
        "produced_at":         datetime.utcnow().isoformat(),
    }


def classify_event(msg: dict) -> str:
    """Tag each message with a simple event type for the consumer."""
    if msg["num_bikes_available"] == 0:
        return "empty_station"
    if msg["num_bikes_available"] < 3:
        return "low_bikes"
    if msg["num_docks_available"] == 0:
        return "full_station"
    return "snapshot"


def main():
    producer = wait_for_kafka(KAFKA_SERVERS)

    while True:
        try:
            records = fetch_stations()
            sent = 0
            for r in records:
                msg = build_message(r)
                msg["event_type"] = classify_event(msg)

                producer.send(
                    TOPIC,
                    key=msg["station_code"].encode(),
                    value=json.dumps(msg).encode()
                )
                sent += 1

            producer.flush()
            log.info(f"Published {sent} messages to '{TOPIC}'")

        except Exception as e:
            log.error(f"Producer error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
