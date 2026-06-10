"""
streaming/consumer.py
─────────────────────────────────────────────────────────────────
Kafka Consumer — reads from 'velib-status' topic and writes
events to PostgreSQL stream_events table.

Runs continuously. Commit offset only after successful DB insert
to guarantee at-least-once delivery.
"""

import os
import json
import time
import logging
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC         = "velib-status"
GROUP_ID      = "velib-consumer-group"
BATCH_SIZE    = 100   # flush to DB every N messages

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   "velib",
    "user":     "airflow",
    "password": "airflow",
}


def wait_for_kafka(retries: int = 20, delay: int = 5) -> KafkaConsumer:
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode()),
                consumer_timeout_ms=5000,
            )
            log.info("Connected to Kafka ✓")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retry {i+1}/{retries} …")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def insert_events(conn, batch: list[dict]):
    """Bulk-insert a batch of events into stream_events."""
    sql = """
        INSERT INTO stream_events
            (station_code, event_type, num_bikes_available,
             num_docks_available, num_ebikes, payload)
        VALUES %s
    """
    rows = [
        (
            msg["station_code"],
            msg.get("event_type", "snapshot"),
            msg.get("num_bikes_available", 0),
            msg.get("num_docks_available", 0),
            msg.get("num_ebikes", 0),
            json.dumps(msg),
        )
        for msg in batch
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def main():
    consumer = wait_for_kafka()
    conn     = get_db_connection()
    batch: list[dict] = []

    log.info(f"Listening on topic '{TOPIC}' …")

    while True:
        try:
            for message in consumer:
                batch.append(message.value)

                if len(batch) >= BATCH_SIZE:
                    insert_events(conn, batch)
                    consumer.commit()
                    log.info(f"Flushed {len(batch)} events to DB")
                    batch.clear()

        except StopIteration:
            # consumer_timeout_ms elapsed — flush partial batch
            if batch:
                try:
                    insert_events(conn, batch)
                    consumer.commit()
                    log.info(f"Flushed {len(batch)} events (timeout flush)")
                    batch.clear()
                except Exception as e:
                    log.error(f"DB flush error: {e}")
                    conn = get_db_connection()   # reconnect

        except psycopg2.OperationalError:
            log.warning("DB connection lost, reconnecting …")
            time.sleep(3)
            conn = get_db_connection()

        except Exception as e:
            log.error(f"Consumer loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
