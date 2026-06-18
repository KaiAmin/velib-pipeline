"""
dags/velib_pipeline.py
─────────────────────────────────────────────────────────────────
Airflow DAG — Vélib' Pipeline Orchestration

Schedule: every 5 minutes
Tasks:
  1. check_api          — HTTP sensor on Paris Open Data
  2. run_etl_ingest     — batch ETL (extract + transform + load)
  3. refresh_agg_arrond — ELT: recompute arrondissement aggregation
  4. refresh_hourly     — ELT: recompute hourly trend
  5. check_data_quality — basic row-count data quality check

All tasks have retries=2 and email_on_failure=False (no SMTP needed).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
import logging
import sys
import os

sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     5432,
    "dbname":   "velib",
    "user":     "airflow",
    "password": "airflow",
}

default_args = {
    "owner":            "velib-pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="velib_pipeline",
    default_args=default_args,
    description="End-to-end Vélib' data pipeline",
    schedule_interval="*/5 * * * *",    # every 5 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["velib", "etl", "streaming"],
) as dag:

    # ── Task 1: Check API is reachable ─────────────────────────────
    def check_api_health(**context):
        url = (
            "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
            "velib-disponibilite-en-temps-reel/"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        log.info(f"API reachable — status {resp.status_code}")

    check_api = PythonOperator(
        task_id="check_api",
        python_callable=check_api_health,
    )

    # ── Task 2: Run batch ETL ──────────────────────────────────────
    def run_etl(**context):
        from etl.ingest import run_etl
        run_etl()

    etl_ingest = PythonOperator(
        task_id="run_etl_ingest",
        python_callable=run_etl,
    )

    # ── Task 3: Refresh arrondissement aggregation ─────────────────
    def refresh_arrond(**context):
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            sql = """
                INSERT INTO agg_availability_by_arrondissement
                    (arrondissement, snapshot_time, total_stations, total_bikes,
                     total_docks, total_ebikes, avg_fill_rate,
                     empty_stations, full_stations)
                SELECT
                    s.arrondissement,
                    DATE_TRUNC('minute', NOW()),
                    COUNT(DISTINCT s.station_code),
                    SUM(st.num_bikes_available),
                    SUM(st.num_docks_available),
                    SUM(st.num_ebikes),
                    ROUND(AVG(
                        100.0 * st.num_bikes_available / NULLIF(s.capacity,0)
                    )::numeric, 1),
                    COUNT(*) FILTER (WHERE st.num_bikes_available = 0),
                    COUNT(*) FILTER (WHERE st.num_docks_available = 0)
                FROM raw_stations s
                JOIN (
                    SELECT DISTINCT ON (station_code)
                        station_code, num_bikes_available,
                        num_docks_available, num_ebikes, ingested_at
                    FROM raw_station_status
                    ORDER BY station_code, ingested_at DESC
                ) st ON s.station_code = st.station_code
                WHERE s.arrondissement IS NOT NULL
                GROUP BY s.arrondissement
                ON CONFLICT (arrondissement, snapshot_time) DO UPDATE SET
                    total_stations = EXCLUDED.total_stations,
                    total_bikes    = EXCLUDED.total_bikes,
                    total_docks    = EXCLUDED.total_docks,
                    total_ebikes   = EXCLUDED.total_ebikes,
                    avg_fill_rate  = EXCLUDED.avg_fill_rate,
                    empty_stations = EXCLUDED.empty_stations,
                    full_stations  = EXCLUDED.full_stations
            """
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            log.info("Refreshed agg_availability_by_arrondissement")
        finally:
            conn.close()

    refresh_agg = PythonOperator(
        task_id="refresh_agg_arrondissement",
        python_callable=refresh_arrond,
    )

    # ── Task 4: Refresh hourly trend ───────────────────────────────
    def refresh_hourly(**context):
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            sql = """
                INSERT INTO agg_hourly_trend
                    (station_code, hour_bucket, avg_bikes, avg_docks,
                     min_bikes, max_bikes, snapshots_count)
                SELECT
                    station_code,
                    DATE_TRUNC('hour', ingested_at),
                    ROUND(AVG(num_bikes_available)::numeric, 2),
                    ROUND(AVG(num_docks_available)::numeric, 2),
                    MIN(num_bikes_available),
                    MAX(num_bikes_available),
                    COUNT(*)
                FROM raw_station_status
                WHERE ingested_at > NOW() - INTERVAL '24 hours'
                GROUP BY station_code, DATE_TRUNC('hour', ingested_at)
                ON CONFLICT (station_code, hour_bucket) DO UPDATE SET
                    avg_bikes       = EXCLUDED.avg_bikes,
                    avg_docks       = EXCLUDED.avg_docks,
                    min_bikes       = EXCLUDED.min_bikes,
                    max_bikes       = EXCLUDED.max_bikes,
                    snapshots_count = EXCLUDED.snapshots_count
            """
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            log.info("Refreshed agg_hourly_trend")
        finally:
            conn.close()

    refresh_trend = PythonOperator(
        task_id="refresh_hourly_trend",
        python_callable=refresh_hourly,
    )

    # ── Task 5: Data quality check ─────────────────────────────────
    def data_quality_check(**context):
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            checks = {
                "raw_stations":      "SELECT COUNT(*) FROM raw_stations",
                "raw_station_status (last hour)": """
                    SELECT COUNT(*) FROM raw_station_status
                    WHERE ingested_at > NOW() - INTERVAL '1 hour'
                """,
                "stream_events (last hour)": """
                    SELECT COUNT(*) FROM stream_events
                    WHERE consumed_at > NOW() - INTERVAL '1 hour'
                """,
            }
            with conn.cursor() as cur:
                for label, sql in checks.items():
                    cur.execute(sql)
                    count = cur.fetchone()[0]
                    log.info(f"DQ check [{label}]: {count} rows")
                    if count == 0 and "stations" in label:
                        raise ValueError(f"DQ FAIL: {label} is empty!")
        finally:
            conn.close()

    dq_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    # ── Task dependencies ──────────────────────────────────────────
    check_api >> etl_ingest >> [refresh_agg, refresh_trend] >> dq_check
