"""
etl/ingest.py
"""

import os
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

STATIONS_INFO_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-disponibilite-en-temps-reel/exports/json"
    "?limit=2000&timezone=Europe%2FParis"
)

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   "velib",
    "user":     "airflow",
    "password": "airflow",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_velib_data() -> list[dict]:
    log.info("Fetching Vélib' data from Paris Open Data …")
    resp = requests.get(STATIONS_INFO_URL, timeout=30)
    resp.raise_for_status()
    records = resp.json()
    log.info(f"Fetched {len(records)} station records")
    return records


def extract_arrondissement(station_code: str) -> int | None:
    try:
        code = str(station_code).strip()
        if len(code) > 3:
            arrond = int(code[:-3])
            if 1 <= arrond <= 99:
                return arrond
    except (ValueError, TypeError):
        pass
    return None


def transform_record(r: dict) -> tuple[dict, dict]:
    station_code = str(r.get("stationcode", "")).strip()
    name = (r.get("name") or "").strip()
    capacity = int(r.get("capacity") or 0)

    geo = r.get("coordonnees_geo") or {}
    lat = geo.get("lat") or r.get("lat")
    lon = geo.get("lon") or r.get("lon")

    arrond = extract_arrondissement(station_code)

    station_info = {
        "station_code":   station_code,
        "name":           name,
        "capacity":       capacity,
        "lat":            float(lat) if lat else None,
        "lon":            float(lon) if lon else None,
        "arrondissement": arrond,
    }

    raw_ts = r.get("duedate") or r.get("last_reported")
    try:
        last_reported = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
    except Exception:
        last_reported = datetime.utcnow()

    station_status = {
        "station_code":        station_code,
        "num_bikes_available": int(r.get("numbikesavailable") or 0),
        "num_docks_available": int(r.get("numdocksavailable") or 0),
        "num_ebikes":          int(r.get("ebike") or 0),
        "num_mechanical":      int(r.get("mechanical") or 0),
        "is_installed":        bool(r.get("is_installed", True)),
        "is_renting":          bool(r.get("is_renting", True)),
        "is_returning":        bool(r.get("is_returning", True)),
        "last_reported":       last_reported,
    }

    return station_info, station_status


def transform_all(records: list[dict]):
    infos, statuses = [], []
    skipped = 0
    for r in records:
        try:
            info, status = transform_record(r)
            if info["station_code"]:
                infos.append(info)
                statuses.append(status)
        except Exception as e:
            log.warning(f"Skipping record {r.get('stationcode')}: {e}")
            skipped += 1
    log.info(f"Transformed {len(infos)} stations ({skipped} skipped)")
    return infos, statuses


def load_stations(conn, infos: list[dict]):
    if not infos:
        return
    sql = """
        INSERT INTO raw_stations
            (station_code, name, capacity, lat, lon, arrondissement)
        VALUES %s
        ON CONFLICT (station_code) DO UPDATE SET
            name            = EXCLUDED.name,
            capacity        = EXCLUDED.capacity,
            lat             = EXCLUDED.lat,
            lon             = EXCLUDED.lon,
            arrondissement  = EXCLUDED.arrondissement,
            ingested_at     = NOW()
    """
    rows = [
        (i["station_code"], i["name"], i["capacity"],
         i["lat"], i["lon"], i["arrondissement"])
        for i in infos
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    log.info(f"Upserted {len(rows)} station records into raw_stations")


def load_status(conn, statuses: list[dict]):
    if not statuses:
        return
    sql = """
        INSERT INTO raw_station_status
            (station_code, num_bikes_available, num_docks_available,
             num_ebikes, num_mechanical, is_installed, is_renting,
             is_returning, last_reported)
        VALUES %s
        ON CONFLICT (station_code, last_reported) DO NOTHING
    """
    rows = [
        (s["station_code"], s["num_bikes_available"], s["num_docks_available"],
         s["num_ebikes"], s["num_mechanical"], s["is_installed"],
         s["is_renting"], s["is_returning"], s["last_reported"])
        for s in statuses
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    log.info(f"Inserted {len(rows)} status snapshots into raw_station_status")


def refresh_agg_by_arrondissement(conn):
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
                100.0 * st.num_bikes_available / NULLIF(s.capacity, 0)
            )::numeric, 1),
            COUNT(*) FILTER (WHERE st.num_bikes_available = 0),
            COUNT(*) FILTER (WHERE st.num_docks_available = 0)
        FROM raw_stations s
        JOIN (
            SELECT DISTINCT ON (station_code)
                station_code,
                num_bikes_available,
                num_docks_available,
                num_ebikes,
                ingested_at
            FROM raw_station_status
            ORDER BY station_code, ingested_at DESC
        ) st ON s.station_code = st.station_code
        WHERE s.arrondissement IS NOT NULL
        GROUP BY s.arrondissement
        ON CONFLICT (arrondissement, snapshot_time) DO UPDATE SET
            total_stations  = EXCLUDED.total_stations,
            total_bikes     = EXCLUDED.total_bikes,
            total_docks     = EXCLUDED.total_docks,
            total_ebikes    = EXCLUDED.total_ebikes,
            avg_fill_rate   = EXCLUDED.avg_fill_rate,
            empty_stations  = EXCLUDED.empty_stations,
            full_stations   = EXCLUDED.full_stations
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("Refreshed agg_availability_by_arrondissement")


def refresh_hourly_trend(conn):
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


def run_etl():
    log.info("═══ Starting Vélib' ETL run ═══")
    records = fetch_velib_data()
    infos, statuses = transform_all(records)

    conn = get_connection()
    try:
        load_stations(conn, infos)
        load_status(conn, statuses)
        refresh_agg_by_arrondissement(conn)
        refresh_hourly_trend(conn)
    finally:
        conn.close()

    log.info("═══ ETL run complete ═══")


if __name__ == "__main__":
    run_etl()