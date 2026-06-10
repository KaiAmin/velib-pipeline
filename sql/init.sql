-- ─────────────────────────────────────────────────────────────────
-- Vélib' Pipeline — Database Schema
-- ─────────────────────────────────────────────────────────────────

-- Create dedicated database for velib data
CREATE DATABASE velib;

\connect velib;

-- ─────────────────────────────────────────────────────────────────
-- RAW / STAGING LAYER
-- ─────────────────────────────────────────────────────────────────

-- Raw station information (batch ETL, updated daily)
CREATE TABLE IF NOT EXISTS raw_stations (
    station_code        VARCHAR(20) PRIMARY KEY,
    name                TEXT,
    capacity            INTEGER,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    arrondissement      INTEGER,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

-- Raw station status snapshots (batch ETL, every 5 min via Airflow)
CREATE TABLE IF NOT EXISTS raw_station_status (
    id                  SERIAL PRIMARY KEY,
    station_code        VARCHAR(20),
    num_bikes_available INTEGER,
    num_docks_available INTEGER,
    num_ebikes          INTEGER,
    num_mechanical      INTEGER,
    is_installed        BOOLEAN,
    is_renting          BOOLEAN,
    is_returning        BOOLEAN,
    last_reported       TIMESTAMP,
    ingested_at         TIMESTAMP DEFAULT NOW(),
    -- Idempotency: avoid duplicates for same station + same last_reported
    UNIQUE (station_code, last_reported)
);

-- ─────────────────────────────────────────────────────────────────
-- STREAMING LAYER
-- ─────────────────────────────────────────────────────────────────

-- Real-time events written by Kafka consumer
CREATE TABLE IF NOT EXISTS stream_events (
    id                  SERIAL PRIMARY KEY,
    station_code        VARCHAR(20),
    event_type          VARCHAR(50),   -- 'low_bikes', 'full_station', 'snapshot'
    num_bikes_available INTEGER,
    num_docks_available INTEGER,
    num_ebikes          INTEGER,
    payload             JSONB,
    consumed_at         TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────
-- ANALYTICAL LAYER (ELT — SQL transformations)
-- ─────────────────────────────────────────────────────────────────

-- Aggregated availability by arrondissement (refreshed by Airflow)
CREATE TABLE IF NOT EXISTS agg_availability_by_arrondissement (
    arrondissement      INTEGER,
    snapshot_time       TIMESTAMP,
    total_stations      INTEGER,
    total_bikes         INTEGER,
    total_docks         INTEGER,
    total_ebikes        INTEGER,
    avg_fill_rate       DOUBLE PRECISION,
    empty_stations      INTEGER,
    full_stations       INTEGER,
    PRIMARY KEY (arrondissement, snapshot_time)
);

-- Hourly trend per station (refreshed by Airflow)
CREATE TABLE IF NOT EXISTS agg_hourly_trend (
    station_code        VARCHAR(20),
    hour_bucket         TIMESTAMP,
    avg_bikes           DOUBLE PRECISION,
    avg_docks           DOUBLE PRECISION,
    min_bikes           INTEGER,
    max_bikes           INTEGER,
    snapshots_count     INTEGER,
    PRIMARY KEY (station_code, hour_bucket)
);

-- ─────────────────────────────────────────────────────────────────
-- ANALYTICAL VIEWS (ELT)
-- ─────────────────────────────────────────────────────────────────

-- Current live status (latest snapshot per station)
CREATE OR REPLACE VIEW v_current_status AS
SELECT
    s.station_code,
    s.name,
    s.arrondissement,
    s.capacity,
    s.lat,
    s.lon,
    st.num_bikes_available,
    st.num_docks_available,
    st.num_ebikes,
    st.num_mechanical,
    st.is_renting,
    st.last_reported,
    ROUND(
        100.0 * st.num_bikes_available / NULLIF(s.capacity, 0),
        1
    ) AS fill_rate_pct
FROM raw_stations s
JOIN raw_station_status st
    ON s.station_code = st.station_code
WHERE st.ingested_at = (
    SELECT MAX(ingested_at)
    FROM raw_station_status s2
    WHERE s2.station_code = st.station_code
);

-- Stations with critical low availability (< 3 bikes)
CREATE OR REPLACE VIEW v_critical_stations AS
SELECT *
FROM v_current_status
WHERE num_bikes_available < 3
  AND is_renting = TRUE
ORDER BY num_bikes_available ASC;

-- Top 10 busiest stations (most activity in last 24h)
CREATE OR REPLACE VIEW v_top_active_stations AS
SELECT
    station_code,
    COUNT(*) AS snapshot_count,
    AVG(num_bikes_available) AS avg_bikes,
    MIN(num_bikes_available) AS min_bikes,
    MAX(num_bikes_available) AS max_bikes
FROM raw_station_status
WHERE ingested_at > NOW() - INTERVAL '24 hours'
GROUP BY station_code
ORDER BY snapshot_count DESC
LIMIT 10;
