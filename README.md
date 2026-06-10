# 🚲 Vélib' Real-Time Analytics Pipeline

> ETL & Pipeline Orchestration Final Project — ESILV MSc A4 | MACSIN4A2125

## Use Case

**Problem:** Paris has 1,400+ Vélib' bike-sharing stations. Knowing where bikes are available in real-time — and spotting patterns by arrondissement and time of day — helps city planners, commuters, and mobility analysts.

**Pipeline:** This project ingests live Vélib' availability data from the Paris Open Data API, processes it through a full ETL/ELT stack with Kafka streaming, orchestrates all tasks in Apache Airflow, and visualises results in a live Streamlit dashboard.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Paris Open Data API                       │
│         (velib-disponibilite-en-temps-reel)                  │
└────────────────────────┬────────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          │                             │
   [Airflow DAG]                [Kafka Producer]
   Every 5 min                  Every 60 sec
          │                             │
   ETL ingest.py               Kafka Topic: velib-status
   (batch ETL)                          │
          │                    [Kafka Consumer]
          ▼                             │
   ┌──────────────┐                     ▼
   │  PostgreSQL  │◄──────── stream_events table
   │              │
   │  raw_stations│
   │  raw_status  │
   │  agg tables  │
   └──────┬───────┘
          │
   [ELT SQL transforms]
   (views + agg tables)
          │
          ▼
   ┌──────────────┐
   │  Streamlit   │
   │  Dashboard   │
   │  :8501       │
   └──────────────┘
```

## Tech Stack

| Layer          | Tool                          |
|----------------|-------------------------------|
| Ingestion      | Python `requests`, Kafka producer |
| Storage        | PostgreSQL 15                 |
| Transformation | pandas, SQL (views + inserts) |
| Streaming      | Apache Kafka + Python consumer |
| Orchestration  | Apache Airflow 2.9            |
| Visualisation  | Streamlit + Plotly            |
| Infrastructure | Docker Compose                |

## Dashboard (5 visualisations)

1. **KPI Cards** — total bikes, e-bikes, docks, stations, empty stations, avg fill rate
2. **Live Map** — colour-coded by fill rate (red=empty, green=full)
3. **Bar Chart** — bikes available vs empty stations by arrondissement
4. **Heatmap** — average fill rate by arrondissement
5. **Event Stream** — live Kafka events (low_bikes, empty_station, full_station)

## Project Structure

```
velib-pipeline/
├── docker-compose.yml       # Full stack definition
├── sql/
│   └── init.sql             # Schema: raw + streaming + analytical layers
├── etl/
│   └── ingest.py            # Batch ETL: extract → transform → load (idempotent)
├── streaming/
│   ├── producer.py          # Kafka producer (polls API every 60s)
│   ├── consumer.py          # Kafka consumer (writes to stream_events)
│   ├── Dockerfile.producer
│   └── Dockerfile.consumer
├── dags/
│   └── velib_pipeline.py    # Airflow DAG (5 tasks, retries, scheduling)
├── dashboard/
│   ├── app.py               # Streamlit dashboard
│   └── Dockerfile.dashboard
├── README.md
└── SETUP.md
```

## Running the Project

See [SETUP.md](SETUP.md) for full instructions.

**Quick start:**
```bash
docker compose up --build -d
```

Then open:
- **Dashboard:** http://localhost:8501
- **Airflow:** http://localhost:8080 (admin / admin)
