# Vélib' Real-Time Analytics Pipeline

ETL & Pipeline Orchestration Final Project — ESILV MSc A4 | MACSIN4A2125

## What is this project?

Paris has over 1,500 Vélib' bike-sharing stations spread across the city and surrounding areas. The idea behind this project was simple: build a pipeline that tracks live bike availability across all stations, aggregates the data by arrondissement, and displays everything on a dashboard that updates in real time.

The data comes from the Paris Open Data API which is free and requires no API key.

## How it works

The pipeline runs two things in parallel:

**Batch side (Airflow):** every 5 minutes, a DAG fetches all station data from the API, cleans and transforms it, then loads it into PostgreSQL. SQL views and aggregation tables are refreshed after each run.

**Streaming side (Kafka):** a producer polls the API every 60 seconds and publishes each station snapshot to a Kafka topic. A consumer reads from that topic continuously and writes events to a stream_events table, including alerts like low_bikes and empty_station.

The Streamlit dashboard reads from PostgreSQL and auto-refreshes every 60 seconds.

## Tech stack

| Layer | Tool |
|---|---|
| Ingestion | Python requests, Kafka producer |
| Storage | PostgreSQL 15 |
| Transformation | pandas, SQL views |
| Streaming | Apache Kafka + Python consumer |
| Orchestration | Apache Airflow 2.9 |
| Visualisation | Streamlit + Plotly |
| Infrastructure | Docker Compose |

## Dashboard

5 visualisations:

1. KPI cards — total bikes, e-bikes, free docks, active stations, empty stations, avg fill rate
2. Live map — all stations colour-coded by fill rate (red = empty, green = full)
3. Bar chart — bikes available vs empty stations by arrondissement
4. Heatmap — average fill rate by arrondissement
5. Event stream — last 50 Kafka events with colour-coded alerts

## Project structure

```
velib-pipeline/
├── docker-compose.yml
├── sql/
│   └── init.sql
├── etl/
│   └── ingest.py
├── streaming/
│   ├── producer.py
│   ├── consumer.py
│   ├── Dockerfile.producer
│   └── Dockerfile.consumer
├── dags/
│   └── velib_pipeline.py
├── dashboard/
│   ├── app.py
│   └── Dockerfile.dashboard
├── README.md
└── SETUP.md
```

## Running the project

Full setup instructions are in SETUP.md.

```bash
docker compose up --build -d
```

Once everything is up:
- Dashboard: http://localhost:8501
- Airflow: http://localhost:8080 (admin / admin)
