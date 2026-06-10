# SETUP — Vélib' Pipeline

## Prerequisites

- Docker Desktop (Mac/Windows) or Docker Engine (Linux)
- Docker Compose v2
- 4 GB RAM available for Docker
- Internet access (to reach Paris Open Data API)

## Step-by-step

### 1. Clone / unzip the project

```bash
cd velib-pipeline
```

### 2. Start the full stack

```bash
docker compose up --build -d
```

First run takes ~3–5 minutes to pull images and initialise Airflow.

### 3. Wait for services to be ready

```bash
# Watch startup logs
docker compose logs -f airflow-init

# Check all containers are running
docker compose ps
```

All containers should show `Up` or `running`.

### 4. Open the interfaces

| Service    | URL                        | Credentials    |
|------------|----------------------------|----------------|
| Dashboard  | http://localhost:8501      | (none)         |
| Airflow    | http://localhost:8080      | admin / admin  |

### 5. Trigger the first ETL run

In Airflow UI:
1. Go to DAGs → `velib_pipeline`
2. Toggle it ON (unpause)
3. Click ▶ (Trigger DAG)

After ~30 seconds, the dashboard will show live data.

### 6. Verify Kafka streaming

```bash
# Check producer logs
docker compose logs kafka-producer --tail=20

# Check consumer logs
docker compose logs kafka-consumer --tail=20
```

You should see messages like:
```
Published 1456 messages to 'velib-status'
Flushed 100 events to DB
```

## Stopping the stack

```bash
docker compose down
```

To also remove all data:
```bash
docker compose down -v
```

## Troubleshooting

**Airflow stuck on "running":**
```bash
docker compose restart airflow-scheduler
```

**Kafka consumer not connecting:**
```bash
docker compose restart kafka-consumer
```

**PostgreSQL connection refused:**
```bash
docker compose logs postgres --tail=20
# Wait for: "database system is ready to accept connections"
```

**Dashboard shows "No data yet":**
- Trigger the Airflow DAG manually (step 5 above)
- Wait 60 seconds for the first Kafka messages

## Architecture note on idempotency

- `raw_stations`: `ON CONFLICT (station_code) DO UPDATE` — safe to re-run
- `raw_station_status`: `ON CONFLICT (station_code, last_reported) DO NOTHING` — deduplicates by station + timestamp
- `agg_*` tables: `ON CONFLICT ... DO UPDATE` — overwrite with fresh values
- DAG `max_active_runs=1` — prevents concurrent runs
