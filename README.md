# Kafka Load Observatory

Load-test a Kafka-backed Flask API with k6 and observe producer and consumer metrics in Prometheus and Grafana.

## Architecture

```text
k6 -> Flask API -> Kafka topic: events -> Processor
                  |                       |
                  +---- Prometheus <------+
                           |
                         Grafana
```

## Prerequisites

- Docker Desktop with Compose
- k6 installed locally for load tests

The Python services and their dependencies run inside Docker. The Compose build context is the repository root because both services use the local `kafka-lib` package.

## Start the stack

From the repository root:

```sh
docker compose up -d --build
```

Kafka starts first, and `kafka-init` creates the `events` topic with four partitions.

Check service status and logs:

```sh
docker compose ps
docker compose logs -f app processor
```

Stop the stack:

```sh
docker compose down
```

Use `docker compose down -v` when you also want to remove Compose-managed volumes.

## Service endpoints

| Service | Host endpoint | Purpose |
| --- | --- | --- |
| Flask API | http://localhost:5001 | Event API |
| Kafka exporter metrics | http://localhost:9308/metrics | Broker health and Kafka metrics |
| Prometheus | http://localhost:9090 | Metrics queries and targets |
| Grafana | http://localhost:3000 | Dashboards |

Grafana's default credentials are `admin` / `admin`. Prometheus is provisioned as its default data source.

## Test the API

Health check:

```sh
curl http://localhost:5001/health
```

Publish one event:

```sh
curl -X POST http://localhost:5001/events \
  -H "Content-Type: application/json" \
  -d '{"id":"1","source":"manual-test","value":12345}'
```

## Run k6

The executable scenario is `k6/scenarios/constant-load.js`. It defaults to 20 virtual users for two minutes and sends requests to `http://127.0.0.1:5001`.

```sh
k6 run k6/scenarios/constant-load.js
```

Override the target, virtual users, and duration with environment variables:

```sh
BASE_URL=http://127.0.0.1:5001 \
VUS=5 \
DURATION=10s \
k6 run k6/scenarios/constant-load.js
```

The scenario checks for HTTP 200 responses and fails when more than one percent of requests fail or the 95th percentile latency exceeds 500 ms.

## Kafka smoke test

Consume events from the Kafka container:

```sh
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic events \
  --from-beginning
```

## Prometheus metrics

The stack is intentionally focused on the Kafka exporter for broker and topic observability. Prometheus scrapes the exporter at `kafka-exporter:9308` via the configured job in `monitoring/prometheus/prometheus.yml`.

Example PromQL queries:

```promql
kafka_broker_info
kafka_topic_partition_current_offset
```

## Repository layout

```text
app/
  app/
    app.py          # Flask application and routes
    enums.py
    metrics.py
    publisher.py
  Dockerfile
  pyproject.toml

processor/
  processor.py      # Kafka consumer and business logic
  Dockerfile
  pyproject.toml

kafka-lib/          # Shared Kafka producer/consumer package
k6/                 # Load scenarios and payload helpers
monitoring/         # Prometheus and Grafana configuration
docker-compose.yaml
```

## Troubleshooting

Rebuild after changing Python source or dependency metadata:

```sh
docker compose build --no-cache app processor
docker compose up -d app processor
```

Run the Python app as a module from its package layout. Internal imports in `app/app/` must remain relative, for example `from .publisher import publisher`. The outer legacy `app/app.py` file should not be restored because it shadows the `app` package.
