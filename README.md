# kafka-load-observatory
Load testing Kafka-based applications using k6 and Prometheus/Grafana.


The framework exposes a lightweight **Flask Event API**. K6 generates HTTP traffic against this API, which publishes messages to Kafka. The consumer service processes the messages and exposes metrics that can be visualized in Grafana.

The primary goal is to answer questions such as:

* How many requests per second can the producer handle?
* How many events per second are received by the consumer?
* How many events per second are successfully processed?
* At what load does consumer lag begin to increase?
* How does processing latency change as load increases?

---

## High-Level Architecture

```text
                    +----------------+
                    |      k6        |
                    | Load Generator |
                    +-------+--------+
                            |
                       HTTP POST
                            |
                            v
                +----------------------+
                |   Flask Event API    |
                +----------+-----------+
                           |
                    Kafka Produce
                           |
                           v
                    +-------------+
                    |   Kafka     |
                    +------+------+ 
                           |
                     Consumer Group
                           |
                           v
              +-------------------------+
              | Consumer Application    |
              +------------+------------+
                           |
                    Business Logic

----------------------------------------------------

Prometheus <----- Producer Metrics
Prometheus <----- Consumer Metrics
Prometheus <----- Kafka Metrics

                 |
                 v

              Grafana
```

---

## Components

### 1. k6

Responsible for generating configurable HTTP traffic.

Responsibilities:

* Generate configurable request rates.
* Support ramp-up/ramp-down scenarios.
* Collect HTTP latency and success metrics.

```sh
BASE_URL=http://127.0.0.1:5001 VUS=5 DURATION=1s \
  k6 run k6/scenarios/constant-load.js
```

---

### 2. Flask API

Acts as the bridge between HTTP and Kafka.

Responsibilities:

* Accept HTTP requests.
* Publish messages to Kafka.
* Return success after Kafka acknowledges the message.
* Expose Prometheus metrics.

---

### 3. Kafka

Responsible for durable message storage and distribution.

Initial configuration:

* Single broker
* 1 topic
* 4 partitions

The configuration can be expanded later to support larger clusters.

#### Testing Kafka

```sh
# start a producer
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic events

# start a consumer
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic events \
  --from-beginning
```
---

### 4. App

```sh
docker compose down -v \
  && docker rmi klo-app \
  && docker compose up -d
```

```sh
curl -X POST http://127.0.0.1:5001/events \
     -H "Content-Type: application/json" \
     -d '{"name": "John Doe", "email": "john@example.com"}'
```

-----

Consumes messages from Kafka.

Responsibilities:

* Receive messages.
* Execute business logic.
* Record processing metrics.
* Expose Prometheus metrics.

---

### 5. Prometheus

Scrapes metrics from:

* Producer API
* Consumer Service
* Kafka Exporter (optional)
* Node Exporter (optional)

---

### 6. Grafana

Visualizes system performance during load tests.

Example dashboards include:

* HTTP Requests/sec
* Producer throughput
* Consumer throughput
* Processing latency
* Consumer lag
* Error rate
* CPU and memory utilization

---

## Metrics

### Producer Metrics

| Metric                           | Description                           |
| -------------------------------- | ------------------------------------- |
| `http_requests_total`            | Total HTTP requests received          |
| `kafka_messages_published_total` | Successfully published Kafka messages |
| `kafka_publish_failures_total`   | Failed publish attempts               |
| `kafka_publish_latency_seconds`  | Kafka publish latency                 |

---

### Consumer Metrics

| Metric                        | Description                     |
| ----------------------------- | ------------------------------- |
| `messages_received_total`     | Messages consumed from Kafka    |
| `messages_processed_total`    | Successfully processed messages |
| `messages_failed_total`       | Failed message processing       |
| `processing_duration_seconds` | End-to-end processing latency   |

---

## Derived Metrics

Prometheus can derive useful throughput metrics using counters.

| Metric               | PromQL                                     |
| -------------------- | ------------------------------------------ |
| Events received/sec  | `rate(messages_received_total[1m])`        |
| Events processed/sec | `rate(messages_processed_total[1m])`       |
| Publish throughput   | `rate(kafka_messages_published_total[1m])` |
| Publish failures/sec | `rate(kafka_publish_failures_total[1m])`   |

---

## Initial Load Profile

| Stage    | Requests/sec | Duration |
| -------- | -----------: | -------: |
| Baseline |          100 |    5 min |
| Light    |          500 |    5 min |
| Moderate |        1,000 |    5 min |
| Heavy    |        5,000 |    5 min |

These values are intended as starting points and can be adjusted based on system capacity.

---

## Repository Structure

```text
├── producer/
│   ├── app.py
│   ├── producer.py
│   ├── metrics.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── consumer/
│   ├── consumer.py
│   ├── processor.py
│   ├── metrics.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── k6/
│   ├── scripts/
│   └── scenarios/
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── docker-compose.yml
└── README.md
```

---

## Future Enhancements

* Multiple Kafka brokers
* Multiple consumer groups
* Batch publishing
* Batch consumption
* Configurable payload sizes
* Authentication and TLS
* Kafka lag monitoring
* Distributed load generation
* CI/CD integration for performance regression testing
* Automated report generation


## Running the app

```shell
docker compose down -v && docker system prune -f
docker compose --build --no-cache && docker compose up -d
```

### Test with single request
```shell
curl -X POST http://localhost:5050/publish \
  -H "Content-Type: application/json" \
  -d '{
    "id": "1",
    "timestamp": "2026-07-14T17:00:00Z",
    "payload": {
      "source": "manual-test",
      "value": 12345
    }
  }'
```

## Running the benchmark (k6)
```shell
# Default: 20 virtual users for 2 minutes
k6 run k6/scenarios/constant-load.js

# 50 virtual users for 5 minutes:
k6 run \
    -e VUS=50 \
    -e DURATION=5m \
    k6/scenarios/constant-load.js

# 10 virtual users for 10 seconds:
k6 run \
    -e VUS=10 \
    -e DURATION=10s \
    k6/scenarios/constant-load.js
```

# TODO: NEED TO SETUP MONITORING NOW