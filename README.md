# kafka-load-observatory
Load testing Kafka-based applications using k6 and Prometheus/Grafana.


The framework exposes a lightweight **Flask Producer API**. K6 generates HTTP traffic against this API, which publishes messages to Kafka. The consumer service processes the messages and exposes metrics that can be visualized in Grafana.

The primary goal is to answer questions such as:

* How many requests per second can the producer handle?
* How many events per second are received by the consumer?
* How many events per second are successfully processed?
* At what load does consumer lag begin to increase?
* How does processing latency change as load increases?

---

## Goals

* Simple setup with minimal dependencies.
* No custom k6 binaries or Kafka extensions.
* End-to-end load testing from HTTP request to Kafka consumer.
* Prometheus-based metrics collection.
* Grafana dashboards for real-time visualization.
* Easily extensible for larger-scale performance testing.

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
                |  Flask Producer API  |
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

---

### 2. Flask Producer API

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
* One topic
* Three partitions

The configuration can be expanded later to support larger clusters.

---

### 4. Consumer Service

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
.
├── producer/
│   ├── app.py
│   ├── kafka_producer.py
│   └── metrics.py
│
├── consumer/
│   ├── consumer.py
│   ├── processor.py
│   └── metrics.py
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
