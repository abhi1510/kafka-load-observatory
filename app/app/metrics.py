from prometheus_client import Counter, Histogram

MESSAGES_PUBLISHED = Counter(
    "kafka_messages_published_total",
    "Total successfully published Kafka messages",
)

PUBLISH_FAILURES = Counter(
    "kafka_publish_failures_total",
    "Total failed Kafka publish attempts",
)

PUBLISH_LATENCY = Histogram(
    "kafka_publish_latency_seconds",
    "Kafka publish latency",
)