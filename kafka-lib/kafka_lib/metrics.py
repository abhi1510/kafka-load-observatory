from prometheus_client import Counter, Histogram

# Message Counters
MESSAGES_RECEIVED = Counter(
    "kafka_consumer_messages_received_total",
    "Total raw messages fetched from Kafka",
    ["topic", "group_id"],
)

MESSAGES_PROCESSED = Counter(
    "kafka_consumer_messages_processed_total",
    "Total messages successfully deserialized and handled",
    ["topic", "group_id"],
)

MESSAGES_FAILED = Counter(
    "kafka_consumer_messages_failed_total",
    "Total messages that failed processing or deserialization",
    ["topic", "group_id", "reason"],  # 'deserialization' or 'handler'
)

# Batch & Latency Histograms
BATCH_SIZE = Histogram(
    "kafka_consumer_batch_size_records",
    "Number of records fetched per poll batch",
    ["topic", "group_id"],
    buckets=[1, 10, 50, 100, 250, 500, 1000],
)

PROCESSING_LATENCY = Histogram(
    "kafka_consumer_processing_duration_seconds",
    "Time spent in the batch execution handler",
    ["topic", "group_id"],
)
