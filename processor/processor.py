import logging
import os
import random
import sys
import threading
import time

from kafka_lib.kafka_consumer import KafkaConsumerClient
from prometheus_client import start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("processor")


def handler(payloads: dict, _):
    """
    Simulate business logic.
    """
    for payload in payloads:
        value = payload["data"]["value"]
        # Simulate processing time
        if value < 30_000:
            processing_time = random.uniform(0.01, 0.05)
        elif value < 70_000:
            processing_time = random.uniform(0.05, 0.2)
        else:
            processing_time = random.uniform(0.2, 0.5)

        time.sleep(processing_time)

        log.info("Processed event=%s value=%s", payload["id"], value)


def main():
    start_http_server(8000, addr="0.0.0.0")

    consumer = KafkaConsumerClient(
        topic=os.getenv("EVENT_TOPIC"),
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS")],
        group_id=os.getenv("KAFKA_GROUP_ID"),
    )
    consumer.start(lambda payloads, records: handler(payloads, records))
    threading.Event().wait()


if __name__ == "__main__":
    main()
