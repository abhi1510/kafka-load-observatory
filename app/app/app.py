import logging
import os
import sys
import time

from flask import Flask, jsonify, request
from kafka_lib.kafka_producer import KafkaPublisherClient
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .enums import DomainObject
from .metrics import (
    MESSAGES_PUBLISHED,
    PUBLISH_FAILURES,
    PUBLISH_LATENCY,
)
from .publisher import publisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("app")

publisher_client = KafkaPublisherClient(
    bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS")]
)
publisher.set_client(client=publisher_client)

app = Flask(__name__)


@app.route("/health")
def health():
    return "OK"


@app.route("/events", methods=["POST"])
def events():
    payload = request.get_json()
    start = time.perf_counter()
    try:
        publisher.publish(DomainObject.EVENT, payload)

        MESSAGES_PUBLISHED.inc()
        PUBLISH_LATENCY.observe(time.perf_counter() - start)

        return jsonify({"status": "published"}), 200

    except Exception as ex:
        PUBLISH_FAILURES.inc()

        return jsonify({"error": str(ex)}), 500


@app.get("/metrics")
def metrics():
    return (
        generate_latest(),
        200,
        {
            "Content-Type": CONTENT_TYPE_LATEST,
        },
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
