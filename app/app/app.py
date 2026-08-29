import logging
import os
import sys

from flask import Flask, jsonify, request
from kafka_lib.kafka_producer import KafkaPublisherClient

from .enums import DomainObject
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
    try:
        publisher.publish(DomainObject.EVENT, payload)
        return jsonify({"status": "published"}), 200
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
