import json
import logging
import random
import time
import typing as t
from threading import Event, Thread

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO)


class KafkaConsumerClient:
    def __init__(
        self,
        topic: str,
        bootstrap_servers: list[str],
        group_id: str,
        value_deserializer: t.Callable[[bytes], t.Any] = lambda v: json.loads(
            v.decode("utf-8")
        ),
        max_poll_records: int = 1000,
        poll_timeout_ms: int = 5000,
        max_retries: int | None = None,
        backoff_base: float = 1,
        backoff_max: float = 30.0,
        # max_msg_retries: int = 3,
        # dlq_topic: str | None = None,
        **kwargs,
    ):
        self.topic = topic
        self._bootstrap_servers = bootstrap_servers
        self.group_id = group_id

        self.value_deserializer = value_deserializer

        self.max_poll_records = max_poll_records
        self.poll_timeout_ms = poll_timeout_ms

        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

        self.stop_event = Event()
        self.thread: Thread | None = None

        self.handler: t.Callable[[list[t.Any], list[ConsumerRecord]], None] | None = (
            None
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def start(self, handler: t.Callable[[list[t.Any], list[ConsumerRecord]], None]):
        if self.thread and self.thread.is_alive():
            raise RuntimeError("Kafka consumer already running!!!")

        if handler is None:
            raise RuntimeError("No handler registered")

        self.handler = handler
        self.stop_event.clear()

        self.thread = Thread(target=self._run, daemon=True)
        self.thread.start()

        self.logger.info("Kafka consumer started for topic '%s'", self.topic)

    def _run(self):
        retries = 0

        while not self.stop_event.is_set():
            try:
                consumer = self._create_consumer()
                self._poll(consumer)

                retries = 0

                if self.stop_event.is_set():
                    break

            except NoBrokersAvailable:
                retries += 1
                self.logger.exception("Kafka brokers unavailable")

            except KafkaError:
                retries += 1
                self.logger.exception("Kafka error")

            except Exception:
                retries += 1
                self.logger.exception("Unexpected consumer error")

            if self.max_retries and retries >= self.max_retries:
                self.logger.error("Max retries exceeded")
                break

            if retries:
                sleep_time = min(
                    self.backoff_base * (2 ** (retries - 1)), self.backoff_max
                ) + random.uniform(0, 1.0)
                self.logger.warning(f"Retrying kafka connection in {sleep_time:1f}s")
                time.sleep(sleep_time)

        self.logger.info("Kafka consumer exiting!!!")

    def _create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=self.max_poll_records,
        )

    def _poll(self, consumer: KafkaConsumer):
        self.logger.info(
            "Consuming from topic %s on group_id %s", self.topic, self.group_id
        )

        try:
            while not self.stop_event.is_set():
                batch = consumer.poll(timeout_ms=self.poll_timeout_ms)

                if not batch:
                    continue

                payloads: list[t.Any] = []
                records: list[ConsumerRecord] = [
                    record
                    for partition_records in batch.values()
                    for record in partition_records
                ]

                for record in records:
                    try:
                        payloads.append(self.value_deserializer(record.value))
                    except Exception:
                        self.logger.exception(
                            "Deserialisation failed - skipping message"
                        )

                try:
                    self.handler(payloads, records)
                    consumer.commit()
                except Exception:
                    self.logger.exception(
                        "Batch processing failed and offset not committed"
                    )
        finally:
            consumer.close()

    def stop(self):
        self.logger.info("Stopping kafka consumer!!!")
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=10)
        self.logger.info("Kafka consumer stopped!!!")
