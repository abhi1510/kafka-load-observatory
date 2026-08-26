import typing as t
from concurrent.futures import Future, ThreadPoolExecutor

from kafka import KafkaProducer


class KafkaPublisherClient:
    def __init__(self, bootstrap_servers: list[str], max_workers: int = 2):
        self.bootstrap_servers = bootstrap_servers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def _publish_sync(self, topic: str, key: bytes | None, value: bytes, headers: list):
        return self._producer.send(topic=topic, key=key, value=value, headers=headers)

    def publish(self, topic: str, data: bytes, **kwargs: dict[str, t.Any]) -> Future:
        key = kwargs["key"].encode("utf-8") if "key" in kwargs else None
        headers = [
            (k, str(v).encode("utf-8")) for k, v in kwargs.items() if k not in {"key"}
        ]
        return self._executor.submit(self._publish_sync, topic, key, data, headers)

    def stop(self):
        self._producer.flush()
        self._executor.stop()
