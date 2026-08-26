import atexit
import json
import logging
import os
from concurrent import futures
from dataclasses import dataclass
from threading import Lock

from sqlalchemy.event import listens_for
from sqlalchemy.orm import Session

from .enums import DomainObject

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublisherEvent:
    domain_object: DomainObject
    payload = dict[str, dict]


class Publisher:
    def __init__(self):
        self.futures_buffer_lock: Lock = Lock()
        self.futures_buffer: set = set()
        self.client = None
        self.pending_domain_events = {}

    def _add_to_buffer(self, future: futures.Future):
        with self.futures_buffer_lock:
            self.futures_buffer.add(future)
        future.add_done_callback(self._callback)

    def _callback(self, future: futures.Future):
        if future.exception() is not None:
            log.warning("Error while publishing")
            return
        with self.futures_buffer_lock:
            self.futures_buffer.discard(future)

    def set_client(self, client):
        self.client = client
        atexit.register(self._finish_publication)

    def publish(self, domain_object: DomainObject, payload: dict[str, dict], **kwargs):
        topic = os.getenv(domain_object.topic)
        future = self.client.publish(
            topic=topic, data=json.dumps(payload).encode("utf-8"), **kwargs
        )
        self._add_to_buffer(future=future)
        log.info(f"Published {domain_object} on topic {topic}")

    def _finish_publication(self):
        if not self.client:
            return

        self.client.stop()
        futures.wait(
            fs=self.futures_buffer, return_when=futures.ALL_COMPLETED, timeout=10
        )
        for future in self.futures_buffer:
            future.result()


publisher = Publisher()


# def receive_after_flush(session, _flush_context=None):
#     upserts = list(session.new) + list(session.dirty)

#     session_events = publisher.pending_domain_events.setdefault(id(session), {})
#     for instance in upserts:
#         session_events[instance._key] = PublisherEvent(
#             domain_object=DomainObject(instance.__class__.__name__),
#             payload=instance.serialize(),
#         )


# def receive_after_commit(session):
#     session_events = publisher.pending_domain_events.pop(id(session), {})
#     try:
#         for publisher_event in session_events.values():
#             publisher.publish(publisher_event.domain_object, publisher_event.payload)
#     except Exception:
#         log.exception("Error publishing after_commit event")


# listens_for(Session, "after_flush")(receive_after_flush)
# listens_for(Session, "after_commit")(receive_after_commit)
