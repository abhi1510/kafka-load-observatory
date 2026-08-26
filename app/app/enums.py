from enum import Enum


class DomainObject(Enum):
    EVENT = "Event"


for member in DomainObject:
    setattr(member, "topic", f"{member.value.upper()}_TOPIC")
