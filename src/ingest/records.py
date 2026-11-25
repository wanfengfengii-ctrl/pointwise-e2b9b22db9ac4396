from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class StatusEvent:
    partition: str
    key: str
    status: str
    value: Decimal
    sequence: int
    source: str


@dataclass(frozen=True)
class EncodedEvent:
    key: str
    sequence: int
    source: str
    code: int
    value: Decimal


@dataclass(frozen=True)
class PartitionBatch:
    partition: str
    batch_id: str
    events: tuple[StatusEvent, ...]


@dataclass(frozen=True)
class PartitionFileDescriptor:
    partition: str
    version: str
    batch_id: str
    generation: int
    dictionary_digest: str
    record_count: int
    digest: str
