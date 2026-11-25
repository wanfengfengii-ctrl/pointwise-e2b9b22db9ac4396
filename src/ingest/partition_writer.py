from dataclasses import dataclass
from hashlib import sha256

from src.catalog.status_dictionary import DictionaryRevision, encode_status
from src.ingest.file_codec import PartitionHeader, encode_partition
from src.ingest.ordering import EventOrderError, order_events
from src.ingest.records import EncodedEvent, PartitionBatch, PartitionFileDescriptor
from src.ingest.validation import BatchValidationError, require_text, validate_batch


@dataclass(frozen=True)
class PreparedPartition:
    descriptor: PartitionFileDescriptor
    payload: bytes


def prepare_partition(batch: PartitionBatch, version: str, revision: DictionaryRevision) -> PreparedPartition:
    require_text(version, 'version')
    batch = validate_batch(batch)
    if revision.ref.partition != batch.partition:
        raise BatchValidationError(f'{batch.batch_id}: dictionary belongs to another partition')
    try:
        events = order_events(batch.events)
    except EventOrderError as exc:
        raise EventOrderError(f'{batch.partition}/{batch.batch_id}: {exc}') from exc
    rows = tuple(EncodedEvent(
        row.key, row.sequence, row.source, encode_status(revision, row.status), row.value,
    ) for row in events)
    header = PartitionHeader(
        batch.partition, version, batch.batch_id, revision.ref.generation,
        revision.ref.digest, len(rows),
    )
    payload = encode_partition(header, rows)
    descriptor = PartitionFileDescriptor(
        header.partition, header.version, header.batch_id, header.generation,
        header.dictionary_digest, header.record_count, sha256(payload).hexdigest(),
    )
    return PreparedPartition(descriptor, payload)
