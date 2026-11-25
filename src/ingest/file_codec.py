import json
from dataclasses import asdict, dataclass

from src.ingest.ordering import order_events
from src.ingest.records import EncodedEvent
from src.ingest.validation import event_value, require_sequence, require_text


@dataclass(frozen=True)
class PartitionHeader:
    partition: str
    version: str
    batch_id: str
    generation: int
    dictionary_digest: str
    record_count: int


class PartitionFormatError(ValueError):
    pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PartitionFormatError(f'duplicate file field {key}')
        result[key] = value
    return result


def encode_partition(header, events):
    rows = []
    for event in events:
        row = asdict(event)
        row['value'] = str(event.value)
        rows.append(row)
    document = {'format': 1, 'header': asdict(header), 'rows': rows}
    return json.dumps(document, ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode('utf-8')


def decode_partition(payload):
    try:
        document = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique_object)
        if not isinstance(document, dict) or set(document) != {'format', 'header', 'rows'}:
            raise PartitionFormatError('invalid file envelope')
        if type(document['format']) is not int or document['format'] != 1:
            raise PartitionFormatError('unsupported partition format')
        header = PartitionHeader(**document['header'])
        for field in ('partition', 'version', 'batch_id', 'dictionary_digest'):
            require_text(getattr(header, field), field)
        if type(header.generation) is not int or header.generation < 1:
            raise PartitionFormatError('invalid dictionary generation')
        rows = document['rows']
        if not isinstance(rows, list) or type(header.record_count) is not int or header.record_count != len(rows):
            raise PartitionFormatError('record count mismatch')
        events = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {'key', 'sequence', 'source', 'code', 'value'}:
                raise PartitionFormatError('invalid event fields')
            if type(row['code']) is not int or not isinstance(row['value'], str):
                raise PartitionFormatError('invalid code or decimal representation')
            events.append(EncodedEvent(
                require_text(row['key'], 'key'), require_sequence(row['sequence']),
                require_text(row['source'], 'source'), row['code'], event_value(row['value']),
            ))
        return header, order_events(events)
    except PartitionFormatError:
        raise
    except (ValueError, TypeError, UnicodeError) as exc:
        raise PartitionFormatError(f'invalid partition file: {exc}') from exc
