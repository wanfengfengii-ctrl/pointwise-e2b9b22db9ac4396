from decimal import Decimal, InvalidOperation

from src.ingest.records import PartitionBatch, StatusEvent


class BatchValidationError(ValueError):
    pass


def require_text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise BatchValidationError(f'{field} must be nonempty text')
    try:
        value.encode('utf-8')
    except UnicodeError as exc:
        raise BatchValidationError(f'{field} must be valid UTF-8 text') from exc
    return value


def require_sequence(value):
    if type(value) is not int or value < 0:
        raise BatchValidationError('sequence must be a nonnegative integer')
    return value


def event_value(value):
    if type(value) not in (str, int, Decimal):
        raise BatchValidationError('value must be an exact decimal')
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise BatchValidationError('value must be an exact decimal') from exc
    if not result.is_finite():
        raise BatchValidationError('value must be finite')
    return result


def validate_batch(batch: PartitionBatch) -> PartitionBatch:
    require_text(batch.partition, 'partition')
    require_text(batch.batch_id, 'batch')
    validated = []
    for position, row in enumerate(batch.events, 1):
        try:
            if not isinstance(row, StatusEvent) or row.partition != batch.partition:
                raise BatchValidationError('event partition does not match batch')
            validated.append(StatusEvent(
                batch.partition,
                require_text(row.key, 'key'),
                require_text(row.status, 'status'),
                event_value(row.value),
                require_sequence(row.sequence),
                require_text(row.source, 'source'),
            ))
        except BatchValidationError as exc:
            raise BatchValidationError(f'{batch.partition}/{batch.batch_id} row {position}: {exc}') from exc
    return PartitionBatch(batch.partition, batch.batch_id, tuple(validated))
