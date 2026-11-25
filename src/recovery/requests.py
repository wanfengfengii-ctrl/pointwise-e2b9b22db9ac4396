import json
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256

from src.ingest.validation import BatchValidationError, event_value


class RequestState(StrEnum):
    PENDING = 'pending'
    RETRY = 'retry'
    REJECTED = 'rejected'
    SUCCESS = 'success'


@dataclass(frozen=True)
class RequestInput:
    request_id: str
    kind: str
    params: dict
    digest: str
    credential_digest: str


@dataclass(frozen=True)
class RequestOutcome:
    request_id: str
    kind: str
    state: RequestState
    digest: str
    result_ref: str | None
    receipt_id: str | None
    reason: str | None


def json_parameters(value):
    if isinstance(value, Decimal):
        return str(value)
    if value is None or type(value) in (int, bool, str):
        return value
    if isinstance(value, (list, tuple)):
        return [json_parameters(item) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: json_parameters(item) for key, item in value.items()}
    raise ValueError('parameters must be JSON fields with exact decimals')


def canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _decimal_identity(raw):
    try:
        value = event_value(raw)
    except BatchValidationError:
        return raw
    if not value:
        return '0'
    sign, digits, exponent = value.as_tuple()
    end = len(digits)
    while digits[end - 1] == 0:
        end -= 1
        exponent += 1
    return str(Decimal((sign, digits[:end], exponent)))


def parameter_digest(kind, params):
    canonical = json_parameters(params)
    if kind == 'publish' and isinstance(canonical, dict) and isinstance(canonical.get('members'), list):
        for member in canonical['members']:
            if not isinstance(member, dict) or not isinstance(member.get('events'), list):
                continue
            dictionary = member.get('dictionary')
            if isinstance(dictionary, list) and all(
                isinstance(pair, list) and len(pair) == 2 and type(pair[0]) is int for pair in dictionary
            ):
                member['dictionary'] = sorted(dictionary, key=lambda pair: pair[0])
            for event in member['events']:
                if isinstance(event, dict) and 'value' in event:
                    event['value'] = _decimal_identity(event['value'])
    serialized = canonical_json([kind, canonical])
    return sha256(serialized.encode('utf-8')).hexdigest()
