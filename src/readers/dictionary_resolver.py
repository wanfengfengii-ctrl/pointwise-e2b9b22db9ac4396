from dataclasses import dataclass
from decimal import Decimal

from src.catalog.status_dictionary import DictionaryError, DictionaryRef, MissingDictionary, load_revision, resolve_status


@dataclass(frozen=True)
class ResolvedEvent:
    partition: str
    version: str
    key: str
    sequence: int
    source: str
    status: str
    value: Decimal


class StatusResolutionError(ValueError):
    def __init__(self, reason, *, retryable=False):
        super().__init__(reason)
        self.retryable = retryable


def resolve_file_events(repo, member, version, header, events):
    descriptor = version.descriptor
    bound = DictionaryRef(descriptor.partition, descriptor.generation, descriptor.dictionary_digest)
    header_ref = DictionaryRef(header.partition, header.generation, header.dictionary_digest)
    manifest_ref = DictionaryRef(member.partition, member.generation, member.dictionary_digest)
    if bound != header_ref or bound != manifest_ref:
        raise StatusResolutionError('file, catalog version and manifest have different dictionary bindings')
    try:
        revision = load_revision(repo, bound.partition, bound.generation, expected_digest=bound.digest)
        return tuple(ResolvedEvent(
            header.partition, header.version, event.key, event.sequence, event.source,
            resolve_status(revision, event.code), event.value,
        ) for event in events)
    except MissingDictionary as exc:
        raise StatusResolutionError(str(exc), retryable=True) from exc
    except DictionaryError as exc:
        raise StatusResolutionError(str(exc)) from exc
