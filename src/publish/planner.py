from dataclasses import dataclass

from src.catalog.status_dictionary import DictionaryError, DictionaryRevision, build_revision, load_revision
from src.catalog.versions import PartitionVersion, require_publishable_version
from src.ingest.partition_writer import PreparedPartition, prepare_partition
from src.ingest.records import PartitionBatch, StatusEvent
from src.ingest.validation import require_text


@dataclass(frozen=True)
class PublishPlan:
    snapshot_id: str
    members: tuple[PartitionVersion | PreparedPartition, ...]
    revisions: tuple[DictionaryRevision, ...]


_REFERENCE_FIELDS = {'partition', 'version'}
_NEW_FIELDS = _REFERENCE_FIELDS | {'batch', 'generation', 'events'}
_EVENT_FIELDS = {'partition', 'key', 'status', 'value', 'sequence', 'source'}


def _batch(spec):
    rows = spec['events']
    if not isinstance(rows, list):
        raise ValueError(f'{spec["batch"]}: events must be an ordered array')
    events = []
    for position, row in enumerate(rows, 1):
        if not isinstance(row, dict) or set(row) != _EVENT_FIELDS:
            raise ValueError(f'{spec["batch"]} row {position}: event fields are incomplete or unknown')
        events.append(StatusEvent(**row))
    return PartitionBatch(spec['partition'], spec['batch'], tuple(events))


def build_publish_plan(repo, params):
    if not isinstance(params, dict) or set(params) != {'snapshot', 'members'}:
        raise ValueError('publish requires snapshot and members')
    snapshot_id = require_text(params['snapshot'], 'snapshot')
    specs = params['members']
    if not isinstance(specs, list) or not specs:
        raise ValueError('members must be a nonempty ordered array')
    seen = set()
    revisions = {}
    for spec in specs:
        if not isinstance(spec, dict) or set(spec) not in (_REFERENCE_FIELDS, _NEW_FIELDS, _NEW_FIELDS | {'dictionary'}):
            raise ValueError('invalid publish member fields')
        partition = require_text(spec['partition'], 'partition')
        version = require_text(spec['version'], 'version')
        key = partition, version
        if key in seen:
            raise ValueError(f'duplicate member {partition}/{version}')
        seen.add(key)
        if 'generation' in spec and (type(spec['generation']) is not int or not 1 <= spec['generation'] <= 2**63 - 1):
            raise DictionaryError('dictionary generation must be a positive SQLite integer')
        if 'dictionary' in spec:
            revision = build_revision(partition, spec['generation'], spec['dictionary'])
            slot = partition, spec['generation']
            if slot in revisions and revisions[slot] != revision:
                raise DictionaryError(f'conflicting declarations for {partition}/{spec["generation"]}')
            revisions[slot] = revision
    members = []
    for spec in specs:
        partition, version = spec['partition'], spec['version']
        if 'events' not in spec:
            members.append(require_publishable_version(repo, partition, version))
            continue
        generation = spec['generation']
        revision = revisions.get((partition, generation))
        if revision is None:
            revision = load_revision(repo, partition, generation)
        members.append(prepare_partition(_batch(spec), version, revision))
    return PublishPlan(snapshot_id, tuple(members), tuple(revisions.values()))
