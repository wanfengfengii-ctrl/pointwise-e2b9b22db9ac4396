import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ManifestMember:
    ordinal: int
    logical_id: int
    partition: str
    version: str
    generation: int
    dictionary_digest: str
    digest: str
    record_count: int


@dataclass(frozen=True)
class SnapshotManifest:
    snapshot_id: str
    members: tuple[ManifestMember, ...]


def manifest_member(ordinal, registered):
    descriptor = registered.descriptor
    return ManifestMember(
        ordinal, registered.logical_id, descriptor.partition, descriptor.version,
        descriptor.generation, descriptor.dictionary_digest, descriptor.digest, descriptor.record_count,
    )


def freeze_manifest(snapshot_id, versions):
    members = tuple(manifest_member(index, version) for index, version in enumerate(versions))
    _check_members(members)
    return SnapshotManifest(snapshot_id, members)


def _check_members(members):
    if not members:
        raise ValueError('a snapshot requires at least one member, which may have zero rows')
    keys = set()
    logical_ids = set()
    for position, member in enumerate(members):
        if type(member.ordinal) is not int or member.ordinal != position:
            raise ValueError('invalid manifest member order')
        for field in ('logical_id', 'generation', 'record_count'):
            value = getattr(member, field)
            minimum = 0 if field == 'record_count' else 1
            if type(value) is not int or value < minimum:
                raise ValueError(f'invalid manifest {field}')
        for field in ('partition', 'version', 'digest', 'dictionary_digest'):
            if not isinstance(getattr(member, field), str) or not getattr(member, field):
                raise ValueError(f'invalid manifest {field}')
        key = member.partition, member.version
        if key in keys or member.logical_id in logical_ids:
            raise ValueError(f'duplicate manifest member {key}')
        keys.add(key)
        logical_ids.add(member.logical_id)


def encode_manifest(manifest):
    return json.dumps(asdict(manifest), ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def decode_manifest(body):
    try:
        value = json.loads(body)
        if set(value) != {'snapshot_id', 'members'} or not isinstance(value['members'], list):
            raise ValueError('invalid manifest fields')
        if not isinstance(value['snapshot_id'], str) or not value['snapshot_id']:
            raise ValueError('invalid manifest snapshot identity')
        members = tuple(ManifestMember(**member) for member in value['members'])
        _check_members(members)
        return SnapshotManifest(value['snapshot_id'], members)
    except (TypeError, KeyError, ValueError) as exc:
        raise ValueError(f'invalid snapshot manifest: {exc}') from exc
