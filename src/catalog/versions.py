from dataclasses import dataclass
from hashlib import sha256

from src.catalog.status_dictionary import load_revision, resolve_status
from src.ingest.file_codec import PartitionHeader, decode_partition
from src.ingest.records import PartitionFileDescriptor


class VersionConflict(ValueError):
    pass


class VersionMissing(VersionConflict):
    pass


@dataclass(frozen=True)
class PartitionVersion:
    logical_id: int
    descriptor: PartitionFileDescriptor


def verify_payload(descriptor, payload):
    label = f'{descriptor.partition}/{descriptor.version}'
    if sha256(payload).hexdigest() != descriptor.digest:
        raise VersionConflict(f'file digest mismatch for {label}')
    header, events = decode_partition(payload)
    expected = PartitionHeader(
        descriptor.partition, descriptor.version, descriptor.batch_id,
        descriptor.generation, descriptor.dictionary_digest, descriptor.record_count,
    )
    if header != expected:
        raise VersionConflict(f'file header disagrees with catalog version {label}')
    return header, events


def check_publishable_payload(repo, descriptor, payload):
    header, events = verify_payload(descriptor, payload)
    revision = load_revision(repo, header.partition, header.generation, expected_digest=header.dictionary_digest)
    for event in events:
        resolve_status(revision, event.code)


def register_version(repo, descriptor, payload):
    if not repo.connection.in_transaction:
        raise RuntimeError('version registration requires the publish transaction')
    try:
        load_version(repo, descriptor.partition, descriptor.version)
    except VersionMissing:
        pass
    else:
        raise VersionConflict(f'version already exists: {descriptor.partition}/{descriptor.version}')
    check_publishable_payload(repo, descriptor, payload)
    cursor = repo.connection.execute('''
        INSERT INTO versions(partition, version, batch_id, generation, dictionary_digest, record_count, digest)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        descriptor.partition, descriptor.version, descriptor.batch_id, descriptor.generation,
        descriptor.dictionary_digest, descriptor.record_count, descriptor.digest,
    ))
    return PartitionVersion(cursor.lastrowid, descriptor)


def load_version(repo, partition, version):
    row = repo.connection.execute(
        'SELECT * FROM versions WHERE partition = ? AND version = ?', (partition, version),
    ).fetchone()
    if row is None:
        raise VersionMissing(f'missing version {partition}/{version}')
    descriptor = PartitionFileDescriptor(
        row['partition'], row['version'], row['batch_id'], row['generation'],
        row['dictionary_digest'], row['record_count'], row['digest'],
    )
    return PartitionVersion(row['logical_id'], descriptor)


def require_publishable_version(repo, partition, version):
    registered = load_version(repo, partition, version)
    desc = registered.descriptor
    load_revision(repo, partition, desc.generation, expected_digest=desc.dictionary_digest)
    if repo.connection.execute(
        'SELECT 1 FROM file_extents WHERE logical_id = ? LIMIT 1', (registered.logical_id,),
    ).fetchone() is None:
        raise VersionMissing(f'no file layout for {partition}/{version}')
    return registered
