from dataclasses import dataclass
from hashlib import sha256


class SnapshotNotFound(LookupError):
    pass


class SnapshotConflict(ValueError):
    pass


@dataclass(frozen=True)
class SnapshotEntry:
    snapshot_id: str
    publish_sequence: int
    manifest: str
    manifest_digest: str


def insert_snapshot(repo, snapshot_id, manifest):
    if not repo.connection.in_transaction:
        raise RuntimeError('snapshot registration requires the publish transaction')
    if repo.connection.execute('SELECT 1 FROM snapshots WHERE snapshot_id = ?', (snapshot_id,)).fetchone():
        raise SnapshotConflict(f'snapshot already exists: {snapshot_id}')
    digest = sha256(manifest.encode('utf-8')).hexdigest()
    cursor = repo.connection.execute(
        'INSERT INTO snapshots(snapshot_id, manifest, manifest_digest) VALUES (?, ?, ?)',
        (snapshot_id, manifest, digest),
    )
    return SnapshotEntry(snapshot_id, cursor.lastrowid, manifest, digest)


def load_snapshot(repo, snapshot_id):
    row = repo.connection.execute('SELECT * FROM snapshots WHERE snapshot_id = ?', (snapshot_id,)).fetchone()
    if row is None:
        raise SnapshotNotFound(f'no committed snapshot {snapshot_id}')
    if sha256(row['manifest'].encode('utf-8')).hexdigest() != row['manifest_digest']:
        raise SnapshotConflict(f'manifest digest mismatch for {snapshot_id}')
    return SnapshotEntry(row['snapshot_id'], row['publish_sequence'], row['manifest'], row['manifest_digest'])
