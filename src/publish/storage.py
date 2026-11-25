import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path


class StorageError(OSError):
    pass


@dataclass(frozen=True)
class FileExtent:
    object_name: str
    logical_offset: int
    physical_offset: int
    length: int


@dataclass(frozen=True)
class PhysicalLayout:
    logical_id: int
    extents: tuple[FileExtent, ...]


class ArtifactStore:
    def __init__(self, workspace):
        self.path = Path(workspace) / 'objects'
        self.path.mkdir(parents=True, exist_ok=True)

    def _object_path(self, name):
        if not isinstance(name, str) or re.fullmatch(r'[a-z0-9_-]+\.blob', name) is None:
            raise StorageError('invalid internal object locator')
        return self.path / name

    def stage_file(self, owner, payload):
        if re.fullmatch(r'[a-z0-9-]+', owner) is None:
            raise StorageError('invalid stage owner')
        descriptor, name = tempfile.mkstemp(prefix=owner + '-', suffix='.blob', dir=self.path)
        artifact = Path(name)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if os.name == 'posix':
                directory_fd = os.open(self.path, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except BaseException:
            try:
                artifact.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return artifact.name

    def object_path(self, name):
        return self._object_path(name)

    def open_object_stream(self, name):
        return self._object_path(name).open('rb')

    def open_layout(self, repo, logical_id):
        rows = repo.connection.execute('''
            SELECT object_name, logical_offset, physical_offset, byte_length
            FROM file_extents WHERE logical_id = ? ORDER BY logical_offset
        ''', (logical_id,)).fetchall()
        if not rows:
            raise StorageError(f'missing layout for logical file {logical_id}')
        return PhysicalLayout(logical_id, tuple(FileExtent(*row) for row in rows))

    def read_logical_file(self, layout, *, streams=None):
        if not layout.extents:
            raise StorageError(f'empty layout for logical file {layout.logical_id}')
        expected_offset = 0
        chunks = []
        for extent in sorted(layout.extents, key=lambda item: item.logical_offset):
            if extent.logical_offset != expected_offset or extent.physical_offset < 0 or extent.length <= 0:
                raise StorageError(f'invalid extent range for logical file {layout.logical_id}')
            pinned = streams.get(extent.object_name) if streams is not None else None
            if pinned is None:
                with self._object_path(extent.object_name).open('rb') as stream:
                    stream.seek(extent.physical_offset)
                    data = stream.read(extent.length)
            else:
                pinned.seek(extent.physical_offset)
                data = pinned.read(extent.length)
            if len(data) != extent.length:
                raise StorageError(f'truncated extent for logical file {layout.logical_id}')
            chunks.append(data)
            expected_offset += extent.length
        return b''.join(chunks)

    def switch_layout(self, repo, layout):
        if not repo.connection.in_transaction:
            raise RuntimeError('layout switch requires a catalog transaction')
        if not layout.extents:
            raise StorageError('cannot install an empty layout')
        connection = repo.connection
        connection.execute('DELETE FROM file_extents WHERE logical_id = ?', (layout.logical_id,))
        for extent in layout.extents:
            self._object_path(extent.object_name)
            connection.execute('INSERT OR IGNORE INTO artifact_objects VALUES (?)', (extent.object_name,))
            connection.execute('INSERT INTO file_extents VALUES (?, ?, ?, ?, ?)', (
                layout.logical_id, extent.logical_offset, extent.object_name,
                extent.physical_offset, extent.length,
            ))

    def discard_stage(self, repo, name):
        if not repo.connection.in_transaction:
            raise RuntimeError('stage disposal requires a catalog transaction')
        referenced = repo.connection.execute(
            'SELECT 1 FROM artifact_objects WHERE object_name = ?', (name,),
        ).fetchone()
        if referenced is None:
            self._object_path(name).unlink(missing_ok=True)

    def discard_owner_stages(self, repo, owner):
        for path in self.path.glob(owner + '-*.blob'):
            self.discard_stage(repo, path.name)


def collect_retired(repo, store):
    """Reclaim physical volumes that were published but no longer back any extent.

    Eligibility is decided from the catalog alone: every name in
    ``artifact_objects`` was once published, and a volume stays protected as long
    as a current logical file has an extent inside it. Files living only in the
    staging area are never listed and are therefore left untouched. Each volume
    is unlinked and deregistered independently inside its own transaction so a
    process that exits mid-cleanup can resume and converge on a later run.
    Readers holding an open descriptor for a removed path keep the underlying
    bytes until they exit, per local POSIX semantics.
    """
    reclaimed = []
    while True:
        with repo.transaction():
            row = repo.connection.execute('''
                SELECT ao.object_name
                FROM artifact_objects ao
                WHERE NOT EXISTS (
                    SELECT 1 FROM file_extents fe WHERE fe.object_name = ao.object_name
                )
                ORDER BY ao.object_name
                LIMIT 1
            ''').fetchone()
            if row is None:
                break
            name = row['object_name']
            store.object_path(name).unlink(missing_ok=True)
            repo.connection.execute('DELETE FROM artifact_objects WHERE object_name = ?', (name,))
        reclaimed.append(name)
    return reclaimed
