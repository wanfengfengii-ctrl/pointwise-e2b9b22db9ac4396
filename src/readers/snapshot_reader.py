from contextlib import contextmanager, nullcontext

from src.catalog.versions import VersionConflict, verify_payload
from src.ingest.file_codec import PartitionFormatError
from src.readers.dictionary_resolver import StatusResolutionError, resolve_file_events
from src.readers.read_plan import ReadPlanError, plan_snapshot_read


class SnapshotReadError(ValueError):
    def __init__(self, reason, *, retryable=False):
        super().__init__(reason)
        self.retryable = retryable


class _ProtectedSnapshotReader:
    """Repeatedly reads a snapshot through a layout pinned at entry time.

    The physical layout is fixed once the scope opens and every backing object
    gets an open descriptor, so another process compaction or even reclaiming
    the old volumes cannot disturb reads performed inside the scope. The body is
    only fetched (and decoded/verified) when :meth:`read` is called.
    """

    def __init__(self, repo, store, snapshot_id, plan):
        self._repo = repo
        self._store = store
        self.snapshot_id = snapshot_id
        self.plan = plan
        self._streams = {}

    def acquire(self):
        try:
            for item in self.plan.members:
                for extent in item.layout.extents:
                    if extent.object_name not in self._streams:
                        self._streams[extent.object_name] = self._store.open_object_stream(extent.object_name)
        except OSError as exc:
            self._close()
            raise SnapshotReadError(f'{self.snapshot_id}: unavailable file: {exc}', retryable=True) from exc

    def _close(self):
        streams = self._streams
        self._streams = {}
        for stream in streams.values():
            stream.close()

    def read(self):
        if not self._streams:
            raise SnapshotReadError(f'{self.snapshot_id}: read scope is closed')
        resolved = []
        for item in self.plan.members:
            label = f'{self.snapshot_id}/{item.manifest.partition}/{item.manifest.version}'
            try:
                payload = self._store.read_logical_file(item.layout, streams=self._streams)
                header, events = verify_payload(item.version.descriptor, payload)
                resolved.extend(
                    resolve_file_events(self._repo, item.manifest, item.version, header, events)
                )
            except OSError as exc:
                raise SnapshotReadError(f'{label}: unavailable file: {exc}', retryable=True) from exc
            except StatusResolutionError as exc:
                raise SnapshotReadError(f'{label}: {exc}', retryable=exc.retryable) from exc
            except (VersionConflict, PartitionFormatError) as exc:
                raise SnapshotReadError(f'{label}: {exc}') from exc
        return tuple(resolved)

    def close(self):
        self._close()


@contextmanager
def open_snapshot_read(repo, store, snapshot_id):
    """Pin the snapshot's physical layout for on-demand, repeatable reads.

    Entry fixes the layout and acquires read rights over the required extents.
    Inside the ``with`` block, parameter-free ``reader.read()`` calls return the
    same ordered rows as :func:`read_snapshot`, even if another process moves
    the last extents away and reclaims the old volumes before the body is read.
    """
    try:
        plan = plan_snapshot_read(repo, store, snapshot_id)
    except ReadPlanError as exc:
        raise SnapshotReadError(f'{snapshot_id}: {exc}', retryable=exc.retryable) from exc
    reader = _ProtectedSnapshotReader(repo, store, snapshot_id, plan)
    reader.acquire()
    try:
        yield reader
    finally:
        reader.close()


def read_snapshot(repo, store, snapshot_id, *, available=None, supplied=None):
    transaction = nullcontext() if repo.connection.in_transaction else repo.transaction(write=False)
    with transaction:
        try:
            plan = plan_snapshot_read(repo, store, snapshot_id, available=available, supplied=supplied)
        except ReadPlanError as exc:
            raise SnapshotReadError(f'{snapshot_id}: {exc}', retryable=exc.retryable) from exc
        resolved = []
        for item in plan.members:
            label = f'{snapshot_id}/{item.manifest.partition}/{item.manifest.version}'
            try:
                payload = store.read_logical_file(item.layout)
                header, events = verify_payload(item.version.descriptor, payload)
                resolved.extend(resolve_file_events(repo, item.manifest, item.version, header, events))
            except OSError as exc:
                raise SnapshotReadError(f'{label}: unavailable file: {exc}', retryable=True) from exc
            except StatusResolutionError as exc:
                raise SnapshotReadError(f'{label}: {exc}', retryable=exc.retryable) from exc
            except (VersionConflict, PartitionFormatError) as exc:
                raise SnapshotReadError(f'{label}: {exc}') from exc
        return tuple(resolved)
