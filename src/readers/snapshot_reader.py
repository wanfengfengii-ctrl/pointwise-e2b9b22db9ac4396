from contextlib import nullcontext

from src.catalog.versions import VersionConflict, verify_payload
from src.ingest.file_codec import PartitionFormatError
from src.readers.dictionary_resolver import StatusResolutionError, resolve_file_events
from src.readers.read_plan import ReadPlanError, plan_snapshot_read


class SnapshotReadError(ValueError):
    def __init__(self, reason, *, retryable=False):
        super().__init__(reason)
        self.retryable = retryable


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
