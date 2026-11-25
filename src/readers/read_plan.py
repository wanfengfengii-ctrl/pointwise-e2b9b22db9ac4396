from contextlib import nullcontext
from dataclasses import dataclass

from src.catalog.snapshots import SnapshotNotFound, load_snapshot
from src.catalog.versions import PartitionVersion, VersionMissing, load_version
from src.publish.manifest import ManifestMember, decode_manifest, manifest_member
from src.publish.storage import PhysicalLayout


class ReadPlanError(ValueError):
    def __init__(self, reason, *, retryable=False):
        super().__init__(reason)
        self.retryable = retryable


@dataclass(frozen=True)
class ReadMember:
    manifest: ManifestMember
    version: PartitionVersion
    layout: PhysicalLayout


@dataclass(frozen=True)
class ReadPlan:
    snapshot_id: str
    publish_sequence: int
    members: tuple[ReadMember, ...]


def plan_snapshot_read(repo, store, snapshot_id, *, available=None, supplied=None):
    transaction = nullcontext() if repo.connection.in_transaction else repo.transaction(write=False)
    with transaction:
        try:
            entry = load_snapshot(repo, snapshot_id)
            manifest = decode_manifest(entry.manifest)
        except SnapshotNotFound as exc:
            raise ReadPlanError(str(exc), retryable=True) from exc
        except ValueError as exc:
            raise ReadPlanError(str(exc)) from exc
        if manifest.snapshot_id != snapshot_id:
            raise ReadPlanError('manifest is for another snapshot')
        expected = tuple((member.partition, member.version) for member in manifest.members)
        if supplied is not None and tuple(supplied) != expected:
            raise ReadPlanError('supplied members differ from the fixed manifest', retryable=True)
        inventory = set(available) if available is not None else None
        members = []
        for member in manifest.members:
            key = member.partition, member.version
            if inventory is not None and key not in inventory:
                raise ReadPlanError(f'missing required member {key[0]}/{key[1]}', retryable=True)
            try:
                version = load_version(repo, *key)
            except VersionMissing as exc:
                raise ReadPlanError(str(exc), retryable=True) from exc
            if manifest_member(member.ordinal, version) != member:
                raise ReadPlanError(f'manifest and catalog disagree for {key[0]}/{key[1]}')
            try:
                layout = store.open_layout(repo, version.logical_id)
            except OSError as exc:
                raise ReadPlanError(str(exc), retryable=True) from exc
            members.append(ReadMember(member, version, layout))
        return ReadPlan(snapshot_id, entry.publish_sequence, tuple(members))
