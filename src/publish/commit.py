from src.catalog.snapshots import insert_snapshot
from src.catalog.status_dictionary import register_revision
from src.catalog.versions import check_publishable_payload, register_version
from src.ingest.partition_writer import PreparedPartition
from src.publish.manifest import encode_manifest, freeze_manifest
from src.publish.storage import FileExtent, PhysicalLayout


class PublishError(OSError):
    pass


def stage_publish(repo, store, plan, owner):
    staged = {}
    try:
        for ordinal, member in enumerate(plan.members):
            if isinstance(member, PreparedPartition):
                staged[ordinal] = store.stage_file(owner, member.payload)
    except OSError as exc:
        discard_staged_publish(repo, store, staged)
        label = f'{member.descriptor.partition}/{member.descriptor.version}'
        raise PublishError(f'could not materialize {label}: {exc.strerror or exc}') from exc
    return staged


def commit_publish(repo, store, plan, staged):
    if not repo.connection.in_transaction:
        raise RuntimeError('publish requires a catalog transaction')
    for revision in plan.revisions:
        register_revision(repo, revision)
    registered_members = []
    for ordinal, member in enumerate(plan.members):
        if isinstance(member, PreparedPartition):
            extent = FileExtent(staged[ordinal], 0, 0, len(member.payload))
            payload = store.read_logical_file(PhysicalLayout(0, (extent,)))
            registered = register_version(repo, member.descriptor, payload)
            store.switch_layout(repo, PhysicalLayout(registered.logical_id, (extent,)))
        else:
            registered = member
            layout = store.open_layout(repo, registered.logical_id)
            check_publishable_payload(repo, registered.descriptor, store.read_logical_file(layout))
        registered_members.append(registered)
    manifest = freeze_manifest(plan.snapshot_id, registered_members)
    return insert_snapshot(repo, plan.snapshot_id, encode_manifest(manifest))


def discard_staged_publish(repo, store, staged):
    for object_name in staged.values():
        try:
            store.discard_stage(repo, object_name)
        except OSError:
            # A failed unlink leaves an invisible object for the next same-request cleanup.
            continue
