from dataclasses import dataclass

from src.catalog.versions import verify_payload
from src.publish.storage import FileExtent, PhysicalLayout
from src.readers.read_plan import plan_snapshot_read


@dataclass(frozen=True)
class CompactionPlan:
    snapshot_id: str
    files: tuple[tuple[int, bytes], ...]


def plan_compaction(repo, store, snapshot_id):
    read_plan = plan_snapshot_read(repo, store, snapshot_id)
    files = []
    for member in read_plan.members:
        data = store.read_logical_file(member.layout)
        verify_payload(member.version.descriptor, data)
        files.append((member.version.logical_id, data))
    return CompactionPlan(snapshot_id, tuple(files))


def _physical_segments(plan, segment_bytes):
    for logical_id, payload in reversed(plan.files):
        offsets = range(0, len(payload), segment_bytes)
        for offset in reversed(offsets):
            yield logical_id, offset, payload[offset:offset + segment_bytes]


def compact_members(repo, store, snapshot_id, *, segment_bytes=4096):
    if type(segment_bytes) is not int or segment_bytes <= 0:
        raise ValueError('segment_bytes must be a positive integer')
    object_name = None
    try:
        with repo.transaction():
            plan = plan_compaction(repo, store, snapshot_id)
            segments = tuple(_physical_segments(plan, segment_bytes))
            object_name = store.stage_file('compact', b''.join(part for _, _, part in segments))
            extents_by_file = {logical_id: [] for logical_id, _ in plan.files}
            physical_offset = 0
            for logical_id, logical_offset, data in segments:
                extents_by_file[logical_id].append(FileExtent(object_name, logical_offset, physical_offset, len(data)))
                physical_offset += len(data)
            new_layouts = []
            for logical_id, original in plan.files:
                extents = tuple(sorted(extents_by_file[logical_id], key=lambda item: item.logical_offset))
                layout = PhysicalLayout(logical_id, extents)
                if store.read_logical_file(layout) != original:
                    raise OSError(f'compacted file {logical_id} does not preserve original bytes')
                new_layouts.append(layout)
            for layout in new_layouts:
                store.switch_layout(repo, layout)
        return tuple(new_layouts)
    except BaseException:
        if object_name is not None:
            with repo.transaction():
                store.discard_stage(repo, object_name)
        raise
