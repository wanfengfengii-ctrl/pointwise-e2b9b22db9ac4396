import unittest

from src.publish.compaction import compact_members
from src.publish.storage import ArtifactStore, collect_retired
from src.readers.snapshot_reader import SnapshotReadError, open_snapshot_read, read_snapshot
from src.recovery.replay import claim_historical_snapshot
from src.recovery.requests import RequestState
from tests.support import WarehouseCase


EXPECTED = [(10, "运行", "4.10", "A网关/10"), (11, "运行", "0.20", "A网关/11"),
            (20, "停机", "7.30", "B网关/20")]


def signature(rows):
    return [(row.sequence, row.status, str(row.value), row.source) for row in rows]


def object_names(layouts):
    return {extent.object_name for layout in layouts for extent in layout.extents}


def blob_names(store):
    return {path.name for path in store.path.glob('*.blob')}


class RetirementWarehouseCase(WarehouseCase):
    def publish_snapshots(self):
        result = self.publish('S0')
        for name, spec in zip(('A-only', 'B-only'), self.sample['S0']['members']):
            member = {key: spec[key] for key in ('partition', 'version')}
            derived = self._derive(name, member)
            self.assertEqual(derived.state, RequestState.SUCCESS, derived.reason)
        return result

    def _derive(self, name, member):
        from src.recovery.replay import resume_publish
        return resume_publish(self.repo, self.store, f'pub-{name}', self.credential,
                              params={'snapshot': name, 'members': [member]})

    def move(self, snapshot, segment_bytes=32):
        return object_names(compact_members(self.repo, self.store, snapshot, segment_bytes=segment_bytes))


class ProtectedReadTests(RetirementWarehouseCase):
    def test_repeated_reads_match_plain_reader_and_keep_order(self):
        self.publish_snapshots()
        with open_snapshot_read(self.repo, self.store, 'S0') as reader:
            first = signature(reader.read())
            second = signature(reader.read())
        self.assertEqual(first, EXPECTED)
        self.assertEqual(second, EXPECTED)
        self.assertEqual(signature(read_snapshot(self.repo, self.store, 'S0')), EXPECTED)

    def test_layout_pinned_while_other_process_reclaims_old_volumes(self):
        publish = self.publish_snapshots()
        shared, = self.move('S0')
        self.move('A-only')
        self.move('B-only')
        with open_snapshot_read(self.repo, self.store, 'B-only') as reader:
            pinned_first = signature(reader.read())
            reclaimed = collect_retired(self.repo, self.store)
            # The shared volume backing the pinned read is gone from the path ...
            self.assertIn(shared, reclaimed)
            self.assertFalse((self.store.path / shared).exists())
            # ... yet the entered reader repeats the original B1 detail in order.
            pinned_second = signature(reader.read())
        self.assertEqual(pinned_first, EXPECTED[2:])
        self.assertEqual(pinned_second, EXPECTED[2:])
        # A fresh reader uses the new layout and gets the same detail.
        self.assertEqual(signature(read_snapshot(self.repo, self.store, 'S0')), EXPECTED)
        # The original credential still claims the original receipt.
        receipt, entry = claim_historical_snapshot(
            self.repo, 'S0', publish.request_id, self.credential, digest=publish.digest)
        self.assertEqual((receipt.receipt_id, entry.snapshot_id), (publish.receipt_id, 'S0'))

    def test_entry_missing_file_is_retryable_and_closed_scope_refuses_reads(self):
        self.publish_snapshots()
        layouts = compact_members(self.repo, self.store, 'S0', segment_bytes=64)
        name, = object_names(layouts)
        (self.store.path / name).unlink()
        with self.assertRaises(SnapshotReadError) as caught:
            open_snapshot_read(self.repo, self.store, 'S0').__enter__()
        self.assertTrue(caught.exception.retryable)
        with self.assertRaises(SnapshotReadError):
            with open_snapshot_read(self.repo, self.store, 'B-only') as reader:
                reader.close()
                reader.read()


class CollectRetiredTests(RetirementWarehouseCase):
    def test_nothing_retired_means_empty_run(self):
        self.publish_snapshots()
        self.assertEqual(collect_retired(self.repo, self.store), [])

    def test_reclaims_orphaned_volumes_but_keeps_staged_and_referenced(self):
        self.publish_snapshots()
        before = blob_names(self.store)
        new_names = self.move('S0')
        staged = self.store.stage_file('fixture', b'keep-me')
        reclaimed = collect_retired(self.repo, self.store)
        # Every pre-compaction volume lost its extents and is now collectible.
        self.assertEqual(set(reclaimed), before - new_names)
        for name in reclaimed:
            self.assertFalse((self.store.path / name).exists())
        # Staged-only files and currently referenced volumes are untouched.
        self.assertTrue((self.store.path / staged).is_file())
        for name in new_names:
            self.assertTrue((self.store.path / name).is_file())
        self.assertEqual(collect_retired(self.repo, self.store), [])

    def test_shared_volume_protected_until_last_logical_file_leaves(self):
        self.publish_snapshots()
        shared, = self.move('S0')
        self.move('A-only')
        # The original publish blobs are orphans and may go, but the shared
        # compacted volume still holds B1 and therefore must be retained.
        reclaimed = collect_retired(self.repo, self.store)
        self.assertNotIn(shared, reclaimed)
        self.assertTrue((self.store.path / shared).is_file())
        self.move('B-only')
        reclaimed = collect_retired(self.repo, self.store)
        self.assertIn(shared, reclaimed)
        self.assertFalse((self.store.path / shared).exists())
        self.assertEqual(collect_retired(self.repo, self.store), [])
        self.assertEqual(signature(read_snapshot(self.repo, self.store, 'S0')), EXPECTED)

    def test_interrupted_cleanup_resumes_and_converges(self):
        self.publish_snapshots()
        before = blob_names(self.store)
        new_names = self.move('S0')
        orphans = before - new_names
        # Simulate a process that unlinked one volume but died before deregistering.
        interrupted = next(iter(orphans))
        (self.store.path / interrupted).unlink()
        resumed = collect_retired(self.repo, self.store)
        self.assertEqual(set(resumed), orphans)
        self.assertEqual(collect_retired(self.repo, self.store), [])
        # The catalog no longer advertises any retired volume, results stay readable.
        for name in orphans:
            row = self.repo.connection.execute(
                'SELECT 1 FROM artifact_objects WHERE object_name = ?', (name,)).fetchone()
            self.assertIsNone(row)
        self.assertEqual(signature(read_snapshot(self.repo, self.store, 'S0')), EXPECTED)


if __name__ == '__main__':
    unittest.main()
