import sqlite3

from src.catalog.repository import open_repository
from src.catalog.snapshots import load_snapshot
from src.publish.compaction import compact_members
from src.readers.read_plan import plan_snapshot_read
from src.readers.reconcile import load_report, prepare_reconciliation
from src.recovery.replay import claim_historical_snapshot
from tests.support import WarehouseCase, evidence, groups, sources


class HistoryTests(WarehouseCase):
    def test_snapshot_stable_after_dictionary_rotation_and_compaction(self):
        first = self.publish('S0')
        old_report = self.report('S0', 'initial')
        pinned = plan_snapshot_read(self.repo, self.store, 'S0')
        originals = [self.store.read_logical_file(member.layout) for member in pinned.members]
        self.publish('A2')
        self.publish('S1')
        layouts = compact_members(self.repo, self.store, 'S0', segment_bytes=32)
        self.assertEqual(len({extent.object_name for layout in layouts for extent in layout.extents}), 1)
        for member, layout, original in zip(pinned.members, layouts, originals):
            self.assertGreater(len(layout.extents), 1)
            self.assertGreater(layout.extents[0].physical_offset, layout.extents[1].physical_offset)
            self.assertEqual(self.store.open_layout(self.repo, layout.logical_id), layout)
            self.assertEqual(self.store.read_logical_file(layout), original)
            self.assertEqual(self.store.read_logical_file(member.layout), original)
        with open_repository(self.root) as reopened:
            receipt, entry = claim_historical_snapshot(reopened, 'S0', first.request_id, self.credential)
            self.assertEqual(receipt.receipt_id, first.receipt_id)
            self.assertEqual(entry.publish_sequence, pinned.publish_sequence)
            self.assertEqual(load_report(reopened, 'initial'), old_report)
            after = prepare_reconciliation(reopened, self.store, 'S0', 'initial')
        self.assertEqual(after, old_report)
        expected = {'运行': [2, '4.30'], '停机': [1, '7.30']}
        self.assertEqual(groups(after), expected)
        self.assertEqual([row.sequence for row in after.summary.events], [10, 11, 20])
        evidence('V3', {'snapshot': 'S0', 'segment_bytes': 32, 'later': ['A2-only', 'S1']},
                 expected, groups(after), sources(old_report), sources(after),
                 ['src/publish/compaction.py', 'src/publish/storage.py', 'src/readers/snapshot_reader.py', 'src/recovery/replay.py'],
                 '手算 S0 原合计并逐字段比对压实前的独立持久报告和原文件字节')

    def test_failed_layout_switch(self):
        self.publish('S0')
        old = self.report('S0')
        snapshot = load_snapshot(self.repo, 'S0')
        pinned = plan_snapshot_read(self.repo, self.store, 'S0')
        original_objects = set(self.store.path.iterdir())
        old_refs = self.count('file_extents')
        rejected_id = pinned.members[1].version.logical_id
        self.repo.connection.execute(f'''
            CREATE TRIGGER reject_layout BEFORE DELETE ON file_extents
            WHEN OLD.logical_id = {rejected_id}
            BEGIN SELECT RAISE(ABORT, 'layout switch rejected'); END
        ''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'layout switch rejected'):
            compact_members(self.repo, self.store, 'S0', segment_bytes=32)
        self.assertEqual(plan_snapshot_read(self.repo, self.store, 'S0'), pinned)
        self.assertEqual(load_snapshot(self.repo, 'S0'), snapshot)
        self.assertEqual(set(self.store.path.iterdir()), original_objects)
        self.assertEqual(self.count('file_extents'), old_refs)
        self.assertEqual(prepare_reconciliation(self.repo, self.store, 'S0', old.report_id), old)
        self.repo.connection.execute('DROP TRIGGER reject_layout')
        changed = compact_members(self.repo, self.store, 'S0', segment_bytes=32)
        self.assertNotEqual(changed[0], pinned.members[0].layout)
        self.assertEqual(prepare_reconciliation(self.repo, self.store, 'S0', old.report_id), old)
