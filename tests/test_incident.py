from src.catalog.repository import open_repository
from src.catalog.versions import load_version
from src.ingest.file_codec import decode_partition
from src.publish.compaction import compact_members
from src.readers.reconcile import prepare_reconciliation
from src.recovery.replay import claim_historical_snapshot
from tests.support import WarehouseCase, evidence, groups, sources


class IncidentTests(WarehouseCase):
    def test_local_dictionary_code_collision_is_not_merged(self):
        self.publish('S0')
        raw_codes = []
        for partition, version in [('甲库', 'A1'), ('乙库', 'B1')]:
            item = load_version(self.repo, partition, version)
            layout = self.store.open_layout(self.repo, item.logical_id)
            _, rows = decode_partition(self.store.read_logical_file(layout))
            raw_codes.append([row.code for row in rows])
        self.assertEqual(raw_codes, [[1, 1], [1]])
        report = self.report('S0', available={('甲库', 'A1'), ('乙库', 'B1')})
        expected = {'运行': [2, '4.30'], '停机': [1, '7.30']}
        expected_sources = [
            ['甲库', 'A1', 10, 'A网关/10', '运行', '4.10'],
            ['甲库', 'A1', 11, 'A网关/11', '运行', '0.20'],
            ['乙库', 'B1', 20, 'B网关/20', '停机', '7.30'],
        ]
        self.assertEqual(groups(report), expected)
        self.assertEqual(sources(report), expected_sources)
        evidence('V7', {'snapshot': 'S0', 'codes_in_files': raw_codes}, expected, groups(report),
                 expected_sources, sources(report), ['src/readers/dictionary_resolver.py', 'src/readers/aggregate.py'],
                 '两区的编码 1 分别对照自己的字典，甲两笔合计 4.30，乙一笔 7.30')

    def test_old_snapshot_keeps_original_status_names(self):
        published = self.publish('S0')
        old = self.report('S0', 'first-night')
        first, _ = claim_historical_snapshot(self.repo, 'S0', published.request_id, self.credential)
        self.publish('A2')
        self.publish('S1')
        compact_members(self.repo, self.store, 'S0', segment_bytes=47)
        with open_repository(self.root) as reopened:
            receipt, snapshot = claim_historical_snapshot(reopened, 'S0', published.request_id, self.credential, digest=published.digest)
            again = prepare_reconciliation(reopened, self.store, snapshot.snapshot_id, old.report_id)
        self.assertEqual(receipt, first)
        self.assertEqual(receipt.result_ref, 'S0')
        self.assertEqual(again, old)
        expected = {'运行': [2, '4.30'], '停机': [1, '7.30']}
        self.assertEqual(groups(again), expected)
        self.assertEqual([(row.partition, row.sequence, row.status) for row in again.summary.events], [
            ('甲库', 10, '运行'), ('甲库', 11, '运行'), ('乙库', 20, '停机'),
        ])
        evidence('V8', {'snapshot': 'S0', 'after_rotation': ['A2', 'B2']}, expected, groups(again),
                 'S0', receipt.result_ref, ['src/catalog/status_dictionary.py', 'src/readers/snapshot_reader.py', 'src/recovery/replay.py'],
                 '原回执固定 S0，甲库编码 1 继续按甲库第 1 代解释为运行')
