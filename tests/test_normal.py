from copy import deepcopy
from decimal import Decimal, localcontext

from src.catalog.repository import open_repository
from src.catalog.snapshots import load_snapshot
from src.catalog.status_dictionary import load_current_revision, load_revision
from src.catalog.versions import load_version
from src.ingest.file_codec import decode_partition
from src.publish.manifest import decode_manifest
from src.recovery.replay import resume_publish
from src.recovery.requests import RequestState
from tests.support import WarehouseCase, evidence, groups, sources


class NormalTests(WarehouseCase):
    def test_publish_partition_with_status_dictionary(self):
        outcome = self.publish('S0')
        entry = load_snapshot(self.repo, outcome.result_ref)
        manifest = decode_manifest(entry.manifest)
        member = manifest.members[0]
        version = load_version(self.repo, '甲库', 'A1')
        layout = self.store.open_layout(self.repo, version.logical_id)
        header, rows = decode_partition(self.store.read_logical_file(layout))
        dictionary = load_revision(self.repo, '甲库', 1, expected_digest=header.dictionary_digest)
        self.assertEqual(member.generation, 1)
        self.assertEqual(header.version, 'A1')
        self.assertEqual(header.dictionary_digest, member.dictionary_digest)
        self.assertEqual(dictionary.entries, ((1, '运行'),))
        self.assertEqual([row.code for row in rows], [1, 1])
        self.assertEqual([row.sequence for row in rows], [10, 11])
        self.assertEqual(self.count('receipts'), 1)
        evidence('V1', self.sample['S0'], [1, 1], [row.code for row in rows], [10, 11],
                 [row.sequence for row in rows], ['src/ingest/partition_writer.py', 'src/publish/commit.py'],
                 '甲库第 1 代只有运行编码 1，采样原序为 10、11')

    def test_order_and_sequence(self):
        self.publish('S0')
        legal = deepcopy(self.sample['A2'])
        rows = deepcopy(self.sample['S0']['members'][0]['events'])
        for row in rows:
            row['status'] = '停机'
        legal['members'][0]['events'] = rows
        first = resume_publish(self.repo, self.store, 'ordered', self.credential, params=legal)
        self.assertEqual(first.state, RequestState.SUCCESS, first.reason)
        self.assertEqual([row.sequence for row in self.report('A2-only').summary.events], [10, 11])
        for name, reordered in [('equal', [rows[0], *rows]), ('reversed', list(reversed(rows)))]:
            with self.subTest(name=name):
                invalid = deepcopy(legal)
                invalid['snapshot'] = name
                invalid['members'][0]['version'] = name
                invalid['members'][0]['events'] = reordered
                outcome = resume_publish(self.repo, self.store, name, self.credential, params=invalid)
                self.assertEqual(outcome.state, RequestState.REJECTED)
                self.assertIn('non-increasing', outcome.reason)
        self.assertEqual(self.count('snapshots'), 2)
        self.assertEqual(self.count('versions'), 3)

    def test_invalid_source_or_value(self):
        for index, (field, value) in enumerate([
            ('source', ''), ('value', None), ('value', 'NaN'), ('value', 'Infinity'),
            ('partition', '外库'), ('key', ''), ('status', ''), ('sequence', True),
        ]):
            with self.subTest(field=field, value=value):
                params = deepcopy(self.sample['A2'])
                params['members'][0]['events'][0][field] = value
                result = resume_publish(self.repo, self.store, f'bad-{index}', self.credential, params=params)
                self.assertEqual(result.state, RequestState.REJECTED)
                self.assertIn(field, result.reason)
        self.assertEqual(self.count('snapshots'), 0)
        self.assertEqual(self.count('receipts'), 0)

    def test_dictionary_conflicts(self):
        self.publish('S0')
        for index, changes in enumerate([
            {'dictionary': [[1, '停机'], [1, '运行']]},
            {'generation': 1, 'dictionary': [[1, '停机']]},
            {'dictionary': [[2, '运行']]},
            {'version': 'A1'},
        ]):
            with self.subTest(changes=changes):
                params = deepcopy(self.sample['A2'])
                params['members'][0].update(changes)
                result = resume_publish(self.repo, self.store, f'dictionary-{index}', self.credential, params=params)
                self.assertEqual(result.state, RequestState.REJECTED)
                self.assertIsNone(result.receipt_id)
        self.assertEqual(self.count('dictionaries'), 2)
        self.assertEqual(self.count('versions'), 2)
        self.assertEqual(load_current_revision(self.repo, '甲库').ref.generation, 1)

    def test_encoding_generation(self):
        self.publish('S0')
        result = self.publish('A2')
        current = load_current_revision(self.repo, '甲库')
        self.assertEqual(current.ref.generation, 2)
        new = load_version(self.repo, '甲库', 'A2')
        old = load_version(self.repo, '甲库', 'A1')
        header, records = decode_partition(self.store.read_logical_file(self.store.open_layout(self.repo, new.logical_id)))
        manifest = decode_manifest(load_snapshot(self.repo, result.result_ref).manifest)
        self.assertEqual([header.generation, new.descriptor.generation, manifest.members[0].generation], [2, 2, 2])
        self.assertEqual([header.dictionary_digest, new.descriptor.dictionary_digest, manifest.members[0].dictionary_digest], [current.ref.digest] * 3)
        self.assertEqual(records[0].code, 1)
        self.assertEqual(old.descriptor.generation, 1)
        self.assertEqual(groups(self.report('A2-only', 'new')), {'停机': [1, '0.05']})
        self.assertEqual(groups(self.report('S0', 'old')), {'运行': [2, '4.30'], '停机': [1, '7.30']})
        params = deepcopy(self.sample['A2'])
        params['snapshot'] = 'A3-only'
        params['members'][0].update(version='A3', generation=current.ref.generation)
        del params['members'][0]['dictionary']
        reused = resume_publish(self.repo, self.store, 'pub-A3', self.credential, params=params)
        self.assertEqual(reused.state, RequestState.SUCCESS, reused.reason)
        self.assertEqual(self.count('dictionaries'), 3)

    def test_init_preserves_data(self):
        result = self.publish('S0')
        report = self.report('S0')
        self.repo.connection.execute('CREATE TABLE shift_notes (note TEXT)')
        self.repo.connection.execute("INSERT INTO shift_notes VALUES ('核对乙库采样 20')")
        with open_repository(self.root) as reopened:
            reopened.initialize()
            self.assertEqual(reopened.connection.execute('SELECT note FROM shift_notes').fetchone()[0], '核对乙库采样 20')
            self.assertEqual(load_snapshot(reopened, 'S0'), load_snapshot(self.repo, 'S0'))
            cached = resume_publish(reopened, self.store, result.request_id, self.credential, digest=result.digest)
            self.assertEqual(cached, result)
        self.assertEqual(self.report('S0'), report)
        self.assertEqual(self.count('snapshots'), 1)

    def test_cross_partition_status_aggregation(self):
        self.publish('S0')
        self.publish('A2')
        self.publish('S1')
        with localcontext() as context:
            context.prec = 1
            report = self.report('S1', available={('甲库', 'A1'), ('乙库', 'B2'), ('甲库', 'A2')})
        expected = {'运行': [3, '5.40'], '停机': [1, '5.60']}
        expected_sources = [
            ['甲库', 'A1', 10, 'A网关/10', '运行', '4.10'],
            ['甲库', 'A1', 11, 'A网关/11', '运行', '0.20'],
            ['乙库', 'B2', 21, 'B网关/21', '停机', '5.60'],
            ['乙库', 'B2', 22, 'B网关/22', '运行', '1.10'],
        ]
        self.assertEqual(groups(report), expected)
        self.assertEqual(sources(report), expected_sources)
        self.assertEqual([row.key for row in report.summary.events], ['A-冷机', 'A-冷机', 'B-冷机', 'B-冷机'])
        evidence('V2', self.sample['S1'], expected, groups(report), expected_sources, sources(report),
                 ['src/readers/read_plan.py', 'src/readers/dictionary_resolver.py', 'src/readers/aggregate.py', 'src/readers/reconcile.py'],
                 '运行按整数百分位相加为 410+20+110，停机只含采样 21 的 560')

    def test_empty_members_and_exact_signed_values(self):
        params = deepcopy(self.sample['S0'])
        params['snapshot'] = 'empty'
        for member in params['members']:
            member['events'] = []
        empty = resume_publish(self.repo, self.store, 'empty-pub', self.credential, params=params)
        self.assertEqual(empty.state, RequestState.SUCCESS, empty.reason)
        report = self.report('empty', 'empty-audit')
        self.assertEqual((report.summary.events, report.summary.totals), ((), ()))
        params['snapshot'] = 'precise'
        first = params['members'][0]
        first['version'] = 'A3'
        template = self.sample['S0']['members'][0]['events'][0]
        values = ['123456789012345678901234567890.01', '-123456789012345678901234567890', '0.000']
        first['events'] = [dict(template, value=value, sequence=index) for index, value in enumerate(values)]
        params['members'][1] = {'partition': '乙库', 'version': 'B1'}
        result = resume_publish(self.repo, self.store, 'precise-pub', self.credential, params=params)
        self.assertEqual(result.state, RequestState.SUCCESS, result.reason)
        with localcontext() as context:
            context.prec = 2
            result = self.report('precise', 'precise-audit')
        self.assertEqual(groups(result), {'运行': [3, '0.010']})
        self.assertEqual(result.summary.totals[0].watt_hours, Decimal('0.01'))
