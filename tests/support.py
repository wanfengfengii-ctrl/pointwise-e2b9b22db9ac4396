import json
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from src.catalog.repository import open_repository
from src.publish.storage import ArtifactStore
from src.readers.reconcile import load_report
from src.recovery.replay import resume_publish, resume_reconcile
from src.recovery.requests import RequestState


def groups(report):
    return {item.status: [item.count, str(item.watt_hours)] for item in report.summary.totals}


def sources(report):
    return [[row.partition, row.version, row.sequence, row.source, row.status, str(row.value)] for row in report.summary.events]


def evidence(case_id, given, expected, actual, downstream_expected, downstream_actual, paths, oracle):
    output = {
        'case_id': case_id, 'input': given, 'expected': expected, 'actual': actual,
        'downstream_expected': downstream_expected, 'downstream_actual': downstream_actual,
        'code_paths': paths, 'oracle': oracle,
    }
    print('POINTWISE_SCENARIO ' + json.dumps(output, ensure_ascii=False, default=str, sort_keys=True))


class WarehouseCase(unittest.TestCase):
    def setUp(self):
        workspace = TemporaryDirectory(prefix='qingyu-test-')
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        self.repo = open_repository(self.root)
        self.addCleanup(self.repo.close)
        self.store = ArtifactStore(self.root)
        path = Path(__file__).resolve().parents[1] / 'samples' / 'workday.json'
        self.sample = json.loads(path.read_text(encoding='utf-8'), parse_float=Decimal)
        self.credential = self.sample['credential']

    def publish(self, name):
        result = resume_publish(self.repo, self.store, f'pub-{name}', self.credential, params=self.sample[name])
        self.assertEqual(result.state, RequestState.SUCCESS, result.reason)
        return result

    def audit(self, snapshot, request='night', **options):
        return resume_reconcile(self.repo, self.store, request, self.credential, params={'snapshot': snapshot}, **options)

    def report(self, snapshot, request='night', **options):
        result = self.audit(snapshot, request, **options)
        self.assertEqual(result.state, RequestState.SUCCESS, result.reason)
        return load_report(self.repo, result.result_ref)

    def count(self, table):
        self.assertIn(table, {'snapshots', 'versions', 'dictionaries', 'reports', 'receipts', 'claims', 'file_extents'})
        return self.repo.connection.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
