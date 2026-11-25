import json
from copy import deepcopy
from hashlib import sha256

from src.catalog.snapshots import SnapshotNotFound, load_snapshot
from src.catalog.versions import VersionMissing, load_version
from src.readers.reconcile import load_report
from src.readers.snapshot_reader import SnapshotReadError, read_snapshot
from src.recovery.receipts import ClaimError, claim_receipt
from src.recovery.replay import resume_publish, resume_reconcile
from src.recovery.requests import RequestState
from tests.support import WarehouseCase, evidence


class FailureTests(WarehouseCase):
    def _alter_isolated_b1(self, change):
        self.publish('S0')
        self.report('S0', 'healthy')
        version = load_version(self.repo, '乙库', 'B1')
        layout = self.store.open_layout(self.repo, version.logical_id)
        extent = layout.extents[0]
        document = json.loads(self.store.read_logical_file(layout))
        change(document)
        altered = json.dumps(document, ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode('utf-8')
        digest = sha256(altered).hexdigest()
        (self.store.path / extent.object_name).write_bytes(altered)
        manifest = json.loads(load_snapshot(self.repo, 'S0').manifest)
        manifest['members'][1]['digest'] = digest
        serialized = json.dumps(manifest, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
        with self.repo.transaction() as connection:
            connection.execute('UPDATE versions SET digest = ? WHERE logical_id = ?', (digest, version.logical_id))
            connection.execute('UPDATE file_extents SET byte_length = ? WHERE logical_id = ?', (len(altered), version.logical_id))
            connection.execute('UPDATE snapshots SET manifest = ?, manifest_digest = ? WHERE snapshot_id = ?', (
                serialized, sha256(serialized.encode('utf-8')).hexdigest(), 'S0',
            ))

    def _assert_no_new_report(self, request, receipt_count):
        self.assertEqual(self.count('reports'), 1)
        self.assertEqual(self.count('receipts'), receipt_count)
        self.assertEqual(load_report(self.repo, 'healthy').snapshot_id, 'S0')
        with self.assertRaises(LookupError):
            load_report(self.repo, request)
        with self.assertRaises(ClaimError):
            claim_receipt(self.repo, request, self.credential)

    def test_missing_or_conflicting_dictionary_rejects_whole_read(self):
        self.publish('S0')
        original = self.report('S0', 'healthy')
        count = self.count('receipts')
        connection = self.repo.connection
        revision = tuple(connection.execute("SELECT * FROM dictionaries WHERE partition = '乙库'").fetchone())
        connection.execute('PRAGMA foreign_keys = OFF')
        connection.execute("DELETE FROM dictionaries WHERE partition = '乙库' AND generation = 1")
        connection.execute('PRAGMA foreign_keys = ON')
        with self.assertRaisesRegex(SnapshotReadError, 'missing dictionary'):
            read_snapshot(self.repo, self.store, 'S0')
        missing = self.audit('S0', 'missing')
        self.assertEqual(missing.state, RequestState.RETRY)
        self.assertIn('missing dictionary', missing.reason)
        self._assert_no_new_report('missing', count)
        connection.execute('INSERT INTO dictionaries VALUES (?, ?, ?, ?)', revision)
        connection.execute("UPDATE dictionaries SET body = '[[1,\"运行\"]]' WHERE partition = '乙库'")
        conflicting = self.audit('S0', 'conflicting')
        self.assertEqual(conflicting.state, RequestState.REJECTED)
        self.assertIn('dictionary digest mismatch', conflicting.reason)
        self._assert_no_new_report('conflicting', count)
        self.assertEqual(load_report(self.repo, 'healthy'), original)
        evidence('V4', {'snapshot': 'S0', 'second_dictionary': ['missing', 'changed']},
                 ['retry', 'rejected'], [missing.state, conflicting.state], 1, self.count('reports'),
                 ['src/readers/dictionary_resolver.py', 'src/readers/snapshot_reader.py', 'src/readers/reconcile.py'],
                 '两次缺证执行都只能保留已签发的原报告，不能签出半份状态账')
        connection.execute('UPDATE dictionaries SET body = ? WHERE partition = ?', (revision[2], '乙库'))
        resumed = resume_reconcile(self.repo, self.store, 'missing', self.credential, digest=missing.digest)
        self.assertEqual(resumed.state, RequestState.SUCCESS, resumed.reason)
        self.assertEqual(load_report(self.repo, 'missing').summary, original.summary)

    def test_foreign_member_and_unavailable_file(self):
        self.publish('S0')
        self.report('S0', 'healthy')
        self.publish('S1')
        count = self.count('receipts')
        wrong = [('甲库', 'A1'), ('乙库', 'B2')]
        foreign = self.audit('S0', 'foreign', supplied=wrong)
        self.assertEqual(foreign.state, RequestState.RETRY)
        self.assertIn('supplied members', foreign.reason)
        self._assert_no_new_report('foreign', count)
        missing = self.audit('S0', 'missing-member', available=wrong)
        self.assertEqual(missing.state, RequestState.RETRY)
        self.assertIn('乙库/B1', missing.reason)
        self._assert_no_new_report('missing-member', count)
        member = load_version(self.repo, '乙库', 'B1')
        extent, = self.store.open_layout(self.repo, member.logical_id).extents
        path = self.store.path / extent.object_name
        path.rename(path.with_suffix('.offline'))
        unavailable = self.audit('S0', 'no-file')
        self.assertEqual(unavailable.state, RequestState.RETRY)
        self.assertIn('unavailable file', unavailable.reason)
        self._assert_no_new_report('no-file', count)

    def test_unknown_code_rejects_even_after_valid_first_member(self):
        self._alter_isolated_b1(lambda document: document['rows'][0].update(code=9))
        count = self.count('receipts')
        result = self.audit('S0', 'unknown')
        self.assertEqual(result.state, RequestState.REJECTED)
        self.assertIn('unknown code 9', result.reason)
        self._assert_no_new_report('unknown', count)
        replay = resume_reconcile(self.repo, self.store, 'unknown', self.credential, digest=result.digest)
        self.assertEqual(replay, result)

    def test_version_conflict_with_matching_digest(self):
        self._alter_isolated_b1(lambda document: document['header'].update(version='B2'))
        count = self.count('receipts')
        result = self.audit('S0', 'version-conflict')
        self.assertEqual(result.state, RequestState.REJECTED)
        self.assertIn('header disagrees', result.reason)
        self._assert_no_new_report('version-conflict', count)

    def test_publish_rolls_back_when_metadata_commit_fails(self):
        self.repo.connection.execute('''
            CREATE TRIGGER fail_receipt BEFORE INSERT ON receipts
            BEGIN SELECT RAISE(ABORT, 'receipt registration rejected'); END
        ''')
        result = resume_publish(self.repo, self.store, 'rollback', self.credential, params=self.sample['S0'])
        self.assertEqual(result.state, RequestState.RETRY)
        self.assertIn('receipt registration rejected', result.reason)
        with self.assertRaises(SnapshotNotFound):
            load_snapshot(self.repo, 'S0')
        with self.assertRaises(VersionMissing):
            load_version(self.repo, '甲库', 'A1')
        observed_counts = [self.count(table) for table in ('snapshots', 'versions', 'dictionaries', 'receipts', 'file_extents')]
        self.assertEqual(observed_counts, [0, 0, 0, 0, 0])
        self.assertEqual(list(self.store.path.iterdir()), [])
        self.repo.connection.execute('DROP TRIGGER fail_receipt')
        resumed = resume_publish(self.repo, self.store, 'rollback', self.credential, digest=result.digest)
        self.assertEqual(resumed.state, RequestState.SUCCESS, resumed.reason)
        self.assertEqual(resumed.result_ref, 'S0')
        self.assertEqual(self.count('snapshots'), 1)
        evidence('V5', {'snapshot': 'S0', 'failed_write': 'receipts'}, [0, 0, 0, 0, 0], observed_counts,
                 'S0', resumed.result_ref, ['src/publish/commit.py', 'src/catalog/repository.py', 'src/recovery/replay.py'],
                 'SQLite 在所有文件和目录项准备后拒绝回执，提交前的五类业务记录必须全部回滚')

    def test_file_write_failure_remains_retriable(self):
        moved = self.root / 'objects-backup'
        self.store.path.rename(moved)
        self.store.path.write_text('blocked object directory', encoding='utf-8')
        params = deepcopy(self.sample['S0'])
        result = resume_publish(self.repo, self.store, 'blocked', self.credential, params=params)
        self.assertEqual(result.state, RequestState.RETRY)
        self.assertIn('could not materialize', result.reason)
        self.assertEqual(self.count('snapshots'), 0)
        self.assertEqual(self.count('receipts'), 0)
        self.store.path.unlink()
        moved.rename(self.store.path)
        retried = resume_publish(self.repo, self.store, 'blocked', self.credential, digest=result.digest)
        self.assertEqual(retried.state, RequestState.SUCCESS, retried.reason)
        self.assertEqual(len(list(self.store.path.iterdir())), 2)
