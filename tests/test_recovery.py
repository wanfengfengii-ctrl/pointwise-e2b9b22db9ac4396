from copy import deepcopy
from decimal import Decimal
from hashlib import sha256

from src.catalog.repository import open_repository
from src.catalog.snapshots import load_snapshot
from src.readers.reconcile import load_report
from src.recovery.idempotency import RequestConflict, open_request
from src.recovery.receipts import ClaimError, claim_receipt
from src.recovery.replay import claim_historical_snapshot, resume_publish, resume_reconcile
from src.recovery.request_store import RequestStore
from src.recovery.requests import RequestState
from tests.support import WarehouseCase, evidence


class RecoveryTests(WarehouseCase):
    def test_same_request_replays_and_different_request_is_rejected(self):
        original = self.publish('S0')
        equivalent = deepcopy(self.sample['S0'])
        equivalent['members'][0]['events'][0]['value'] = Decimal('4.1000')
        replayed = resume_publish(self.repo, self.store, original.request_id, self.credential, params=equivalent)
        self.assertEqual(replayed, original)
        altered = deepcopy(equivalent)
        altered['members'][0]['events'][0]['value'] = Decimal('4.11')
        with self.assertRaises(RequestConflict):
            resume_publish(self.repo, self.store, original.request_id, self.credential, params=altered)
        self.assertEqual(self.count('snapshots'), 1)
        self.assertEqual(self.count('receipts'), 1)
        self.assertEqual(self.count('claims'), 1)
        report = self.report('S0')
        self.assertEqual(str(report.summary.events[0].value), '4.10')
        evidence('V6', {'same_id_values': ['4.10', '4.1000', '4.11']}, 'S0', replayed.result_ref,
                 '4.10', str(report.summary.events[0].value),
                 ['src/recovery/requests.py', 'src/recovery/idempotency.py', 'src/recovery/replay.py'],
                 '等值十进制应复领原件，变为 4.11 属于异参，原采样仍是 4.10')

    def test_retry_pending_uses_original_parameters_and_cleans_own_orphans(self):
        with self.repo.transaction():
            pending = open_request(self.repo, 'pending', 'publish', self.credential, params=self.sample['S0'])
            owner = 'r-' + sha256(b'pending').hexdigest()
            orphan = self.store.stage_file(owner, b'abandoned by an interrupted operation')
            other = self.store.stage_file('r-unrelated', b'belongs to a different operation')
        self.assertEqual(pending.state, RequestState.PENDING)
        self.assertEqual(self.count('receipts'), 0)
        with open_repository(self.root) as reopened:
            result = resume_publish(reopened, self.store, 'pending', self.credential, digest=pending.input.digest)
        self.assertEqual(result.state, RequestState.SUCCESS, result.reason)
        self.assertEqual(result.result_ref, 'S0')
        self.assertFalse((self.store.path / orphan).exists())
        self.assertTrue((self.store.path / other).exists())
        self.assertEqual(self.count('versions'), 2)
        self.assertEqual(self.count('snapshots'), 1)

    def test_lost_response_for_publish_and_reconciliation(self):
        with open_repository(self.root) as sender:
            resume_publish(sender, self.store, 'lost-publish', self.credential, params=self.sample['S0'])
            resume_reconcile(sender, self.store, 'lost-audit', self.credential, params={'snapshot': 'S0'})
        request_store = RequestStore(self.repo)
        publication = dict(request_store.load_receipt('lost-publish'))
        audit = dict(request_store.load_receipt('lost-audit'))
        self.assertEqual(self.count('claims'), 0)
        entry = load_snapshot(self.repo, 'S0')
        original = load_report(self.repo, 'lost-audit')
        with open_repository(self.root) as receiver:
            recovered = resume_publish(receiver, self.store, 'lost-publish', self.credential, digest=publication['digest'])
            recovered_audit = resume_reconcile(receiver, self.store, 'lost-audit', self.credential, digest=audit['digest'])
        self.assertEqual(recovered.receipt_id, publication['receipt_id'])
        self.assertEqual(recovered_audit.receipt_id, audit['receipt_id'])
        self.assertEqual(recovered_audit.result_ref, 'lost-audit')
        self.assertEqual(load_snapshot(self.repo, recovered.result_ref), entry)
        self.assertEqual(load_report(self.repo, recovered_audit.result_ref), original)
        self.assertEqual(self.count('snapshots'), 1)
        self.assertEqual(self.count('reports'), 1)
        self.assertEqual(self.count('receipts'), 2)
        self.assertEqual(self.count('claims'), 2)

    def test_invalid_credential_and_wrong_claim_target(self):
        result = self.publish('S0')
        with self.assertRaises(ClaimError):
            claim_receipt(self.repo, result.request_id, 'incorrect')
        with self.assertRaises(ClaimError):
            resume_publish(self.repo, self.store, result.request_id, 'incorrect', digest=result.digest)
        with self.assertRaises(ClaimError):
            claim_historical_snapshot(self.repo, 'S1', result.request_id, self.credential)
        with self.assertRaises(ClaimError):
            claim_receipt(self.repo, result.request_id, self.credential, digest='wrong')
        self.assertEqual(self.count('claims'), 0)
        first = claim_receipt(self.repo, result.request_id, self.credential)
        self.assertEqual(first, claim_receipt(self.repo, result.request_id, self.credential))
        self.assertEqual(self.count('claims'), 1)
        retry = self.audit('missing-snapshot', 'no-outcome')
        self.assertEqual(retry.state, RequestState.RETRY)
        with self.assertRaises(ClaimError):
            claim_receipt(self.repo, 'no-outcome', self.credential)
