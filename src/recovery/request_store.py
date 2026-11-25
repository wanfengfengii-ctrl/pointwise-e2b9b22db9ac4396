import json
from dataclasses import dataclass

from src.recovery.requests import RequestInput, RequestState, canonical_json


@dataclass(frozen=True)
class StoredRequest:
    input: RequestInput
    state: RequestState
    result_ref: str | None
    reason: str | None


class RequestStore:
    def __init__(self, repo):
        self.connection = repo.connection

    def _require_transaction(self):
        if not self.connection.in_transaction:
            raise RuntimeError('request updates require a catalog transaction')

    def lookup(self, request_id):
        row = self.connection.execute('SELECT * FROM requests WHERE request_id = ?', (request_id,)).fetchone()
        if row is None:
            return None
        request = RequestInput(
            row['request_id'], row['kind'], json.loads(row['params']), row['digest'], row['credential_digest'],
        )
        return StoredRequest(request, RequestState(row['state']), row['result_ref'], row['reason'])

    def insert_pending(self, request):
        self._require_transaction()
        self.connection.execute('''
            INSERT INTO requests(request_id, kind, params, digest, credential_digest, state)
            VALUES (?, ?, ?, ?, ?, 'pending')
        ''', (request.request_id, request.kind, canonical_json(request.params), request.digest, request.credential_digest))
        return StoredRequest(request, RequestState.PENDING, None, None)

    def save_outcome(self, request_id, state, *, result_ref=None, reason=None):
        self._require_transaction()
        updated = self.connection.execute('''
            UPDATE requests SET state = ?, result_ref = ?, reason = ?
            WHERE request_id = ? AND state IN ('pending', 'retry')
        ''', (state, result_ref, reason, request_id))
        if updated.rowcount != 1:
            raise RuntimeError(f'request is no longer pending: {request_id}')
        return self.lookup(request_id)

    def load_receipt(self, request_id):
        return self.connection.execute('SELECT * FROM receipts WHERE request_id = ?', (request_id,)).fetchone()

    def save_receipt(self, receipt):
        self._require_transaction()
        self.connection.execute('INSERT INTO receipts VALUES (?, ?, ?, ?, ?)', (
            receipt.request_id, receipt.receipt_id, receipt.kind, receipt.digest, receipt.result_ref,
        ))

    def record_claim(self, receipt_id, credential_digest):
        self._require_transaction()
        self.connection.execute('INSERT OR IGNORE INTO claims VALUES (?, ?)', (receipt_id, credential_digest))
        row = self.connection.execute('SELECT credential_digest FROM claims WHERE receipt_id = ?', (receipt_id,)).fetchone()
        return row['credential_digest']
