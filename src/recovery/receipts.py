from contextlib import nullcontext
from dataclasses import dataclass
from hashlib import sha256
from hmac import compare_digest

from src.recovery.request_store import RequestStore
from src.recovery.requests import RequestState, canonical_json


class ClaimError(ValueError):
    pass


@dataclass(frozen=True)
class Receipt:
    request_id: str
    receipt_id: str
    kind: str
    digest: str
    result_ref: str


def credential_hash(credential):
    if not isinstance(credential, str) or not credential.strip():
        raise ClaimError('a nonempty claim credential is required')
    try:
        return sha256(credential.encode('utf-8')).hexdigest()
    except UnicodeError as exc:
        raise ClaimError('the claim credential is invalid') from exc


def issue_receipt(request, result_ref):
    identity = canonical_json([request.request_id, request.kind, request.digest, result_ref])
    receipt_id = sha256(identity.encode('utf-8')).hexdigest()
    return Receipt(request.request_id, receipt_id, request.kind, request.digest, result_ref)


def claim_receipt(repo, request_id, credential, *, digest=None, expected=None):
    transaction = nullcontext() if repo.connection.in_transaction else repo.transaction()
    with transaction:
        store = RequestStore(repo)
        request = store.lookup(request_id)
        credential_digest = credential_hash(credential)
        if request is None or not compare_digest(request.input.credential_digest, credential_digest):
            raise ClaimError('request or credential does not match')
        if digest is not None and digest != request.input.digest:
            raise ClaimError('claim parameter digest does not match')
        if request.state != RequestState.SUCCESS:
            raise ClaimError(f'request {request_id} has no successful receipt')
        receipt = issue_receipt(request.input, request.result_ref)
        row = store.load_receipt(request_id)
        if row is None or Receipt(**dict(row)) != receipt:
            raise ClaimError('stored receipt does not match the committed result')
        if expected is not None and expected != (receipt.kind, receipt.result_ref):
            raise ClaimError('receipt belongs to a different result')
        if receipt.kind == 'publish':
            query = 'SELECT 1 FROM snapshots WHERE snapshot_id = ?'
        else:
            query = 'SELECT 1 FROM reports WHERE report_id = ?'
        if repo.connection.execute(query, (receipt.result_ref,)).fetchone() is None:
            raise ClaimError('committed result is missing')
        if not compare_digest(store.record_claim(receipt.receipt_id, credential_digest), credential_digest):
            raise ClaimError('receipt was associated with a different credential')
        return receipt
