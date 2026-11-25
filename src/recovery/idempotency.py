from hmac import compare_digest

from src.ingest.validation import require_text
from src.recovery.receipts import ClaimError, credential_hash, issue_receipt
from src.recovery.request_store import RequestStore
from src.recovery.requests import RequestInput, RequestOutcome, RequestState, json_parameters, parameter_digest


class RequestConflict(ValueError):
    pass


def open_request(repo, request_id, kind, credential, *, params=None, digest=None):
    require_text(request_id, 'request id')
    if kind not in {'publish', 'reconcile'}:
        raise ValueError('unsupported request kind')
    credential_digest = credential_hash(credential)
    store = RequestStore(repo)
    existing = store.lookup(request_id)
    if existing is not None and not compare_digest(existing.input.credential_digest, credential_digest):
        raise ClaimError('request credential does not match')
    if params is not None:
        if not isinstance(params, dict):
            raise ValueError('request parameters must be an object')
        params = json_parameters(params)
        calculated = parameter_digest(kind, params)
        if digest is not None and digest != calculated:
            raise RequestConflict('supplied digest differs from supplied parameters')
        digest = calculated
    if existing is None:
        if params is None:
            raise ClaimError('no stored request to resume')
        return store.insert_pending(RequestInput(request_id, kind, params, digest, credential_digest))
    if existing.input.kind != kind or digest != existing.input.digest:
        raise RequestConflict(f'request {request_id} has different operation or parameters')
    return existing


def request_outcome(repo, stored):
    receipt = RequestStore(repo).load_receipt(stored.input.request_id)
    return RequestOutcome(
        stored.input.request_id, stored.input.kind, stored.state, stored.input.digest,
        stored.result_ref, receipt['receipt_id'] if receipt is not None else None, stored.reason,
    )


def complete_request(repo, request, result_ref):
    store = RequestStore(repo)
    current = store.lookup(request.input.request_id)
    if current is None or current.input != request.input:
        raise RequestConflict('request changed before business commit')
    if current.state not in {RequestState.PENDING, RequestState.RETRY}:
        raise RequestConflict('request already has a terminal outcome')
    final = store.save_outcome(request.input.request_id, RequestState.SUCCESS, result_ref=result_ref)
    store.save_receipt(issue_receipt(final.input, result_ref))
    return request_outcome(repo, final)


def record_request_failure(repo, request_id, reason, *, retryable):
    store = RequestStore(repo)
    current = store.lookup(request_id)
    if current.state in {RequestState.PENDING, RequestState.RETRY}:
        state = RequestState.RETRY if retryable else RequestState.REJECTED
        current = store.save_outcome(request_id, state, reason=reason)
    return request_outcome(repo, current)
