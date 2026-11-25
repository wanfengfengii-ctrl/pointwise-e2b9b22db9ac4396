import sqlite3
from hashlib import sha256

from src.catalog.snapshots import SnapshotConflict, SnapshotNotFound, load_snapshot
from src.catalog.status_dictionary import MissingDictionary
from src.catalog.versions import VersionMissing
from src.ingest.validation import require_text
from src.publish.commit import commit_publish, discard_staged_publish, stage_publish
from src.publish.planner import build_publish_plan
from src.readers.reconcile import prepare_reconciliation, save_report
from src.readers.snapshot_reader import SnapshotReadError
from src.recovery.idempotency import complete_request, open_request, record_request_failure, request_outcome
from src.recovery.receipts import ClaimError, claim_receipt
from src.recovery.request_store import RequestStore
from src.recovery.requests import RequestState


def _resume(repo, request_id, kind, credential, params, digest, execute, discard):
    with repo.transaction():
        open_request(repo, request_id, kind, credential, params=params, digest=digest)
    staged = {}
    try:
        with repo.transaction():
            current = RequestStore(repo).lookup(request_id)
            if current.state == RequestState.SUCCESS:
                claim_receipt(repo, request_id, credential, digest=current.input.digest)
                return request_outcome(repo, current)
            if current.state == RequestState.REJECTED:
                return request_outcome(repo, current)
            result = execute(current.input.params, staged)
            return complete_request(repo, current, result)
    except ClaimError:
        raise
    except (OSError, sqlite3.Error, ValueError) as exc:
        retryable = isinstance(exc, (OSError, sqlite3.Error, MissingDictionary, VersionMissing))
        if isinstance(exc, SnapshotReadError):
            retryable = exc.retryable
        with repo.transaction():
            if staged:
                discard(staged)
            outcome = record_request_failure(repo, request_id, str(exc), retryable=retryable)
            if outcome.state == RequestState.SUCCESS:
                claim_receipt(repo, request_id, credential, digest=outcome.digest)
            return outcome


def resume_publish(repo, store, request_id, credential, *, params=None, digest=None):
    require_text(request_id, 'request id')
    owner = 'r-' + sha256(request_id.encode('utf-8')).hexdigest()

    def execute(saved, staged):
        store.discard_owner_stages(repo, owner)
        plan = build_publish_plan(repo, saved)
        staged.update(stage_publish(repo, store, plan, owner))
        return commit_publish(repo, store, plan, staged).snapshot_id

    return _resume(
        repo, request_id, 'publish', credential, params, digest, execute,
        lambda staged: discard_staged_publish(repo, store, staged),
    )


def resume_reconcile(repo, store, request_id, credential, *, params=None, digest=None, available=None, supplied=None):
    def execute(saved, _staged):
        if set(saved) != {'snapshot'}:
            raise ValueError('reconciliation requires exactly one snapshot id')
        snapshot_id = require_text(saved['snapshot'], 'snapshot')
        report = prepare_reconciliation(repo, store, snapshot_id, request_id, available=available, supplied=supplied)
        return save_report(repo, report)

    return _resume(repo, request_id, 'reconcile', credential, params, digest, execute, None)


def claim_historical_snapshot(repo, snapshot_id, request_id, credential, *, digest=None):
    with repo.transaction():
        receipt = claim_receipt(repo, request_id, credential, digest=digest, expected=('publish', snapshot_id))
        try:
            snapshot = load_snapshot(repo, snapshot_id)
        except (SnapshotNotFound, SnapshotConflict) as exc:
            raise ClaimError(str(exc)) from exc
        return receipt, snapshot
