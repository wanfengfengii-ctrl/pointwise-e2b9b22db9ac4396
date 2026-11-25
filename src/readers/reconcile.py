import json
from dataclasses import asdict, dataclass
from decimal import Decimal

from src.readers.aggregate import StatusSummary, StatusTotal, summarize_statuses
from src.readers.dictionary_resolver import ResolvedEvent
from src.readers.snapshot_reader import read_snapshot


@dataclass(frozen=True)
class ReconciliationReport:
    report_id: str
    snapshot_id: str
    summary: StatusSummary


def prepare_reconciliation(repo, store, snapshot_id, report_id, *, available=None, supplied=None):
    rows = read_snapshot(repo, store, snapshot_id, available=available, supplied=supplied)
    return ReconciliationReport(report_id, snapshot_id, summarize_statuses(rows))


def save_report(repo, report):
    if not repo.connection.in_transaction:
        raise RuntimeError('report save requires the reconciliation transaction')
    body = json.dumps(asdict(report.summary), default=str, ensure_ascii=False, separators=(',', ':'))
    repo.connection.execute('INSERT INTO reports VALUES (?, ?, ?)', (report.report_id, report.snapshot_id, body))
    return report.report_id


def load_report(repo, report_id):
    row = repo.connection.execute('SELECT * FROM reports WHERE report_id = ?', (report_id,)).fetchone()
    if row is None:
        raise LookupError(f'no committed report {report_id}')
    document = json.loads(row['body'])
    totals = tuple(StatusTotal(item['status'], item['count'], Decimal(item['watt_hours'])) for item in document['totals'])
    events = []
    for item in document['events']:
        item['value'] = Decimal(item['value'])
        events.append(ResolvedEvent(**item))
    return ReconciliationReport(report_id, row['snapshot_id'], StatusSummary(totals, tuple(events)))
