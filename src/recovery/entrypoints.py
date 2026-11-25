import argparse
import json
import sqlite3
import sys
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from src.catalog.repository import open_repository
from src.publish.compaction import compact_members
from src.publish.storage import ArtifactStore
from src.readers.reconcile import load_report, prepare_reconciliation
from src.recovery.receipts import claim_receipt
from src.recovery.replay import claim_historical_snapshot, resume_publish, resume_reconcile
from src.recovery.requests import RequestState


_SAMPLE = Path(__file__).resolve().parents[2] / 'samples' / 'workday.json'


def _read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'), parse_float=Decimal)


def _success(outcome):
    if outcome.state != RequestState.SUCCESS:
        raise ValueError(f'{outcome.request_id}: {outcome.state}: {outcome.reason}')
    return outcome


def run_workday(repo, store):
    sample = _read_json(_SAMPLE)
    credential = sample['credential']
    first = _success(resume_publish(repo, store, 'pub-S0', credential, params=sample['S0']))
    initial = _success(resume_reconcile(repo, store, 'audit-S0', credential, params={'snapshot': first.result_ref}))
    before = load_report(repo, initial.result_ref)
    _success(resume_publish(repo, store, 'pub-A2', credential, params=sample['A2']))
    second = _success(resume_publish(repo, store, 'pub-S1', credential, params=sample['S1']))
    later = _success(resume_reconcile(repo, store, 'audit-S1', credential, params={'snapshot': second.result_ref}))
    compact_members(repo, store, first.result_ref, segment_bytes=64)
    receipt, entry = claim_historical_snapshot(repo, first.result_ref, first.request_id, credential, digest=first.digest)
    after = prepare_reconciliation(repo, store, entry.snapshot_id, initial.result_ref)
    if before != after:
        raise ValueError('historical report changed after layout switch')
    return {
        'original': asdict(before), 'mixed_generations': asdict(load_report(repo, later.result_ref)),
        'historical': asdict(after), 'original_receipt': asdict(receipt),
    }


def _parser():
    parser = argparse.ArgumentParser(description='青屿冷链分区用电湖')
    commands = parser.add_subparsers(dest='command', required=True)
    demo = commands.add_parser('demo')
    demo.add_argument('workspace', nargs='?')
    commands.add_parser('init').add_argument('workspace')
    for name in ('publish', 'reconcile', 'claim'):
        command = commands.add_parser(name)
        command.add_argument('workspace')
        command.add_argument('request_id')
        command.add_argument('credential')
        if name == 'claim':
            command.add_argument('--digest')
            command.add_argument('--snapshot')
        else:
            source = command.add_mutually_exclusive_group(required=True)
            source.add_argument('--digest')
            source.add_argument('--input' if name == 'publish' else '--snapshot')
    compact = commands.add_parser('compact')
    compact.add_argument('workspace')
    compact.add_argument('snapshot')
    compact.add_argument('--segment-bytes', type=int, default=4096)
    return parser


def _run(args):
    with open_repository(args.workspace) as repo:
        store = ArtifactStore(repo.root)
        if args.command == 'demo':
            return run_workday(repo, store)
        if args.command == 'init':
            return {'workspace': str(repo.root)}
        if args.command == 'compact':
            layouts = compact_members(repo, store, args.snapshot, segment_bytes=args.segment_bytes)
            return {'snapshot': args.snapshot, 'logical_files': len(layouts), 'extents': sum(len(layout.extents) for layout in layouts)}
        if args.command == 'claim':
            if args.snapshot is not None:
                receipt, entry = claim_historical_snapshot(repo, args.snapshot, args.request_id, args.credential, digest=args.digest)
                return {'receipt': asdict(receipt), 'snapshot': entry.snapshot_id, 'publish_sequence': entry.publish_sequence}
            return asdict(claim_receipt(repo, args.request_id, args.credential, digest=args.digest))
        if args.command == 'publish':
            params = _read_json(args.input) if args.input is not None else None
            outcome = resume_publish(repo, store, args.request_id, args.credential, params=params, digest=args.digest)
            return asdict(_success(outcome))
        params = {'snapshot': args.snapshot} if args.snapshot is not None else None
        outcome = _success(resume_reconcile(repo, store, args.request_id, args.credential, params=params, digest=args.digest))
        return {'outcome': asdict(outcome), 'report': asdict(load_report(repo, outcome.result_ref))}


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        if args.command == 'demo' and args.workspace is None:
            with TemporaryDirectory(prefix='qingyu-lake-') as workspace:
                args.workspace = workspace
                result = _run(args)
        else:
            result = _run(args)
    except (LookupError, OSError, ValueError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, default=str, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
