import argparse
import json
import multiprocessing as mp
import os
import signal
import sys
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from src.catalog.repository import open_repository
from src.publish.compaction import compact_members
from src.publish.storage import ArtifactStore
from src.readers.read_plan import plan_snapshot_read
from src.readers.snapshot_reader import read_snapshot
from src.recovery.replay import claim_historical_snapshot, resume_publish
from src.recovery.requests import RequestState


EXPECTED = [(10, "运行", "4.10", "A网关/10"), (11, "运行", "0.20", "A网关/11"),
            (20, "停机", "7.30", "B网关/20")]


def signature(rows):
    return [(row.sequence, row.status, str(row.value), row.source) for row in rows]


def move(repo, store, snapshot):
    layouts = compact_members(repo, store, snapshot, segment_bytes=32)
    name, = {extent.object_name for layout in layouts for extent in layout.extents}
    return name


def prepare(repo, store):
    path = Path(__file__).with_name("samples") / "workday.json"
    sample = json.loads(path.read_bytes(), parse_float=Decimal)
    credential = sample["credential"]
    first = resume_publish(repo, store, "retire-S0", credential, params=sample["S0"])
    assert first.state == RequestState.SUCCESS, first.reason
    for name, spec in zip(("A-only", "B-only"), sample["S0"]["members"]):
        member = {key: spec[key] for key in ("partition", "version")}
        result = resume_publish(repo, store, "retire-" + name, credential,
                                params={"snapshot": name, "members": [member]})
        assert result.state == RequestState.SUCCESS, result.reason
    return credential, first, move(repo, store, "S0")


def reference_count(repo, name):
    query = "SELECT count(DISTINCT logical_id) FROM file_extents WHERE object_name = ?"
    return repo.connection.execute(query, (name,)).fetchone()[0]


def baseline(root):
    with open_repository(root) as repo:
        store = ArtifactStore(root)
        _, _, shared = prepare(repo, store)
        counts = [reference_count(repo, shared)]
        move(repo, store, "A-only")
        counts.append(reference_count(repo, shared))
        layout = plan_snapshot_read(repo, store, "B-only").members[0].layout
        original = store.read_logical_file(layout)
        move(repo, store, "B-only")
        counts.append(reference_count(repo, shared))
        assert counts == [2, 1, 0], counts
        assert {extent.object_name for extent in layout.extents} == {shared}
        assert store.read_logical_file(layout) == original
        rows = signature(read_snapshot(repo, store, "S0"))
        assert rows == EXPECTED, rows
        return {"old_object": shared, "references": counts, "rows": rows}


def receive(conn):
    if not conn.poll(30):
        raise AssertionError("reader did not reach the next checkpoint")
    return conn.recv()


def worker_read(root, conn):
    from src.readers.snapshot_reader import open_snapshot_read

    with conn, open_repository(root) as repo:
        with open_snapshot_read(repo, ArtifactStore(root), "B-only") as reader:
            conn.send("entered")
            assert conn.recv() == "read"
            first = signature(reader.read())
            second = signature(reader.read())
        conn.send((first, second))


def reader_case(root, *, kill):
    from src.publish.storage import collect_retired

    ctx = mp.get_context("spawn")
    with open_repository(root) as repo:
        store = ArtifactStore(root)
        credential, first, shared = prepare(repo, store)
        unrelated = move(repo, store, "A-only")
        staged = store.stage_file("fixture", b"unpublished")
        assert shared not in collect_retired(repo, store)
        assert (store.path / shared).is_file()
        assert signature(read_snapshot(repo, store, "B-only")) == EXPECTED[2:]

        parent, child = ctx.Pipe()
        with parent, child:
            process = ctx.Process(target=worker_read, args=(root, child))
            process.start()
            child.close()
            try:
                assert receive(parent) == "entered"
                move(repo, store, "A-only")
                new_name = move(repo, store, "B-only")
                reclaimed = collect_retired(repo, store)
                assert unrelated in reclaimed and not (store.path / unrelated).exists()
                new_rows = signature(read_snapshot(repo, store, "S0"))
                assert new_rows == EXPECTED, new_rows
                if kill:
                    process.kill()
                else:
                    replacement = store.path / new_name
                    held = store.path / ".fixture-new-copy"
                    replacement.rename(held)
                    try:
                        parent.send("read")
                        assert receive(parent) == (EXPECTED[2:], EXPECTED[2:])
                    finally:
                        held.rename(replacement)
                process.join(30)
                assert process.exitcode == (-signal.SIGKILL if kill else 0), process.exitcode
            finally:
                if process.is_alive():
                    process.kill()
                    process.join()
                process.close()

    with open_repository(root) as repo:
        store = ArtifactStore(root)
        final = collect_retired(repo, store)
        assert shared in reclaimed or shared in final
        assert not (store.path / shared).exists()
        assert (store.path / staged).read_bytes() == b"unpublished"
        receipt, entry = claim_historical_snapshot(repo, "S0", first.request_id, credential)
        assert (receipt.receipt_id, entry.snapshot_id) == (first.receipt_id, "S0")
        assert signature(read_snapshot(repo, store, "S0")) == EXPECTED
        return {"retired": shared, "reclaimed_during_read": reclaimed,
                "reclaimed_after_exit": final, "reader_killed": kill,
                "rows": new_rows, "receipt": receipt.receipt_id}


def worker_collect(root, watched):
    from src.publish.storage import collect_retired

    with open_repository(root) as repo:
        store = ArtifactStore(root)
        path = store.path / watched

        def stop_on_removal(frame, event, arg):
            if event in ("line", "return") and not path.exists():
                os.kill(os.getpid(), signal.SIGKILL)
            return stop_on_removal

        sys.settrace(stop_on_removal)
        try:
            collect_retired(repo, store)
        finally:
            sys.settrace(None)
    raise AssertionError("collector returned without an observable removal")


def interrupted_collection(root):
    from src.publish.storage import collect_retired

    with open_repository(root) as repo:
        store = ArtifactStore(root)
        _, _, shared = prepare(repo, store)
        move(repo, store, "A-only")
        move(repo, store, "B-only")
        assert (store.path / shared).is_file()
    ctx = mp.get_context("spawn")
    process = ctx.Process(target=worker_collect, args=(root, shared))
    process.start()
    try:
        process.join(30)
        assert process.exitcode == -signal.SIGKILL, process.exitcode
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        process.close()
    with open_repository(root) as repo:
        store = ArtifactStore(root)
        recovered = collect_retired(repo, store)
        assert not (store.path / shared).exists()
        assert collect_retired(repo, store) == []
        assert signature(read_snapshot(repo, store, "S0")) == EXPECTED
        return {"interrupted_object": shared, "recovered": recovered}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("baseline", "verify", "demo"))
    mode = parser.parse_args().mode
    with TemporaryDirectory(prefix="qingyu-retire-") as workspace:
        root = Path(workspace)
        if mode == "baseline":
            result = baseline(root)
        else:
            result = {"normal_exit": reader_case(root / "normal", kill=False)}
            if mode == "verify":
                result["killed_reader"] = reader_case(root / "killed", kill=True)
                result["interrupted_collector"] = interrupted_collection(root / "restart")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
