import sqlite3
from contextlib import contextmanager
from pathlib import Path


_SCHEMA = '''
CREATE TABLE IF NOT EXISTS dictionaries (
    partition TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    body TEXT NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (partition, generation)
);
CREATE TABLE IF NOT EXISTS dictionary_heads (
    partition TEXT PRIMARY KEY NOT NULL,
    generation INTEGER NOT NULL,
    FOREIGN KEY (partition, generation) REFERENCES dictionaries(partition, generation)
);
CREATE TABLE IF NOT EXISTS versions (
    logical_id INTEGER PRIMARY KEY,
    partition TEXT NOT NULL,
    version TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    dictionary_digest TEXT NOT NULL,
    record_count INTEGER NOT NULL CHECK(record_count >= 0),
    digest TEXT NOT NULL,
    UNIQUE (partition, version),
    FOREIGN KEY (partition, generation) REFERENCES dictionaries(partition, generation)
);
CREATE TABLE IF NOT EXISTS artifact_objects (
    object_name TEXT PRIMARY KEY NOT NULL
);
CREATE TABLE IF NOT EXISTS file_extents (
    logical_id INTEGER NOT NULL REFERENCES versions(logical_id),
    logical_offset INTEGER NOT NULL CHECK(logical_offset >= 0),
    object_name TEXT NOT NULL REFERENCES artifact_objects(object_name),
    physical_offset INTEGER NOT NULL CHECK(physical_offset >= 0),
    byte_length INTEGER NOT NULL CHECK(byte_length > 0),
    PRIMARY KEY (logical_id, logical_offset)
);
CREATE TABLE IF NOT EXISTS snapshots (
    publish_sequence INTEGER PRIMARY KEY,
    snapshot_id TEXT NOT NULL UNIQUE,
    manifest TEXT NOT NULL,
    manifest_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY NOT NULL,
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('publish', 'reconcile')),
    params TEXT NOT NULL,
    digest TEXT NOT NULL,
    credential_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending', 'retry', 'rejected', 'success')),
    result_ref TEXT,
    reason TEXT,
    CHECK((state = 'success') = (result_ref IS NOT NULL)),
    CHECK((state IN ('retry', 'rejected')) = (reason IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS receipts (
    request_id TEXT PRIMARY KEY NOT NULL REFERENCES requests(request_id),
    receipt_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    digest TEXT NOT NULL,
    result_ref TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS claims (
    receipt_id TEXT PRIMARY KEY NOT NULL REFERENCES receipts(receipt_id),
    credential_digest TEXT NOT NULL
);
'''


class CatalogRepository:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.root / 'catalog.sqlite3', isolation_level=None, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys = ON')

    def initialize(self):
        if self.connection.in_transaction:
            raise RuntimeError('initialize requires an idle connection')
        try:
            self.connection.executescript('BEGIN IMMEDIATE;\n' + _SCHEMA + '\nCOMMIT;')
        except BaseException:
            self.connection.rollback()
            raise

    @contextmanager
    def transaction(self, *, write=True):
        if self.connection.in_transaction:
            raise RuntimeError('transaction already active')
        self.connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        try:
            yield self.connection
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def open_repository(workspace):
    repository = CatalogRepository(workspace)
    try:
        repository.initialize()
    except BaseException:
        repository.close()
        raise
    return repository
