import json
from dataclasses import dataclass
from hashlib import sha256

from src.ingest.validation import require_text


class DictionaryError(ValueError):
    pass


class MissingDictionary(DictionaryError):
    pass


@dataclass(frozen=True)
class DictionaryRef:
    partition: str
    generation: int
    digest: str


@dataclass(frozen=True)
class DictionaryRevision:
    ref: DictionaryRef
    entries: tuple[tuple[int, str], ...]


def _body(entries):
    return json.dumps(entries, ensure_ascii=False, separators=(',', ':'))


def build_revision(partition, generation, entries):
    require_text(partition, 'dictionary partition')
    if type(generation) is not int or not 1 <= generation <= 2**63 - 1:
        raise DictionaryError('dictionary generation must be a positive SQLite integer')
    if not isinstance(entries, (list, tuple)):
        raise DictionaryError('dictionary requires a sequence of code and name pairs')
    mapping = {}
    for pair in entries:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise DictionaryError('invalid dictionary entry')
        code, name = pair
        if type(code) is not int or code in mapping:
            raise DictionaryError(f'invalid or duplicate local code {code!r}')
        mapping[code] = require_text(name, 'status name')
    canonical = tuple(sorted(mapping.items()))
    digest = sha256(_body(canonical).encode('utf-8')).hexdigest()
    return DictionaryRevision(DictionaryRef(partition, generation, digest), canonical)


def load_revision(repo, partition, generation, *, expected_digest=None):
    row = repo.connection.execute(
        'SELECT body, digest FROM dictionaries WHERE partition = ? AND generation = ?',
        (partition, generation),
    ).fetchone()
    if row is None:
        raise MissingDictionary(f'missing dictionary {partition}/{generation}')
    try:
        revision = build_revision(partition, generation, json.loads(row['body']))
    except ValueError as exc:
        raise DictionaryError(f'invalid stored dictionary {partition}/{generation}: {exc}') from exc
    if revision.ref.digest != row['digest'] or expected_digest not in (None, revision.ref.digest):
        raise DictionaryError(f'dictionary digest mismatch for {partition}/{generation}')
    return revision


def load_current_revision(repo, partition):
    row = repo.connection.execute(
        'SELECT generation FROM dictionary_heads WHERE partition = ?', (partition,),
    ).fetchone()
    if row is None:
        raise MissingDictionary(f'no current dictionary for {partition}')
    return load_revision(repo, partition, row['generation'])


def register_revision(repo, revision):
    if not repo.connection.in_transaction:
        raise RuntimeError('dictionary registration requires the publish transaction')
    ref = revision.ref
    if build_revision(ref.partition, ref.generation, revision.entries) != revision:
        raise DictionaryError('invalid dictionary declaration')
    try:
        existing = load_revision(repo, ref.partition, ref.generation)
    except MissingDictionary:
        repo.connection.execute(
            'INSERT INTO dictionaries(partition, generation, body, digest) VALUES (?, ?, ?, ?)',
            (ref.partition, ref.generation, _body(revision.entries), ref.digest),
        )
    else:
        if existing != revision:
            raise DictionaryError(f'conflicting dictionary {ref.partition}/{ref.generation}')
    repo.connection.execute('''
        INSERT INTO dictionary_heads VALUES (?, ?)
        ON CONFLICT(partition) DO UPDATE SET generation = excluded.generation
        WHERE excluded.generation > dictionary_heads.generation
    ''', (ref.partition, ref.generation))
    return ref


def encode_status(revision, name):
    for code, candidate in revision.entries:
        if candidate == name:
            return code
    ref = revision.ref
    raise DictionaryError(f'status {name!r} is absent from {ref.partition}/{ref.generation}')


def resolve_status(revision, code):
    for candidate, name in revision.entries:
        if candidate == code:
            return name
    ref = revision.ref
    raise DictionaryError(f'unknown code {code} in {ref.partition}/{ref.generation}')
