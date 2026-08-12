"""Tests for the app-owned index definitions and applier."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from pymongo.errors import OperationFailure

from cfdb.indexes import (
    IndexSpec,
    all_index_specs,
    data_index_specs,
    ensure_indexes,
    materialized_files_index_specs,
    operational_index_specs,
)

#: The hand-maintained JS bootstrap kept for the local mongo image. The
#: lockstep tests pin its specs to the Python source of truth.
_CREATE_INDEXES_JS = (
    Path(__file__).resolve().parents[1] / "scripts" / "create-indexes.js"
)
_JS = _CREATE_INDEXES_JS.read_text()


def _spec_by_name(specs: list[IndexSpec], name: str) -> IndexSpec:
    """Return the single spec named ``name`` (asserts uniqueness)."""
    matches = [s for s in specs if s.name == name]
    assert len(matches) == 1, f"expected exactly one spec named {name}"
    return matches[0]


def _js_active_predicate(name: str) -> bool | None:
    """Read the ``active`` partial-filter bool for a named JS jobs index.

    Returns True/False for ``partialFilterExpression: { active: <bool> }``
    or None when the named index or predicate isn't found.
    """
    m = re.search(
        r'name:\s*"'
        + re.escape(name)
        + r'".*?partialFilterExpression:\s*\{\s*active:\s*(true|false)\s*\}',
        _JS,
        re.DOTALL,
    )
    return None if m is None else (m.group(1) == "true")


def _parse_js_keys(src: str) -> tuple[tuple[str, int], ...]:
    """Parse a JS key object body like ``id_namespace: 1, local_id: 1``."""
    keys: list[tuple[str, int]] = []
    for part in src.split(","):
        part = part.strip()
        if not part:
            continue
        field, _, direction = part.partition(":")
        keys.append((field.strip(), int(direction.strip())))
    return tuple(keys)


def _parse_js_data_indexes() -> set[tuple[str, tuple[tuple[str, int], ...], bool]]:
    """Parse ``db.<coll>.createIndex(...)`` calls into a comparable set.

    Excludes the operational collections (``jobs`` uses the ``ensureIndex``
    helper, not ``createIndex``; ``locks`` is operational) so the result
    mirrors ``data_index_specs()``.
    """
    pattern = re.compile(
        r"db\.(\w+)\.createIndex\(\s*\{([^}]*)\}\s*(?:,\s*\{([^}]*)\})?\s*\)"
    )
    result: set[tuple[str, tuple[tuple[str, int], ...], bool]] = set()
    for match in pattern.finditer(_JS):
        collection, keys_src, opts_src = match.group(1), match.group(2), match.group(3)
        if collection in ("jobs", "locks"):
            continue
        unique = "unique:true" in (opts_src or "").replace(" ", "")
        result.add((collection, _parse_js_keys(keys_src), unique))
    return result


class TestIndexSpec:
    def test___init___should_derive_default_name_when_name_omitted(self):
        """Test that an unnamed spec gets MongoDB's default index name.

        Given:
            An IndexSpec built with a compound key and no explicit name.
        When:
            The spec is constructed.
        Then:
            It should derive the ``field_dir_field_dir`` name MongoDB
            would assign to an otherwise-unnamed index.
        """
        # Act
        spec = IndexSpec("file", [("id_namespace", 1), ("local_id", 1)])

        # Assert
        assert spec.name == "id_namespace_1_local_id_1"

    def test___init___should_coerce_keys_to_tuple(self):
        """Test that a list of keys is normalized to a tuple.

        Given:
            An IndexSpec built with a list of key pairs.
        When:
            The spec is constructed.
        Then:
            It should store ``keys`` as a tuple of tuples so the frozen
            spec holds only immutable data.
        """
        # Act
        spec = IndexSpec("file", [("filename", 1)])

        # Assert
        assert spec.keys == (("filename", 1),)

    def test___init___should_keep_explicit_name(self):
        """Test that an explicit name is preserved.

        Given:
            An IndexSpec built with an explicit ``name``.
        When:
            The spec is constructed.
        Then:
            It should keep the provided name rather than deriving one.
        """
        # Act
        spec = IndexSpec("jobs", [("workflow_key", 1)], name="workflow_key_active_unique")

        # Assert
        assert spec.name == "workflow_key_active_unique"

    def test_create_kwargs_should_include_only_set_options(self):
        """Test that create_kwargs omits unset options.

        Given:
            A plain single-field spec with no unique/partial/TTL options.
        When:
            ``create_kwargs`` is called.
        Then:
            It should contain only the index name.
        """
        # Arrange
        spec = IndexSpec("file", [("filename", 1)])

        # Act
        kwargs = spec.create_kwargs()

        # Assert
        assert kwargs == {"name": "filename_1"}

    def test_create_kwargs_should_render_partial_unique_ttl_options(self):
        """Test that create_kwargs renders unique, partial, and TTL options.

        Given:
            A spec with unique, partialFilterExpression, and TTL set.
        When:
            ``create_kwargs`` is called.
        Then:
            It should map each onto the pymongo keyword arguments.
        """
        # Arrange
        spec = IndexSpec(
            "jobs",
            [("updated_at", 1)],
            name="terminal_ttl",
            unique=True,
            partial_filter={"active": False},
            expire_after_seconds=10,
        )

        # Act
        kwargs = spec.create_kwargs()

        # Assert
        assert kwargs == {
            "name": "terminal_ttl",
            "unique": True,
            "partialFilterExpression": {"active": False},
            "expireAfterSeconds": 10,
        }


def test_operational_index_specs_should_filter_mutex_on_active_true():
    """Test the workflow_key mutex index filters on active=True.

    Given:
        The operational index specs.
    When:
        The ``workflow_key_active_unique`` spec is inspected.
    Then:
        It should be a unique partial index whose filter is the
        DocumentDB-safe implicit-equality predicate ``{"active": True}``
        (no ``$in``).
    """
    # Arrange
    specs = operational_index_specs()

    # Act
    spec = _spec_by_name(specs, "workflow_key_active_unique")

    # Assert
    assert spec.collection == "jobs"
    assert spec.keys == (("workflow_key", 1),)
    assert spec.unique is True
    assert spec.partial_filter == {"active": True}


def test_operational_index_specs_should_filter_ttl_on_active_false():
    """Test the terminal_ttl index filters on active=False.

    Given:
        The operational index specs.
    When:
        The ``terminal_ttl`` spec is inspected.
    Then:
        It should be a TTL index whose filter is ``{"active": False}``.
    """
    # Arrange
    specs = operational_index_specs()

    # Act
    spec = _spec_by_name(specs, "terminal_ttl")

    # Assert
    assert spec.expire_after_seconds == 60 * 60 * 24 * 7
    assert spec.partial_filter == {"active": False}


def test_operational_index_specs_should_not_use_in_operator():
    """Test no operational partial filter uses the $in operator.

    Given:
        The operational index specs (DocumentDB rejects $in in a
        partialFilterExpression).
    When:
        Their partial filters are inspected.
    Then:
        None should contain a ``$in`` clause.
    """
    # Act
    filters = [s.partial_filter for s in operational_index_specs() if s.partial_filter]

    # Assert
    assert all("$in" not in repr(f) for f in filters)


def test_operational_index_specs_should_include_locks_active():
    """Test the operational set includes the locks.active index.

    Given:
        The operational index specs.
    When:
        The specs are filtered to the ``locks`` collection.
    Then:
        It should include the ``active_1`` index.
    """
    # Act
    specs = operational_index_specs()

    # Assert
    locks = [s for s in specs if s.collection == "locks"]
    assert [s.name for s in locks] == ["active_1"]


def test_data_index_specs_should_cover_core_collections():
    """Test the data specs cover the materialized C2M2 collections.

    Given:
        The data index specs.
    When:
        The set of collections they target is collected.
    Then:
        It should include the central queryable collections.
    """
    # Act
    collections = {s.collection for s in data_index_specs()}

    # Assert
    assert {"file", "dcc", "biosample", "collection", "project"} <= collections


def test_data_index_specs_should_mark_ontology_submission_id_unique():
    """Test ontology collections get a unique (submission, id) index.

    Given:
        The data index specs.
    When:
        The ``file_format`` composite (submission, id) spec is found.
    Then:
        It should be marked unique (one term per DCC submission).
    """
    # Arrange
    specs = data_index_specs()

    # Act
    spec = _spec_by_name(
        [s for s in specs if s.collection == "file_format"],
        "submission_1_id_1",
    )

    # Assert
    assert spec.unique is True


def test_all_index_specs_should_concatenate_operational_then_data():
    """Test all_index_specs is operational followed by data.

    Given:
        The operational, data, and combined spec lists.
    When:
        Their ordering is compared.
    Then:
        all_index_specs should equal operational + data.
    """
    # Act
    combined = all_index_specs()

    # Assert
    assert combined == operational_index_specs() + data_index_specs()


def test_create_indexes_js_should_not_use_in_in_partial_filters():
    """Test the JS bootstrap has no $in (DocumentDB-incompatible).

    Given:
        The committed scripts/create-indexes.js.
    When:
        The source is scanned.
    Then:
        Its executable code (comments stripped) should contain no ``$in``
        operator, since DocumentDB rejects ``$in`` inside a
        partialFilterExpression.
    """
    # Arrange
    code = re.sub(r"//.*", "", _JS)

    # Assert
    assert "$in" not in code


def test_create_indexes_js_operational_predicates_should_match_python():
    """Test the JS jobs partial predicates match the Python specs.

    Given:
        The committed JS and the operational specs.
    When:
        The JS ``active`` predicates for the named jobs indexes are read.
    Then:
        They should equal the Python specs' ``active`` filter values, so
        the local-mongo bootstrap can't drift from the app.
    """
    # Arrange
    specs = {s.name: s for s in operational_index_specs()}

    # Assert
    assert (
        _js_active_predicate("workflow_key_active_unique")
        is specs["workflow_key_active_unique"].partial_filter["active"]
    )
    assert (
        _js_active_predicate("terminal_ttl")
        is specs["terminal_ttl"].partial_filter["active"]
    )


def test_create_indexes_js_should_define_all_operational_index_names():
    """Test the JS defines every named operational index.

    Given:
        The committed JS and the operational specs that carry names.
    When:
        The JS source is read.
    Then:
        It should reference each named operational index and the
        locks.active index. The names are derived from
        ``operational_index_specs()`` (not hard-coded) so a newly-added
        operational index is guarded automatically.
    """
    # Arrange — the named jobs-collection operational indexes (the locks
    # index relies on Mongo's auto-generated name and is asserted below).
    named = [
        spec.name
        for spec in operational_index_specs()
        if spec.name and spec.collection == "jobs"
    ]

    # Assert
    assert named, "expected at least one named jobs operational index"
    for name in named:
        assert name in _JS, f"{name} missing from create-indexes.js"
    assert "db.locks.createIndex({ active: 1 })" in _JS


def test_create_indexes_js_data_indexes_should_match_python():
    """Test the JS data indexes stay in lockstep with data_index_specs.

    Given:
        The data createIndex calls parsed from the committed JS and the
        Python data specs.
    When:
        Both are reduced to (collection, keys, unique) sets.
    Then:
        The two sets should be equal, guarding drift in the ~150 data
        indexes the local-mongo image bootstraps.
    """
    # Arrange
    py_set = {(s.collection, s.keys, s.unique) for s in data_index_specs()}

    # Act
    js_set = _parse_js_data_indexes()

    # Assert
    assert js_set == py_set


def _conflict_collection(mocker, *, existing_info: dict, create_side_effect):
    """Build a Mongo-shaped collection mock for conflict-recovery tests."""
    collection = mocker.MagicMock()
    collection.create_index = mocker.AsyncMock(side_effect=create_side_effect)
    collection.index_information = mocker.AsyncMock(return_value=existing_info)
    collection.drop_index = mocker.AsyncMock()
    return collection


def _db_for(mocker, collection):
    """Wrap a collection mock in a Mongo-shaped db (``db[name]``)."""
    db = mocker.MagicMock()
    db.__getitem__.return_value = collection
    return db


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ensure_indexes_should_create_specs():
    """Test ensure_indexes creates the requested indexes.

    Given:
        A fresh in-memory Mongo (mongomock-motor) and the operational
        specs.
    When:
        ensure_indexes is awaited.
    Then:
        It should create each index and report the count ensured.
    """
    # Arrange
    from mongomock_motor import AsyncMongoMockClient

    db = AsyncMongoMockClient()["test"]
    specs = operational_index_specs()

    # Act
    count = await ensure_indexes(db, specs)

    # Assert
    assert count == len(specs)
    info = await db.jobs.index_information()
    assert "workflow_key_active_unique" in info


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ensure_indexes_should_be_idempotent_on_repeat():
    """Test a second ensure_indexes call is a no-op.

    Given:
        A database that already has the operational indexes ensured.
    When:
        ensure_indexes is awaited a second time with the same specs.
    Then:
        It should succeed without error and report the same count.
    """
    # Arrange
    from mongomock_motor import AsyncMongoMockClient

    db = AsyncMongoMockClient()["test"]
    specs = operational_index_specs()
    await ensure_indexes(db, specs)

    # Act
    count = await ensure_indexes(db, specs)

    # Assert
    assert count == len(specs)


@pytest.mark.asyncio
async def test_ensure_indexes_should_drop_and_recreate_on_options_conflict(mocker):
    """Test a changed index spec is dropped and recreated.

    Given:
        A collection whose create_index first raises an
        IndexOptionsConflict (code 85) then succeeds, with the existing
        index registered under the spec's own name.
    When:
        ensure_indexes is awaited with that spec.
    Then:
        It should drop the named index and recreate it.
    """
    # Arrange
    collection = _conflict_collection(
        mocker,
        existing_info={"job_id_unique": {"key": [("job_id", 1)], "unique": True}},
        create_side_effect=[OperationFailure("conflict", 85), None],
    )
    db = _db_for(mocker, collection)
    spec = IndexSpec("jobs", [("job_id", 1)], name="job_id_unique", unique=True)

    # Act
    count = await ensure_indexes(db, [spec])

    # Assert
    assert count == 1
    collection.drop_index.assert_awaited_once_with("job_id_unique")
    assert collection.create_index.await_count == 2


@pytest.mark.asyncio
async def test_ensure_indexes_should_drop_conflicting_index_under_different_name(mocker):
    """Test recovery drops a same-key index registered under another name.

    Given:
        A conflict (code 85) where the existing index on the same key is
        registered under a legacy name, not the spec's name.
    When:
        ensure_indexes is awaited.
    Then:
        It should drop the existing index by its actual name and recreate.
    """
    # Arrange
    collection = _conflict_collection(
        mocker,
        existing_info={"legacy_wf": {"key": [("workflow_key", 1)], "unique": True}},
        create_side_effect=[OperationFailure("conflict", 85), None],
    )
    db = _db_for(mocker, collection)
    spec = IndexSpec(
        "jobs", [("workflow_key", 1)], name="workflow_key_active_unique", unique=True
    )

    # Act
    await ensure_indexes(db, [spec])

    # Assert
    collection.drop_index.assert_awaited_once_with("legacy_wf")


@pytest.mark.asyncio
async def test_ensure_indexes_should_ignore_index_not_found_on_drop(mocker):
    """Test an IndexNotFound during the drop is swallowed.

    Given:
        A conflict whose drop_index raises IndexNotFound (code 27)
        because the conflicting index vanished underneath us.
    When:
        ensure_indexes is awaited.
    Then:
        It should not raise and should still recreate the index.
    """
    # Arrange
    collection = _conflict_collection(
        mocker,
        existing_info={},
        create_side_effect=[OperationFailure("conflict", 85), None],
    )
    collection.drop_index = mocker.AsyncMock(
        side_effect=OperationFailure("not found", 27)
    )
    db = _db_for(mocker, collection)
    spec = IndexSpec("jobs", [("job_id", 1)], name="job_id_unique", unique=True)

    # Act
    count = await ensure_indexes(db, [spec])

    # Assert
    assert count == 1
    assert collection.create_index.await_count == 2


@pytest.mark.asyncio
async def test_ensure_indexes_should_reraise_unexpected_failure(mocker):
    """Test an unrelated OperationFailure is not swallowed.

    Given:
        A collection whose create_index raises a non-conflict
        OperationFailure (e.g. an auth error, code 13).
    When:
        ensure_indexes is awaited.
    Then:
        It should propagate the error rather than drop the index.
    """
    # Arrange
    collection = _conflict_collection(
        mocker,
        existing_info={},
        create_side_effect=OperationFailure("unauthorized", 13),
    )
    db = _db_for(mocker, collection)
    spec = IndexSpec("jobs", [("job_id", 1)], name="job_id_unique")

    # Act & assert
    with pytest.raises(OperationFailure):
        await ensure_indexes(db, [spec])
    collection.drop_index.assert_not_awaited()


def test_data_index_specs_should_not_target_the_materialized_files_collection():
    """Test the ownership split between the two index sources.

    This module owns the raw C2M2 collections; the Rust materializer owns
    the denormalized ``files`` collection it builds, and indexes it in
    ``index_keys``. The module docstring states that split in prose only,
    so this pins it: adding a ``files`` spec here would create a second
    writer for those indexes, silently competing with the materializer.

    Given:
        The data index specs.
    When:
        The set of collections they target is collected.
    Then:
        It should include the raw ``file`` collection and not ``files``.
    """
    # Act
    targets = {spec.collection for spec in data_index_specs()}

    # Assert
    assert "file" in targets
    assert "files" not in targets


def test_materialized_files_index_specs_should_not_overlap_the_data_specs():
    """Test that the narrow files exception does not become a second writer.

    ``files`` has exactly one owner for its full index set -- the
    materializer. The accession specs exist only because ``_sync_encode``
    writes ``files`` directly and never runs it, so an ENCODE-only database
    would otherwise have no index at all. Keeping the two sources disjoint
    is what stops that exception from growing into a duplicate of
    ``index_keys``.

    Given:
        Both Python-side index sources.
    When:
        Their (collection, name) pairs are compared.
    Then:
        They should share none, and the files specs should target only
        ``files``.
    """
    # Act
    data = {(s.collection, s.name) for s in data_index_specs()}
    files = {(s.collection, s.name) for s in materialized_files_index_specs()}

    # Assert
    assert not data & files
    assert {s.collection for s in materialized_files_index_specs()} == {"files"}


def test_materialized_files_index_specs_should_cover_both_accession_paths():
    """Test that both queryable accession paths are indexed.

    These are exactly the two predicates ``to_query`` can emit for an
    accession filter; an accession lookup that missed either would scan the
    whole collection on a public endpoint.

    Given:
        The materialized files index specs.
    When:
        Their key tuples are collected.
    Then:
        They should cover the file-level and nested collection accessions.
    """
    # Act
    keys = {spec.keys for spec in materialized_files_index_specs()}

    # Assert
    assert keys == {(("accession_id", 1),), (("collections.accession_id", 1),)}


def test_all_index_specs_should_not_repeat_a_collection_and_name():
    """Test that no index is declared twice.

    Given:
        The full operational-plus-data spec list.
    When:
        Its (collection, name) pairs are collected.
    Then:
        None should repeat, so appending a field to two loops -- or to the
        same loop twice -- fails here rather than issuing a redundant
        createIndex against a live database.
    """
    # Act
    pairs = [(spec.collection, spec.name) for spec in all_index_specs()]

    # Assert
    assert len(pairs) == len(set(pairs))
