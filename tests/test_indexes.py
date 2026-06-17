"""Tests for the app-owned index definitions and applier."""

from __future__ import annotations

from pathlib import Path

import pytest
from pymongo.errors import OperationFailure

from cfdb.indexes import (
    IndexSpec,
    all_index_specs,
    data_index_specs,
    ensure_indexes,
    operational_index_specs,
)
from cfdb.workflows.models import ACTIVE_STATUSES, JobStatus

#: The hand-maintained JS bootstrap kept for the local mongo image. The
#: lockstep tests pin its operational specs to the Python source of truth.
_CREATE_INDEXES_JS = (
    Path(__file__).resolve().parents[1] / "scripts" / "create-indexes.js"
)


def _spec_by_name(specs: list[IndexSpec], name: str) -> IndexSpec:
    """Return the single spec named ``name`` (KeyError-style assert if absent)."""
    matches = [s for s in specs if s.name == name]
    assert len(matches) == 1, f"expected exactly one spec named {name}"
    return matches[0]


def _js_array(values: list[str]) -> str:
    """Render values the way create-indexes.js renders a ``$in`` array."""
    return "[" + ", ".join(f'"{v}"' for v in values) + "]"


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
            partial_filter={"status": {"$in": ["completed"]}},
            expire_after_seconds=10,
        )

        # Act
        kwargs = spec.create_kwargs()

        # Assert
        assert kwargs == {
            "name": "terminal_ttl",
            "unique": True,
            "partialFilterExpression": {"status": {"$in": ["completed"]}},
            "expireAfterSeconds": 10,
        }


class TestOperationalIndexSpecs:
    def test_operational_index_specs_should_derive_mutex_filter_from_active_statuses(self):
        """Test the workflow_key mutex index tracks ACTIVE_STATUSES.

        Given:
            The operational index specs.
        When:
            The ``workflow_key_active_unique`` spec is inspected.
        Then:
            It should be a unique partial index whose ``$in`` filter is
            exactly the active status values, in lockstep with the enum.
        """
        # Arrange
        specs = operational_index_specs()
        expected_active = [s.value for s in ACTIVE_STATUSES]

        # Act
        spec = _spec_by_name(specs, "workflow_key_active_unique")

        # Assert
        assert spec.collection == "jobs"
        assert spec.keys == [("workflow_key", 1)]
        assert spec.unique is True
        assert spec.partial_filter == {"status": {"$in": expected_active}}

    def test_operational_index_specs_should_derive_ttl_filter_from_terminal_statuses(self):
        """Test the terminal_ttl index covers the complement of active.

        Given:
            The operational index specs.
        When:
            The ``terminal_ttl`` spec is inspected.
        Then:
            It should be a TTL index whose ``$in`` filter is exactly the
            non-active (terminal) status values.
        """
        # Arrange
        specs = operational_index_specs()
        expected_terminal = [s.value for s in JobStatus if s not in ACTIVE_STATUSES]

        # Act
        spec = _spec_by_name(specs, "terminal_ttl")

        # Assert
        assert spec.expire_after_seconds == 60 * 60 * 24 * 7
        assert spec.partial_filter == {"status": {"$in": expected_terminal}}

    def test_operational_index_specs_should_include_locks_active(self):
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


class TestDataIndexSpecs:
    def test_data_index_specs_should_cover_core_collections(self):
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

    def test_data_index_specs_should_mark_ontology_submission_id_unique(self):
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

    def test_all_index_specs_should_concatenate_operational_then_data(self):
        """Test all_index_specs is operational followed by data.

        Given:
            The operational, data, and combined spec lists.
        When:
            Their lengths and ordering are compared.
        Then:
            all_index_specs should equal operational + data.
        """
        # Act
        combined = all_index_specs()

        # Assert
        assert combined == operational_index_specs() + data_index_specs()


class TestCreateIndexesJsLockstep:
    def test_create_indexes_js_should_match_active_status_filter(self):
        """Test the JS mutex predicate matches ACTIVE_STATUSES.

        Given:
            The committed scripts/create-indexes.js and the Python active
            status values.
        When:
            The JS source is read.
        Then:
            It should contain the active-status ``$in`` array the Python
            workflow_key spec produces, so the two can't drift.
        """
        # Arrange
        js = _CREATE_INDEXES_JS.read_text()
        active = [s.value for s in ACTIVE_STATUSES]

        # Assert
        assert _js_array(active) in js

    def test_create_indexes_js_should_match_terminal_status_filter(self):
        """Test the JS TTL predicate matches the terminal status set.

        Given:
            The committed scripts/create-indexes.js and the Python
            terminal status values.
        When:
            The JS source is read.
        Then:
            It should contain the terminal-status ``$in`` array the
            Python terminal_ttl spec produces.
        """
        # Arrange
        js = _CREATE_INDEXES_JS.read_text()
        terminal = [s.value for s in JobStatus if s not in ACTIVE_STATUSES]

        # Assert
        assert _js_array(terminal) in js

    def test_create_indexes_js_should_define_all_operational_index_names(self):
        """Test the JS defines every named operational index.

        Given:
            The committed scripts/create-indexes.js and the operational
            specs that carry explicit names.
        When:
            The JS source is read.
        Then:
            It should reference each named operational index, so the
            local-mongo bootstrap stays in lockstep with the app.
        """
        # Arrange
        js = _CREATE_INDEXES_JS.read_text()
        named = [
            "workflow_key_active_unique",
            "job_id_unique",
            "status_updated_at",
            "terminal_ttl",
        ]

        # Assert
        for name in named:
            assert name in js
        assert "db.locks.createIndex({ active: 1 })" in js


class TestEnsureIndexes:
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_ensure_indexes_should_create_specs(self):
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
    async def test_ensure_indexes_should_be_idempotent_on_repeat(self):
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
    async def test_ensure_indexes_should_drop_and_recreate_on_options_conflict(
        self, mocker
    ):
        """Test a changed index spec is dropped and recreated.

        Given:
            A collection whose create_index first raises an
            IndexOptionsConflict (code 85) then succeeds.
        When:
            ensure_indexes is awaited with that spec.
        Then:
            It should drop the named index and recreate it.
        """
        # Arrange
        collection = mocker.Mock()
        collection.create_index = mocker.AsyncMock(
            side_effect=[OperationFailure("conflict", 85), None]
        )
        collection.drop_index = mocker.AsyncMock()
        db = {"jobs": collection}
        spec = IndexSpec("jobs", [("job_id", 1)], name="job_id_unique", unique=True)

        # Act
        count = await ensure_indexes(db, [spec])

        # Assert
        assert count == 1
        collection.drop_index.assert_awaited_once_with("job_id_unique")
        assert collection.create_index.await_count == 2

    @pytest.mark.asyncio
    async def test_ensure_indexes_should_reraise_unexpected_failure(self, mocker):
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
        collection = mocker.Mock()
        collection.create_index = mocker.AsyncMock(
            side_effect=OperationFailure("unauthorized", 13)
        )
        collection.drop_index = mocker.AsyncMock()
        db = {"jobs": collection}
        spec = IndexSpec("jobs", [("job_id", 1)], name="job_id_unique")

        # Act & assert
        with pytest.raises(OperationFailure):
            await ensure_indexes(db, [spec])
        collection.drop_index.assert_not_awaited()
