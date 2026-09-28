"""Tests for the shared router preamble in ``cfdb.api.routers._helpers``."""

from __future__ import annotations

import asyncio
from itertools import cycle

import pytest
from fastapi import HTTPException
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from cfdb import api
from cfdb.api.routers._helpers import (
    FILE_DOC_PROJECTION,
    enforce_hubmap_access,
    lookup_file_doc,
    resolve_file_doc,
)
from cfdb.dcc_registry import get_all_dcc_names, normalize_dcc_name


def _make_file_doc(**overrides):
    """Return a minimal projected file document."""
    doc = {
        "submission": "4dn",
        "local_id": "4DNFIABC123",
        "filename": "sample.mcool",
        "md5": "d41d8cd98f00b204e9800998ecf8427e",
        "access_url": "https://data.4dnucleome.org/files/sample.mcool",
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
        "file_format": {"name": "HDF5"},
    }
    doc.update(overrides)
    return doc


class TestFileDocProjection:
    """The projection is the only thing that limits what routers can read."""

    def test_projection_should_select_genome_assembly(self):
        """Test that the projection carries the field coordSystem derives from.

        Given:
            ``FILE_DOC_PROJECTION``, the projection every router reads
            through.
        When:
            The tileset routers derive HiGlass's ``coordSystem`` from
            ``genome_assembly``.
        Then:
            The projection MUST select it. This is asserted structurally
            rather than behaviourally because ``FakeCollection.find_one``
            ignores ``projection`` entirely, so a field omitted here still
            reaches every unit test and only returns ``None`` in
            production.
        """
        # Assert
        assert FILE_DOC_PROJECTION.get("genome_assembly") == 1

    def test_projection_should_strip_the_mongo_object_id(self):
        """Test that the document never carries a bson ObjectId.

        Given:
            ``FILE_DOC_PROJECTION``.
        When:
            A projected document is handed to the workflow subsystem and
            pickled across the wool boundary.
        Then:
            ``_id`` MUST be excluded, since ``bson.ObjectId`` is not part
            of the workflow subsystem's contract.
        """
        # Assert
        assert FILE_DOC_PROJECTION["_id"] == 0


class TestLookupFileDoc:
    """The canonical query keeping every router on one record."""

    @pytest.mark.asyncio
    async def test_should_prefer_the_materialized_files_collection(self, mock_db):
        """Test that the materialized document wins over the raw one.

        Given:
            Distinguishable documents for the same ``(submission,
            local_id)`` pair in both the materialized ``files`` collection
            and the raw ``file`` collection.
        When:
            lookup_file_doc is called.
        Then:
            It should return the ``files`` document, so all four routers
            resolve the same record and the workflow-key mutex derived
            from it actually serializes concurrent requests.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc(filename="materialized.mcool")]
        mock_db.file.docs = [_make_file_doc(filename="raw.mcool")]

        # Act
        file_doc = await lookup_file_doc(mock_db, "4dn", "4DNFIABC123")

        # Assert
        assert file_doc["filename"] == "materialized.mcool"


class TestEnforceHubmapAccess:
    """The fail-closed HuBMAP access guard shared by every router."""

    @given(
        normalized_dcc=st.sampled_from(["hubmap", "4dn", "encode"]),
        level=st.one_of(
            st.none(),
            st.just("public"),
            st.sampled_from(
                ["Public", "PUBLIC", " public", "public ", "protected", "consortium", ""]
            ),
            st.text(max_size=32),
        ),
        field_present=st.booleans(),
    )
    @settings(max_examples=50)
    def test_should_forbid_every_non_exact_public_level_under_hubmap(
        self, normalized_dcc, level, field_present
    ):
        """Test that the guard is fail-closed and total over its domain.

        Given:
            A normalized DCC drawn from the registry and a
            ``data_access_level`` drawn from None, the exact string
            ``"public"``, casing and padding variants of it, the known
            non-public levels, arbitrary text, or an absent field.
        When:
            enforce_hubmap_access is called with a document carrying that
            access level.
        Then:
            It should raise HTTP 403 for every HuBMAP document whose level
            is not exactly ``"public"``, return None for an exact
            ``"public"``, and never raise for any other DCC.
        """
        # Arrange
        file_doc = {"data_access_level": level} if field_present else {}

        # Act & assert
        if normalized_dcc != "hubmap":
            assert enforce_hubmap_access(normalized_dcc, file_doc) is None
        elif field_present and level == "public":
            assert enforce_hubmap_access(normalized_dcc, file_doc) is None
        else:
            with pytest.raises(HTTPException) as exc_info:
                enforce_hubmap_access(normalized_dcc, file_doc)
            assert exc_info.value.status_code == 403


class TestResolveFileDoc:
    """The preamble every file-serving router shares."""

    @pytest.mark.asyncio
    async def test_should_return_the_normalized_dcc_and_document(self, mock_db):
        """Test the success path normalizes the DCC and returns the record.

        Given:
            A file document present under submission ``4dn``.
        When:
            resolve_file_doc is called with a mixed-case DCC name.
        Then:
            It should return the lower-cased DCC and the projected
            document.
        """
        # Arrange
        mock_db.file.docs = [_make_file_doc()]

        # Act
        normalized_dcc, file_doc = await resolve_file_doc("4DN", "4DNFIABC123")

        # Assert
        assert normalized_dcc == "4dn"
        assert file_doc["local_id"] == "4DNFIABC123"

    @pytest.mark.asyncio
    async def test_should_reject_an_unknown_dcc_with_400(self, mock_db):
        """Test that an unrecognized DCC is a client error.

        Given:
            A DCC name that is not in the registry.
        When:
            resolve_file_doc is called.
        Then:
            It should raise HTTP 400 naming the valid DCCs, before any
            database access.
        """
        # Act
        with pytest.raises(HTTPException) as exc_info:
            await resolve_file_doc("nosuchdcc", "abc")

        # Assert
        assert exc_info.value.status_code == 400
        assert "nosuchdcc" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_should_report_500_when_the_database_is_unwired(self, mocker):
        """Test that a missing database is a server error, not a 404.

        Given:
            ``api.db`` is None, as it is before the lifespan runs.
        When:
            resolve_file_doc is called with a valid DCC.
        Then:
            It should raise HTTP 500 rather than reporting the file
            missing.
        """
        # Arrange
        mocker.patch.object(api, "db", None)

        # Act
        with pytest.raises(HTTPException) as exc_info:
            await resolve_file_doc("4dn", "4DNFIABC123")

        # Assert
        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_should_report_404_when_no_document_matches(self, mock_db):
        """Test that an absent record is a 404.

        Given:
            An empty file collection.
        When:
            resolve_file_doc is called.
        Then:
            It should raise HTTP 404.
        """
        # Arrange
        mock_db.file.docs = []

        # Act
        with pytest.raises(HTTPException) as exc_info:
            await resolve_file_doc("4dn", "4DNFIABC123")

        # Assert
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_should_not_require_an_access_url_by_default(self, mock_db):
        """Test that a record with no access URL resolves for /index.

        Given:
            A document with no ``access_url`` — servable by /index from an
            upstream sidecar, and tileable from a cached artifact.
        When:
            resolve_file_doc is called without ``require_access_url``.
        Then:
            It should return the document rather than raising 501.
        """
        # Arrange
        doc = _make_file_doc()
        del doc["access_url"]
        mock_db.file.docs = [doc]

        # Act
        _, file_doc = await resolve_file_doc("4dn", "4DNFIABC123")

        # Assert
        assert file_doc.get("access_url") is None

    @pytest.mark.asyncio
    async def test_should_report_501_when_an_access_url_is_required(self, mock_db):
        """Test that /data's no-access-method case stays a 501.

        Given:
            A document with no ``access_url``.
        When:
            resolve_file_doc is called with ``require_access_url=True``.
        Then:
            It should raise HTTP 501, the code /data has always returned
            for a file with no supported access method.
        """
        # Arrange
        doc = _make_file_doc()
        del doc["access_url"]
        mock_db.file.docs = [doc]

        # Act
        with pytest.raises(HTTPException) as exc_info:
            await resolve_file_doc("4dn", "4DNFIABC123", require_access_url=True)

        # Assert
        assert exc_info.value.status_code == 501

    @pytest.mark.asyncio
    async def test_should_refuse_a_non_public_hubmap_file_with_403(self, mock_db):
        """Test that the HuBMAP access guard runs on every resolution.

        Given:
            A HuBMAP document whose ``data_access_level`` is not public.
        When:
            resolve_file_doc is called.
        Then:
            It should raise HTTP 403, so no caller can reach a protected
            record regardless of which router asked.
        """
        # Arrange
        mock_db.file.docs = [
            _make_file_doc(
                submission="hubmap",
                local_id="HBM123",
                data_access_level="consortium",
            )
        ]

        # Act
        with pytest.raises(HTTPException) as exc_info:
            await resolve_file_doc("hubmap", "HBM123")

        # Assert
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_should_refuse_a_protected_hubmap_file_before_the_501(
        self, mock_db
    ):
        """Test that /data's 501 is checked ahead of the access guard.

        Given:
            A non-public HuBMAP document that also has no ``access_url``.
        When:
            resolve_file_doc is called with ``require_access_url=True``.
        Then:
            It should raise 501, not 403 — pinning the ordering /data has
            always had, so that a signed URL is never logged before the
            access guard runs.
        """
        # Arrange
        doc = _make_file_doc(
            submission="hubmap", local_id="HBM123", data_access_level="protected"
        )
        del doc["access_url"]
        mock_db.file.docs = [doc]

        # Act
        with pytest.raises(HTTPException) as exc_info:
            await resolve_file_doc("hubmap", "HBM123", require_access_url=True)

        # Assert
        assert exc_info.value.status_code == 501

    @given(
        dcc=st.sampled_from(get_all_dcc_names()),
        upper_mask=st.lists(st.booleans(), min_size=1, max_size=8),
        unknown=st.text(max_size=32),
    )
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_should_normalize_any_casing_and_reject_unknown_dccs(
        self, mock_db, dcc, upper_mask, unknown
    ):
        """Test DCC normalization and rejection across the input domain.

        Given:
            A registered DCC name under a generated per-character casing,
            and an arbitrary string that does not normalize to any
            registered DCC, with a matching document seeded for the
            registered one.
        When:
            resolve_file_doc is called with each.
        Then:
            It should resolve every casing variant to the lower-cased DCC
            and return the seeded document, and reject every unknown
            string with exactly HTTP 400.
        """
        # Arrange
        assume(normalize_dcc_name(unknown) not in get_all_dcc_names())
        variant = "".join(
            ch.upper() if up else ch.lower() for ch, up in zip(dcc, cycle(upper_mask))
        )
        mock_db.file.docs = [
            _make_file_doc(submission=dcc, data_access_level="public")
        ]

        # Act
        normalized_dcc, file_doc = asyncio.run(
            resolve_file_doc(variant, "4DNFIABC123")
        )
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(resolve_file_doc(unknown, "4DNFIABC123"))

        # Assert
        assert normalized_dcc == dcc
        assert file_doc["local_id"] == "4DNFIABC123"
        assert exc_info.value.status_code == 400
