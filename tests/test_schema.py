"""Tests for simplified GraphQL files query."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from mongomock_motor import AsyncMongoMockClient
from starlette.testclient import TestClient
from strawberry.printer import print_schema

from cfdb import api
from cfdb.api import main
from cfdb.api.gql.schema import from_pydantic, schema
from cfdb.api.gql.types import FileMetadataType
from cfdb.models import FileMetadataModel
from cfdb.services import locks


def test_from_pydantic_should_convert_nested_model_lists_and_leave_json_untouched():
    """Test from_pydantic resolves Optional[List[Model]] and passes JSON through.

    Given:
        A dumped FileMetadataModel with a nested Optional[List[ExtraFile]]
        (extra.fourdn.extra_files) and a JSON dict field
        (collections[].extra.hubmap.metadata).
    When:
        from_pydantic builds the FileMetadataType.
    Then:
        It should convert the nested list items to the Strawberry type
        (the #52 peel fix) while leaving the JSON dict field untouched.
    """
    # Arrange
    payload = FileMetadataModel(
        dcc={"dcc_abbreviation": "4DN_DCIC"},
        collections=[
            {"biosamples": [], "extra": {"hubmap": {"metadata": {"k": "v", "n": 1}}}}
        ],
        extra={"fourdn": {"extra_files": [{"href": "/x", "file_format": "pairs_px2"}]}},
    ).model_dump()

    # Act
    result = from_pydantic(FileMetadataType, payload)

    # Assert
    extra_file = result.extra.fourdn.extra_files[0]
    assert hasattr(type(extra_file), "__strawberry_definition__")
    assert extra_file.file_format == "pairs_px2"
    assert result.collections[0].extra.hubmap.metadata == {"k": "v", "n": 1}


def _make_file_doc(
    local_id: str, submission: str = "hubmap", size_in_bytes: int | None = None
) -> dict:
    """Return a minimal file document that satisfies FileMetadataModel."""
    return {
        "id_namespace": "ns",
        "local_id": local_id,
        "project_id_namespace": "ns",
        "project_local_id": "proj",
        "filename": f"{local_id}.bam",
        "submission": submission,
        "data_access_level": "public",
        "size_in_bytes": size_in_bytes,
        "dcc": {
            "dcc_name": submission.upper(),
            "dcc_abbreviation": submission,
        },
        "collections": [],
    }


def _named_type(type_ref: dict) -> str | None:
    """Unwrap an introspection type reference down to its named type."""
    while type_ref is not None and type_ref.get("name") is None:
        type_ref = type_ref.get("ofType")
    return type_ref.get("name") if type_ref else None


def _make_distinct_doc(local_id: str, dcc_name: str, submission: str = "hubmap") -> dict:
    """Return a file document with a configurable dcc_name for distinct-values tests."""
    return {
        "id_namespace": "ns",
        "local_id": local_id,
        "project_id_namespace": "ns",
        "project_local_id": "proj",
        "filename": f"{local_id}.bam",
        "submission": submission,
        "data_access_level": "public",
        "dcc": {
            "dcc_name": dcc_name,
            "dcc_abbreviation": submission,
        },
        "collections": [],
    }


class TestFilesQuery:
    @pytest.fixture(autouse=True)
    def _patch_cutover(self, mocker):
        """No-op ``locks.wait_for_cutover`` for every test in this class."""
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    @pytest.mark.asyncio
    async def test_files_should_return_page_size_items_when_more_documents_match(
        self, mock_db
    ):
        """Test the pagination cap is applied to the files query.

        Given:
            Three files in the database.
        When:
            The GraphQL files query is executed with page=0, page_size=2.
        Then:
            It should return exactly 2 files (no access-level over-fetch logic).
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 2) {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["files"]["items"]) == 2

    @pytest.mark.asyncio
    async def test_files_should_return_an_error_when_page_size_is_zero(self, mock_db):
        """Test a page size of zero is refused rather than fetching everything.

        Given:
            Thirty files in the database — more than the default page size,
            so an unbounded fetch is distinguishable from a default page.
        When:
            The GraphQL files query is executed with pageSize: 0, which
            MongoDB would read as "no limit".
        Then:
            It should return a GraphQL error naming the argument, the
            rejected value and the accepted range, return no data, and
            point the caller at the fileCount query.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc(f"f{i}") for i in range(30)]

        # Act
        result = await schema.execute(
            """
            query {
                files(pageSize: 0) {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is not None
        assert result.data is None
        message = result.errors[0].message
        # The literal ceiling is asserted here rather than interpolated from
        # MAX_PAGE_SIZE: it is a documented part of the client contract
        # (README), so raising it should fail loudly and prompt a doc update.
        assert "pageSize must be between 1 and 500" in message
        assert "(got 0)" in message
        assert "fileCount" in message

    @pytest.mark.asyncio
    async def test_files_should_return_an_error_when_page_size_is_negative(self, mock_db):
        """Test a negative page size is refused rather than quietly clamped.

        Given:
            Three files in the database.
        When:
            The GraphQL files query is executed with pageSize: -1, which the
            MongoDB wire protocol reads as "at most one, then close".
        Then:
            It should return a GraphQL error naming the rejected value and
            return no data.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(pageSize: -1) {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is not None
        assert result.data is None
        assert "pageSize" in result.errors[0].message
        assert "(got -1)" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_files_should_return_an_error_when_page_size_exceeds_the_maximum(self, mock_db):
        """Test the page size ceiling is enforced.

        Given:
            A file in the database.
        When:
            The GraphQL files query is executed with a page size one above
            MAX_PAGE_SIZE.
        Then:
            It should return a GraphQL error naming the rejected value and
            return no data.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("f1")]
        over_ceiling = api.MAX_PAGE_SIZE + 1

        # Act
        result = await schema.execute(
            """
            query Files($pageSize: Int!) {
                files(pageSize: $pageSize) {
                    items {
                        localId
                    }
                }
            }
            """,
            variable_values={"pageSize": over_ceiling},
        )

        # Assert
        assert result.errors is not None
        assert result.data is None
        assert f"(got {over_ceiling})" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_files_should_return_an_error_when_page_is_negative(self, mock_db, mocker):
        """Test a negative page is refused before a cursor is ever built.

        Given:
            Three files in the database and spies on both collection calls
            the resolver makes.
        When:
            The GraphQL files query is executed with page: -1, which would
            otherwise reach pymongo as a negative skip.
        Then:
            It should return a GraphQL error naming the page argument rather
            than leaking the driver's own skip complaint, and it should not
            query the collection at all.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]
        find = mocker.spy(mock_db.files, "find")
        count_documents = mocker.spy(mock_db.files, "count_documents")

        # Act
        result = await schema.execute(
            """
            query {
                files(page: -1) {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is not None
        assert result.data is None
        message = result.errors[0].message
        assert "page must be >= 0 (got -1)" in message
        assert "skip" not in message
        assert find.call_count == 0
        assert count_documents.call_count == 0

    # GraphQL's Int scalar is 32-bit, so values outside it are rejected during
    # variable coercion rather than by the resolver. Bounding the generated
    # domain keeps this test about the resolver's own validation.
    _INT32_MIN = -(2**31)
    _INT32_MAX = 2**31 - 1

    @given(
        pagination=st.one_of(
            st.tuples(
                st.integers(_INT32_MIN, -1),
                st.integers(1, api.MAX_PAGE_SIZE),
            ),
            st.tuples(
                st.integers(0, 20),
                st.one_of(
                    st.integers(_INT32_MIN, 0),
                    st.integers(api.MAX_PAGE_SIZE + 1, _INT32_MAX),
                ),
            ),
        )
    )
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_files_should_return_an_error_when_any_pagination_argument_is_out_of_range(
        self, mock_db, pagination
    ):
        """Test no out-of-range pagination request ever returns documents.

        Given:
            A fixed collection of 12 files and a pagination pair in which at
            least one of page and page_size falls outside the accepted range.
        When:
            The GraphQL files query runs with those pagination arguments.
        Then:
            It should always return a validation error naming the offending
            argument and the rejected value, and never any data.
        """
        # Arrange
        # As in ``test_files_should_report_total_count_invariant_under_pagination``:
        # ``mock_db`` is function-scoped and reused across examples, so the
        # dataset is reseeded each time, and the async resolver is driven
        # via asyncio.run.
        page, page_size = pagination
        mock_db.files.docs = [_make_file_doc(f"f{i}") for i in range(12)]

        # Act
        result = asyncio.run(
            schema.execute(
                """
                query Files($page: Int!, $pageSize: Int!) {
                    files(page: $page, pageSize: $pageSize) {
                        totalCount
                        items {
                            localId
                        }
                    }
                }
                """,
                variable_values={"page": page, "pageSize": page_size},
            )
        )

        # Assert
        assert result.errors is not None
        assert result.data is None
        message = result.errors[0].message
        # Asserting the message shape, not just that something failed: the
        # cursor double rejects a negative skip too, so a bare "an error
        # occurred" oracle would be satisfied by the driver over half this
        # domain even with the resolver's own guard removed.
        assert message.startswith(("page must be", "pageSize must be"))
        assert f"(got {page if page < 0 else page_size})" in message

    @pytest.mark.asyncio
    async def test_files_should_not_log_an_error_when_pagination_is_out_of_range(
        self, mock_db, caplog
    ):
        """Test a refused request is logged as a client mistake, not a fault.

        Given:
            Log capture over the whole application.
        When:
            The GraphQL files query is executed with pageSize: 0.
        Then:
            It should record the rejection without an ERROR-level entry, so
            an anonymous caller cannot drive the error stream a deployment
            alerts on.
        """
        # Act
        with caplog.at_level(logging.INFO):
            result = await schema.execute("{ files(pageSize: 0) { totalCount } }")

        # Assert
        assert result.errors is not None
        assert [r.message for r in caplog.records if r.levelno >= logging.ERROR] == []
        assert any("pageSize must be between" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_files_should_log_an_error_when_the_resolver_fails_unexpectedly(
        self, mock_db, mocker, caplog
    ):
        """Test a genuine fault still reaches the error stream.

        Given:
            A cutover wait that raises, standing in for an unexpected
            server-side failure.
        When:
            An in-range GraphQL files query is executed.
        Then:
            It should record the failure at ERROR with the exception
            attached, unlike a refused pagination argument.
        """
        # Arrange
        mocker.patch.object(
            locks, "wait_for_cutover", side_effect=RuntimeError("cutover exploded")
        )

        # Act
        with caplog.at_level(logging.INFO):
            result = await schema.execute("{ files { totalCount } }")

        # Assert
        assert result.errors is not None
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors
        assert errors[0].exc_info is not None

    @pytest.mark.asyncio
    async def test_files_should_return_one_item_when_page_size_is_one(self, mock_db):
        """Test the smallest accepted page size is served, not rejected.

        Given:
            Three files in the database.
        When:
            The GraphQL files query is executed with page: 1, pageSize: 1.
        Then:
            It should return exactly the second file, with no errors.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 1, pageSize: 1) {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert [f["localId"] for f in result.data["files"]["items"]] == ["f2"]

    @pytest.mark.asyncio
    async def test_files_should_return_items_when_page_size_is_the_maximum(
        self, mock_db
    ):
        """Test the page size ceiling is inclusive.

        Given:
            Three files in the database.
        When:
            The GraphQL files query is executed with a page size of exactly
            MAX_PAGE_SIZE.
        Then:
            It should return all three files, with no errors.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        # Act
        result = await schema.execute(
            """
            query Files($pageSize: Int!) {
                files(pageSize: $pageSize) {
                    items {
                        localId
                    }
                }
            }
            """,
            variable_values={"pageSize": api.MAX_PAGE_SIZE},
        )

        # Assert
        assert result.errors is None
        assert len(result.data["files"]["items"]) == 3

    @pytest.mark.asyncio
    async def test_files_should_return_a_default_page_when_pagination_is_omitted(
        self, mock_db
    ):
        """Test the default page size still satisfies the new bounds.

        Given:
            Five more files than the default page size.
        When:
            The GraphQL files query is executed with neither pagination
            argument supplied.
        Then:
            It should return exactly PAGE_SIZE files alongside the full
            match count.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc(f"f{i}") for i in range(api.PAGE_SIZE + 5)
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["files"]["items"]) == api.PAGE_SIZE
        assert result.data["files"]["totalCount"] == api.PAGE_SIZE + 5

    @pytest.mark.asyncio
    async def test_files_should_return_total_count_independent_of_page_size(
        self, mock_db
    ):
        """Test totalCount reports every match, not just the returned page.

        Given:
            Three files in the database.
        When:
            The GraphQL files query is executed with a page_size of 2.
        Then:
            It should return a totalCount of 3 alongside only 2 items.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 2) {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 3
        assert len(result.data["files"]["items"]) == 2

    @pytest.mark.asyncio
    async def test_files_should_return_stable_total_count_across_pages(self, mock_db):
        """Test totalCount does not vary with the page being requested.

        Given:
            Three files in the database.
        When:
            The same query is executed for page 0 and then page 1.
        Then:
            It should report the same totalCount of 3 on both pages.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]
        query = """
            query Files($page: Int!) {
                files(page: $page, pageSize: 2) {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """

        # Act
        first = await schema.execute(query, variable_values={"page": 0})
        second = await schema.execute(query, variable_values={"page": 1})

        # Assert
        assert first.errors is None
        assert second.errors is None
        assert first.data["files"]["totalCount"] == 3
        assert second.data["files"]["totalCount"] == 3
        assert len(first.data["files"]["items"]) == 2
        assert len(second.data["files"]["items"]) == 1

    @pytest.mark.asyncio
    async def test_files_should_return_true_total_count_when_page_past_end(
        self, mock_db
    ):
        """Test a page beyond the last still reports the real match count.

        Given:
            Three files in the database.
        When:
            The GraphQL files query requests page 5 with a page_size of 2.
        Then:
            It should return an empty items list alongside a totalCount of 3.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1"),
            _make_file_doc("f2"),
            _make_file_doc("f3"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 5, pageSize: 2) {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 3
        assert result.data["files"]["items"] == []

    @pytest.mark.asyncio
    async def test_files_should_return_zero_total_count_when_no_documents_match(
        self, mock_db
    ):
        """Test a filter matching nothing yields an empty envelope, not null.

        Given:
            Files in the database, none matching the requested filename.
        When:
            The GraphQL files query filters on that absent filename.
        Then:
            It should return a totalCount of 0 and an empty items list.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("f1"), _make_file_doc("f2")]

        # Act
        result = await schema.execute(
            """
            query {
                files(input: [{ filename: ["absent.bam"] }]) {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 0
        assert result.data["files"]["items"] == []

    @pytest.mark.asyncio
    async def test_files_should_return_the_uncompressed_sentinel_as_an_empty_string(
        self, mock_db
    ):
        """Test that the uncompressed sentinel survives the resolver as "".

        Given:
            One file known to be uncompressed and one gzipped file.
        When:
            The GraphQL files query selects compressionFormat.
        Then:
            It should return "" for the uncompressed file rather than null,
            preserving the distinction from an undetermined value.
        """
        # Arrange
        mock_db.files.docs = [
            {**_make_file_doc("f1"), "compression_format": ""},
            {**_make_file_doc("f2"), "compression_format": "format:3989"},
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(input: []) {
                    items {
                        localId
                        compressionFormat
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        returned = {
            item["localId"]: item["compressionFormat"]
            for item in result.data["files"]["items"]
        }
        assert returned == {"f1": "", "f2": "format:3989"}

    @pytest.mark.asyncio
    async def test_files_should_filter_documents_by_compression_format(self, mock_db):
        """Test that the derived term is queryable through the input filter.

        Given:
            An uncompressed file, a gzipped file and a bgzipped file.
        When:
            The GraphQL files query filters on the gzip term.
        Then:
            It should return only the gzipped file.
        """
        # Arrange
        mock_db.files.docs = [
            {**_make_file_doc("f1"), "compression_format": ""},
            {**_make_file_doc("f2"), "compression_format": "format:3989"},
            {**_make_file_doc("f3"), "compression_format": "format:3615"},
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(input: [{ compressionFormat: ["format:3989"] }]) {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 1
        assert result.data["files"]["items"] == [{"localId": "f2"}]

    @pytest.mark.asyncio
    async def test_files_should_count_only_matching_documents_when_input_filter_supplied(
        self, mock_db
    ):
        """Test totalCount reflects the filter rather than the collection size.

        Given:
            Five files, of which two belong to the 4DN DCC.
        When:
            The GraphQL files query filters on the 4DN DCC abbreviation.
        Then:
            It should report a totalCount of 2, not the collection total of 5.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1", "4dn"),
            _make_file_doc("f2", "4dn"),
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("h2", "hubmap"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(input: [{ dcc: [{ dccAbbreviation: ["4dn"] }] }]) {
                    totalCount
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 2
        assert {f["localId"] for f in result.data["files"]["items"]} == {"f1", "f2"}

    @given(page=st.integers(0, 20), page_size=st.integers(1, 20))
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_files_should_report_total_count_invariant_under_pagination(
        self, mock_db, page, page_size
    ):
        """Test totalCount stays the full match count for any pagination window.

        Given:
            A fixed collection of 12 files and arbitrary page and page_size
            requests drawn from [0, 20] and [1, 20].
        When:
            The GraphQL files query runs with those pagination arguments.
        Then:
            It should always report a totalCount of 12, and items should hold
            exactly the requested page slice.
        """
        # Arrange
        # ``mock_db`` is function-scoped, so Hypothesis reuses one instance
        # across examples (hence the suppressed health check); reseeding the
        # constant dataset each example keeps them independent. The resolver
        # is async and Hypothesis does not compose with pytest-asyncio, so it
        # is driven synchronously via asyncio.run.
        all_ids = [f"f{i}" for i in range(12)]
        mock_db.files.docs = [_make_file_doc(local_id) for local_id in all_ids]

        # Act
        result = asyncio.run(
            schema.execute(
                """
                query Files($page: Int!, $pageSize: Int!) {
                    files(page: $page, pageSize: $pageSize) {
                        totalCount
                        items {
                            localId
                        }
                    }
                }
                """,
                variable_values={"page": page, "pageSize": page_size},
            )
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 12
        skip = page * page_size
        assert len(result.data["files"]["items"]) == len(
            all_ids[skip : skip + page_size]
        )

    @pytest.mark.asyncio
    async def test_files_should_expose_display_title_token_when_extra_file_format_is_cv_object(
        self, mock_db
    ):
        """Test the files query serializes 4DN extra_files with a CV-object format.

        Given:
            A 4DN file whose extra.fourdn.extra_files[0].file_format is a 4DN
            CV object (a dict carrying the token under display_title) — the
            shape that crashed the query.
        When:
            The GraphQL files query selects extra.fourdn.extraFiles.fileFormat.
        Then:
            It should return without errors and expose the display_title token.
        """
        # Arrange
        doc = _make_file_doc("4DNFITEST", submission="4dn")
        doc["extra"] = {
            "fourdn": {
                "extra_files": [
                    {
                        "href": "/files/x.pairs_px2",
                        "file_format": {
                            "status": "released",
                            "display_title": "pairs_px2",
                        },
                    }
                ]
            }
        }
        mock_db.files.docs = [doc]

        # Act
        result = await schema.execute(
            """
            query {
                files {
                    items {
                        localId
                        extra { fourdn { extraFiles { href fileFormat } } }
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        extra_file = result.data["files"]["items"][0]["extra"]["fourdn"]["extraFiles"][0]
        assert extra_file["fileFormat"] == "pairs_px2"
        assert extra_file["href"] == "/files/x.pairs_px2"

    @pytest.mark.asyncio
    async def test_files_should_return_file_when_collection_protocol_fields_are_floats(
        self, mock_db
    ):
        """Test the files query serializes 4DN float collection protocol fields.

        Given:
            A 4DN file whose collections[0].extra.fourdn carries float
            protocol fields (the shape persisted by the sync that crashed the
            query with a string_type validation error).
        When:
            The GraphQL files query selects only localId.
        Then:
            It should return without errors and the file should be present
            (no data:null).
        """
        # Arrange
        doc = _make_file_doc("4DNFIFLOAT", submission="4dn")
        doc["collections"] = [
            {
                "biosamples": [],
                "extra": {
                    "fourdn": {
                        "crosslinking_temperature": 25.0,
                        "ligation_volume": 0.12,
                        "digestion_time": 960.0,
                    }
                },
            }
        ]
        mock_db.files.docs = [doc]

        # Act
        result = await schema.execute(
            """
            query {
                files {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["files"]["items"]) == 1
        assert result.data["files"]["items"][0]["localId"] == "4DNFIFLOAT"

    @pytest.mark.asyncio
    async def test_files_should_return_string_forms_when_collection_protocol_fields_are_floats(
        self, mock_db
    ):
        """Test the files query exposes float protocol fields as string forms.

        Given:
            A 4DN file whose collections[0].extra.fourdn carries float
            protocol fields.
        When:
            The GraphQL files query selects the nested protocol fields.
        Then:
            It should return without errors and each value should be the
            string form.
        """
        # Arrange
        doc = _make_file_doc("4DNFIFLOAT2", submission="4dn")
        doc["collections"] = [
            {
                "biosamples": [],
                "extra": {
                    "fourdn": {
                        "crosslinking_temperature": 25.0,
                        "ligation_volume": 0.12,
                        "digestion_time": 960.0,
                    }
                },
            }
        ]
        mock_db.files.docs = [doc]

        # Act
        result = await schema.execute(
            """
            query {
                files {
                    items {
                        localId
                        collections {
                            extra {
                                fourdn {
                                    crosslinkingTemperature
                                    ligationVolume
                                    digestionTime
                                }
                            }
                        }
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        fourdn = result.data["files"]["items"][0]["collections"][0]["extra"]["fourdn"]
        assert fourdn["crosslinkingTemperature"] == "25.0"
        assert fourdn["ligationVolume"] == "0.12"
        assert fourdn["digestionTime"] == "960.0"

    @pytest.mark.asyncio
    async def test_files_should_return_all_items_when_page_mixes_float_and_clean_docs(
        self, mock_db
    ):
        """Test a page mixing a float-protocol doc with clean docs returns all.

        Given:
            A page containing one 4DN file with float collection protocol
            fields alongside two clean files (the blast-radius case where one
            bad doc previously failed the whole page).
        When:
            The GraphQL files query selects localId.
        Then:
            It should return without errors and all three files should be
            present.
        """
        # Arrange
        float_doc = _make_file_doc("4DNFIMIX", submission="4dn")
        float_doc["collections"] = [
            {
                "biosamples": [],
                "extra": {"fourdn": {"digestion_time": 960.0}},
            }
        ]
        mock_db.files.docs = [
            float_doc,
            _make_file_doc("clean1", "hubmap"),
            _make_file_doc("clean2", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 10) {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        returned = {f["localId"] for f in result.data["files"]["items"]}
        assert returned == {"4DNFIMIX", "clean1", "clean2"}

    @pytest.mark.asyncio
    async def test_files_should_return_all_dccs_when_no_input_filter_supplied(
        self, mock_db
    ):
        """Test the files query returns all DCCs when no filter is supplied.

        Given:
            Files from multiple DCCs including HuBMAP.
        When:
            The GraphQL files query is executed with no input filter.
        Then:
            It should return files from all DCCs without access-level
            filtering.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                files(page: 0, pageSize: 10) {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["files"]["items"]) == 3


class TestDistinctValuesQuery:
    @pytest.fixture(autouse=True)
    def _patch_cutover(self, mocker):
        """No-op ``locks.wait_for_cutover`` for every test in this class."""
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    @pytest.mark.asyncio
    async def test_distinct_values_returns_all_unique_values_for_single_field(
        self, mock_db
    ):
        """Test distinct values for a single nested field without filtering.

        Given:
            Three files with different dcc.dcc_name values
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name"]
        Then:
            It should return one entry containing all three distinct DCC names
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP", "hubmap"),
            _make_distinct_doc("f2", "4DN", "4dn"),
            _make_distinct_doc("f3", "ENCODE", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert len(result.data["distinctValues"]) == 1
        entry = result.data["distinctValues"][0]
        assert entry["field"] == "dcc.dcc_name"
        assert sorted(entry["values"]) == ["4DN", "ENCODE", "HuBMAP"]

    @pytest.mark.asyncio
    async def test_distinct_values_returns_entries_for_multiple_fields(
        self, mock_db
    ):
        """Test distinct values for multiple fields in one call.

        Given:
            Three files spanning two DCCs and two abbreviations
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name", "dcc.dcc_abbreviation"]
        Then:
            It should return two entries, each with the correct distinct values
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP", "hubmap"),
            _make_distinct_doc("f2", "HuBMAP", "hubmap"),
            _make_distinct_doc("f3", "4DN", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name", "dcc.dcc_abbreviation"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entries = result.data["distinctValues"]
        assert len(entries) == 2
        by_field = {e["field"]: e["values"] for e in entries}
        assert sorted(by_field["dcc.dcc_name"]) == ["4DN", "HuBMAP"]
        assert sorted(by_field["dcc.dcc_abbreviation"]) == ["4dn", "hubmap"]

    @pytest.mark.asyncio
    async def test_distinct_values_applies_input_filter(self, mock_db):
        """Test distinct values with a DCC filter applied.

        Given:
            Three files, two from HuBMAP and one from 4DN
        When:
            The distinctValues query is executed with a DCC filter for HuBMAP and fields: ["dcc.dcc_abbreviation"]
        Then:
            It should return only the abbreviations from HuBMAP files
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("h1", "HuBMAP", "hubmap"),
            _make_distinct_doc("h2", "HuBMAP", "hubmap"),
            _make_distinct_doc("f1", "4DN", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(
                    fields: ["dcc.dcc_abbreviation"]
                    input: [{ dcc: [{ dccName: ["HuBMAP"] }] }]
                ) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["values"] == ["hubmap"]

    @pytest.mark.asyncio
    async def test_distinct_values_should_return_derived_compression_formats(
        self, mock_db
    ):
        """Test distinct values for a field the DCCs populate.

        Given:
            Three files carrying the uncompressed sentinel, the gzip term and
            the bgzip term
        When:
            The distinctValues query is executed with fields: ["compression_format"]
        Then:
            It should return all three as scalar strings, including the empty
            string
        """
        # Arrange
        mock_db.files.docs = [
            {**_make_distinct_doc("f1", "4DN"), "compression_format": ""},
            {
                **_make_distinct_doc("f2", "ENCODE"),
                "compression_format": "format:3989",
            },
            {
                **_make_distinct_doc("f3", "ENCODE"),
                "compression_format": "format:3615",
            },
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["compression_format"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert sorted(entry["values"]) == ["", "format:3615", "format:3989"]
        assert all(isinstance(value, str) for value in entry["values"])

    @pytest.mark.asyncio
    async def test_distinct_values_returns_empty_list_for_missing_field(
        self, mock_db
    ):
        """Test distinct values for a field absent from all documents.

        Given:
            Three files, none with a compression_format value
        When:
            The distinctValues query is executed with fields: ["compression_format"]
        Then:
            It should return one entry with an empty values list
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP"),
            _make_distinct_doc("f2", "4DN"),
            _make_distinct_doc("f3", "ENCODE"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["compression_format"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["values"] == []

    @pytest.mark.asyncio
    async def test_distinct_values_returns_empty_list_for_empty_database(
        self, mock_db
    ):
        """Test distinct values against an empty collection.

        Given:
            An empty database
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name"]
        Then:
            It should return one entry with an empty values list
        """
        # Arrange
        mock_db.files.docs = []

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["values"] == []

    @pytest.mark.asyncio
    async def test_distinct_values_deduplicates_values(self, mock_db):
        """Test that duplicate values are collapsed to unique entries.

        Given:
            Three files where two share the same dcc.dcc_name
        When:
            The distinctValues query is executed with fields: ["dcc.dcc_name"]
        Then:
            It should return only the deduplicated values
        """
        # Arrange
        mock_db.files.docs = [
            _make_distinct_doc("f1", "HuBMAP", "hubmap"),
            _make_distinct_doc("f2", "HuBMAP", "hubmap"),
            _make_distinct_doc("f3", "4DN", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["dcc.dcc_name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert sorted(entry["values"]) == ["4DN", "HuBMAP"]

    @pytest.mark.asyncio
    async def test_distinct_values_rejects_disallowed_field(self, mock_db):
        """Test that fields outside the allowlist are rejected.

        Given:
            A request for a field not in ALLOWED_DISTINCT_FIELDS
        When:
            The distinctValues query is executed with fields: ["secret_field"]
        Then:
            It should return an error naming the disallowed field
        """
        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["secret_field"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is not None
        assert "secret_field" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_distinct_values_returns_unique_subdoc_names(self, mock_db):
        """Test distinct values for an EDAM subdocument sub-path.

        Given:
            Three files whose file_format subdocuments carry two distinct names
        When:
            The distinctValues query is executed with fields: ["file_format.name"]
        Then:
            It should return one entry containing the two distinct format names
        """
        # Arrange
        def doc_with_format(local_id: str, fmt_id: str, fmt_name: str) -> dict:
            doc = _make_distinct_doc(local_id, "ENCODE", "encode")
            doc["file_format"] = {"id": fmt_id, "name": fmt_name}
            return doc

        mock_db.files.docs = [
            doc_with_format("f1", "format:3003", "BED"),
            doc_with_format("f2", "format:3003", "BED"),
            doc_with_format("f3", "format:3004", "bigBed"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["file_format.name"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        entry = result.data["distinctValues"][0]
        assert entry["field"] == "file_format.name"
        assert sorted(entry["values"]) == ["BED", "bigBed"]

    @pytest.mark.asyncio
    async def test_distinct_values_rejects_bare_subdocument_field(self, mock_db):
        """Test that the bare top-level subdocument name is no longer allowlisted.

        Given:
            The bare ``file_format`` field returned subdocuments rather than scalar
            values, so it has been removed from ALLOWED_DISTINCT_FIELDS in favor of
            the indexed ``file_format.id`` and ``file_format.name`` sub-paths.
        When:
            The distinctValues query is executed with fields: ["file_format"]
        Then:
            It should return an error naming the disallowed field
        """
        # Act
        result = await schema.execute(
            """
            query {
                distinctValues(fields: ["file_format"]) {
                    field
                    values
                }
            }
            """
        )

        # Assert
        assert result.errors is not None
        assert "file_format" in result.errors[0].message


# DCC abbreviations the fileCount property test draws documents and filters
# from. Kept small so generated docs use fake-resolvable dict paths
# (``dcc.dcc_abbreviation``).
_DCC_POOL = ["hubmap", "4dn", "encode"]


class TestFileCountQuery:
    @pytest.fixture(autouse=True)
    def _patch_cutover(self, mocker):
        """No-op ``locks.wait_for_cutover`` for every test in this class."""
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    @pytest.mark.asyncio
    async def test_file_count_should_return_total_when_no_filter(self, mock_db):
        """Test the file count reflects every document when no filter is supplied.

        Given:
            Three files spanning multiple DCCs
        When:
            The fileCount query is executed with no input filter
        Then:
            It should return the total number of files
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute("query { fileCount }")

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 3

    @pytest.mark.asyncio
    async def test_file_count_should_count_only_matches_when_filter_applied(
        self, mock_db
    ):
        """Test the file count honors an input filter.

        Given:
            Three files, two from HuBMAP and one from 4DN
        When:
            The fileCount query is executed with a DCC filter for HuBMAP
        Then:
            It should return only the count of matching files
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("h2", "hubmap"),
            _make_file_doc("f1", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                fileCount(input: [{ dcc: [{ dccAbbreviation: ["hubmap"] }] }])
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 2

    @pytest.mark.asyncio
    async def test_file_count_should_return_zero_when_filter_matches_nothing(
        self, mock_db
    ):
        """Test the file count is zero when no document satisfies the filter.

        Given:
            Three files, none from the filtered DCC
        When:
            The fileCount query is executed with a DCC filter for an absent DCC
        Then:
            It should return zero
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("h2", "hubmap"),
            _make_file_doc("f1", "4dn"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                fileCount(input: [{ dcc: [{ dccAbbreviation: ["encode"] }] }])
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 0

    @pytest.mark.asyncio
    async def test_file_count_should_return_zero_when_database_empty(self, mock_db):
        """Test the file count is zero against an empty collection.

        Given:
            An empty database
        When:
            The fileCount query is executed with no input filter
        Then:
            It should return zero
        """
        # Arrange
        mock_db.files.docs = []

        # Act
        result = await schema.execute("query { fileCount }")

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 0

    @pytest.mark.asyncio
    async def test_file_count_should_count_all_when_input_is_an_empty_list(
        self, mock_db
    ):
        """Test a present-but-empty input list counts every document.

        Given:
            Three files and an explicitly empty input list
        When:
            The fileCount query is executed with input: []
        Then:
            It should return the total, since an empty list is falsy and
            builds no filter (distinct from an omitted/null input)
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute("query { fileCount(input: []) }")

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 3

    @pytest.mark.asyncio
    async def test_file_count_should_count_the_union_when_a_field_lists_multiple_values(
        self, mock_db
    ):
        """Test multiple values in one field are combined as an OR union.

        Given:
            Three files, one each from HuBMAP, 4DN, and ENCODE
        When:
            The fileCount query is executed with a DCC filter listing two
            abbreviations (["hubmap", "4dn"])
        Then:
            It should return the union count of the two DCCs, excluding ENCODE
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                fileCount(input: [{ dcc: [{ dccAbbreviation: ["hubmap", "4dn"] }] }])
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 2

    @pytest.mark.asyncio
    async def test_file_count_should_count_the_union_when_input_has_multiple_entries(
        self, mock_db
    ):
        """Test multiple entries in the outer input list are combined as an OR.

        Given:
            Three files with distinct local IDs
        When:
            The fileCount query is executed with two separate input entries,
            each filtering a different local ID
        Then:
            It should return the union count of the two entries
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                fileCount(input: [{ localId: ["h1"] }, { localId: ["f1"] }])
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 2

    @pytest.mark.asyncio
    async def test_file_count_should_count_the_intersection_when_multiple_fields_filter(
        self, mock_db
    ):
        """Test multiple fields in one entry are combined as an AND intersection.

        Given:
            Three files: a public HuBMAP file, a non-public HuBMAP file, and a
            public 4DN file
        When:
            The fileCount query is executed with a filter combining a DCC field
            and a data-access-level field (HuBMAP AND public)
        Then:
            It should return only the file matching both conditions, excluding
            the non-public HuBMAP file and the public 4DN file
        """
        # Arrange
        public_hubmap = _make_file_doc("h1", "hubmap")
        protected_hubmap = _make_file_doc("h2", "hubmap")
        protected_hubmap["data_access_level"] = "protected"
        public_4dn = _make_file_doc("f1", "4dn")
        mock_db.files.docs = [public_hubmap, protected_hubmap, public_4dn]

        # Act
        result = await schema.execute(
            """
            query {
                fileCount(input: [{
                    dcc: [{ dccAbbreviation: ["hubmap"] }],
                    dataAccessLevel: ["public"]
                }])
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == 1

    @pytest.mark.asyncio
    async def test_file_count_should_equal_the_files_result_length_for_the_same_filter(
        self, mock_db
    ):
        """Test fileCount agrees with the number of files the same filter returns.

        Given:
            A mixed multi-DCC dataset smaller than one page
        When:
            The fileCount and files queries are executed with the same filter
        Then:
            fileCount should equal the length of the files result
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("h1", "hubmap"),
            _make_file_doc("h2", "hubmap"),
            _make_file_doc("f1", "4dn"),
            _make_file_doc("e1", "encode"),
        ]

        # Act
        result = await schema.execute(
            """
            query {
                fileCount(input: [{ dcc: [{ dccAbbreviation: ["hubmap"] }] }])
                files(input: [{ dcc: [{ dccAbbreviation: ["hubmap"] }] }], pageSize: 100) {
                    items {
                        localId
                    }
                }
            }
            """
        )

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == len(result.data["files"]["items"])
        assert result.data["fileCount"] == 2

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
            HealthCheck.too_slow,
        ],
    )
    @given(
        abbreviations=st.lists(st.sampled_from(_DCC_POOL), max_size=15),
        selected=st.lists(st.sampled_from(_DCC_POOL), unique=True),
    )
    def test_file_count_should_equal_the_number_of_matching_documents(
        self, mock_db, abbreviations, selected
    ):
        """Test the count equals the number of documents matching the filter.

        Given:
            An arbitrary set of files each tagged with a DCC abbreviation drawn
            from a small pool, and an arbitrary (possibly empty) subset of
            abbreviations as the filter
        When:
            The fileCount query is executed with that filter (or no filter when
            the subset is empty)
        Then:
            It should equal the number of files whose abbreviation is in the
            subset, and the total when the subset is empty
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc(f"f{i}", abbr) for i, abbr in enumerate(abbreviations)
        ]
        variables = (
            {"input": [{"dcc": [{"dccAbbreviation": selected}]}]} if selected else None
        )
        expected = (
            sum(1 for abbr in abbreviations if abbr in selected)
            if selected
            else len(abbreviations)
        )

        # Act
        result = asyncio.run(
            schema.execute(
                "query FileCount($input: [FileMetadataInput!]) {"
                " fileCount(input: $input) }",
                variable_values=variables,
            )
        )

        # Assert
        assert result.errors is None
        assert result.data["fileCount"] == expected


class TestMetadataEndpointPagination:
    """The bounds as a client sees them: over HTTP, through /metadata."""

    @pytest.fixture()
    def client(self, mocker):
        """Serve the app over an in-memory client, mirroring test_cors."""
        # The app binds the real lifespan at import, so entering the client
        # runs it — back it with mongomock and disable the workflow
        # subsystem, as the CORS suite does.
        mocker.patch.object(
            main, "create_mongodb_client", return_value=AsyncMongoMockClient()
        )
        mocker.patch.object(main.WorkflowProfile, "from_env", return_value=None)
        with TestClient(main.app) as test_client:
            yield test_client

    _QUERY = """
        query Files($page: Int!, $pageSize: Int!) {
            files(page: $page, pageSize: $pageSize) {
                totalCount
            }
        }
    """

    def test_files_should_answer_with_data_when_pagination_is_in_range(self, client):
        """Test the endpoint serves a valid paginated request.

        Given:
            The application mounted at /metadata and in-range pagination
            variables.
        When:
            The files query is POSTed to the endpoint.
        Then:
            It should answer 200 with a match count and no errors.
        """
        # Act
        response = client.post(
            "/metadata",
            json={
                "query": self._QUERY,
                "variables": {"page": 0, "pageSize": api.PAGE_SIZE},
            },
        )

        # Assert
        assert response.status_code == 200
        body = response.json()
        assert "errors" not in body
        assert body["data"]["files"]["totalCount"] == 0

    @pytest.mark.parametrize(
        ("variables", "expected"),
        [
            ({"page": -1, "pageSize": 25}, "page must be >= 0 (got -1)"),
            ({"page": 0, "pageSize": 0}, "pageSize must be between 1 and 500 (got 0)"),
            ({"page": 0, "pageSize": -1}, "pageSize must be between 1 and 500 (got -1)"),
            (
                {"page": 0, "pageSize": api.MAX_PAGE_SIZE + 1},
                f"pageSize must be between 1 and 500 (got {api.MAX_PAGE_SIZE + 1})",
            ),
        ],
    )
    def test_files_should_answer_with_a_graphql_error_when_pagination_is_out_of_range(
        self, client, variables, expected
    ):
        """Test out-of-range pagination reaches the client as a GraphQL error.

        Given:
            The application mounted at /metadata and pagination variables
            outside the accepted range.
        When:
            The files query is POSTed to the endpoint.
        Then:
            It should answer 200 with a null data field and an errors array
            carrying the message for the argument that was refused.
        """
        # Act
        response = client.post(
            "/metadata",
            json={"query": self._QUERY, "variables": variables},
        )

        # Assert
        assert response.status_code == 200
        body = response.json()
        assert body["data"] is None
        assert expected in body["errors"][0]["message"]


# The exact size, in bytes, of the ENCODE .hic file named in issue #83 —
# above the 2**31-1 ceiling GraphQL's ``Int`` scalar imposes.
_ISSUE_83_SIZE = 6262125716


class TestSizeInBytesScalar:
    """Coverage of the 64-bit ``BigInt`` scalar carrying ``sizeInBytes``."""

    @pytest.fixture(autouse=True)
    def _patch_cutover(self, mocker):
        """No-op ``locks.wait_for_cutover`` for every test in this class."""
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

    @pytest.mark.asyncio
    async def test_size_in_bytes_should_resolve_a_file_above_the_int32_ceiling(
        self, mock_db
    ):
        """Test a file larger than 2 GB reports its true size.

        Given:
            The 6,262,125,716-byte ENCODE file from issue #83, whose size
            exceeds what a 32-bit GraphQL Int can represent.
        When:
            The GraphQL files query selects sizeInBytes.
        Then:
            It should return the exact size with no errors, rather than the
            null-plus-per-field-error the Int scalar produced.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("f1", size_in_bytes=_ISSUE_83_SIZE)]

        # Act
        result = await schema.execute(
            "{ files { items { sizeInBytes } } }",
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["items"][0]["sizeInBytes"] == _ISSUE_83_SIZE

    @pytest.mark.parametrize(
        "size",
        [
            0,
            4096,
            2**31 - 1,
            2**31,
            2**53 - 1,
            2**63 - 1,
        ],
        ids=["zero", "small", "int32-max", "int32-max-plus-one", "js-safe-max", "int64-max"],
    )
    @pytest.mark.asyncio
    async def test_size_in_bytes_should_round_trip_across_the_64_bit_range(
        self, mock_db, size
    ):
        """Test sizes spanning the declared range survive serialization intact.

        Given:
            A file whose size sits at a notable point of the 64-bit range —
            zero, an ordinary size, either side of the old Int ceiling, the
            JavaScript safe-integer maximum, and the 64-bit maximum.
        When:
            The GraphQL files query selects sizeInBytes.
        Then:
            It should return that exact value with no errors.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("f1", size_in_bytes=size)]

        # Act
        result = await schema.execute("{ files { items { sizeInBytes } } }")

        # Assert
        assert result.errors is None
        assert result.data["files"]["items"][0]["sizeInBytes"] == size

    @pytest.mark.asyncio
    async def test_size_in_bytes_should_be_null_when_the_file_records_no_size(
        self, mock_db
    ):
        """Test an absent size still resolves to null rather than an error.

        Given:
            A file whose size_in_bytes is unset.
        When:
            The GraphQL files query selects sizeInBytes.
        Then:
            It should return null with no errors, as the field is optional.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("f1")]

        # Act
        result = await schema.execute("{ files { items { sizeInBytes } } }")

        # Assert
        assert result.errors is None
        assert result.data["files"]["items"][0]["sizeInBytes"] is None

    @pytest.mark.asyncio
    async def test_size_in_bytes_should_null_only_its_own_field_when_unrepresentable(
        self, mock_db
    ):
        """Test an out-of-range stored size does not take down the page.

        Given:
            Two files, the first holding a size beyond the 64-bit range and
            the second an ordinary size.
        When:
            The GraphQL files query selects sizeInBytes alongside other
            fields.
        Then:
            It should null only the offending file's sizeInBytes, report the
            failure at that field's path, and return every other field and
            the sibling file untouched.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("f1", size_in_bytes=2**70),
            _make_file_doc("f2", size_in_bytes=1234),
        ]

        # Act
        result = await schema.execute(
            "{ files { items { localId sizeInBytes } } }",
        )

        # Assert
        assert [e.path for e in result.errors] == [
            ["files", "items", 0, "sizeInBytes"]
        ]
        items = result.data["files"]["items"]
        assert items[0] == {"localId": "f1", "sizeInBytes": None}
        assert items[1] == {"localId": "f2", "sizeInBytes": 1234}

    @pytest.mark.asyncio
    async def test_files_should_filter_on_a_size_above_the_int32_ceiling(self, mock_db):
        """Test a literal size filter selects a file larger than 2 GB.

        Given:
            One file at the issue #83 size and one ordinary file.
        When:
            The GraphQL files query filters on that size as a query literal.
        Then:
            It should return only the large file, so sizes above the old Int
            ceiling are filterable and not merely readable.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("big", size_in_bytes=_ISSUE_83_SIZE),
            _make_file_doc("small", size_in_bytes=1234),
        ]

        # Act
        result = await schema.execute(
            "{ files(input: [{ sizeInBytes: [%d] }])"
            " { totalCount items { localId } } }" % _ISSUE_83_SIZE,
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 1
        assert result.data["files"]["items"][0]["localId"] == "big"

    @pytest.mark.asyncio
    async def test_files_should_filter_on_a_large_size_passed_as_a_variable(
        self, mock_db
    ):
        """Test a BigInt variable filters as a query literal does.

        Given:
            One file at the issue #83 size and one ordinary file.
        When:
            The GraphQL files query filters on that size through a
            [BigInt!] variable, which GraphQL coerces by a different path
            than a query literal.
        Then:
            It should return only the large file.
        """
        # Arrange
        mock_db.files.docs = [
            _make_file_doc("big", size_in_bytes=_ISSUE_83_SIZE),
            _make_file_doc("small", size_in_bytes=1234),
        ]

        # Act
        result = await schema.execute(
            "query Files($sizes: [BigInt!]) {"
            " files(input: [{ sizeInBytes: $sizes }])"
            " { totalCount items { localId } } }",
            variable_values={"sizes": [_ISSUE_83_SIZE]},
        )

        # Assert
        assert result.errors is None
        assert result.data["files"]["totalCount"] == 1
        assert result.data["files"]["items"][0]["localId"] == "big"

    @pytest.mark.asyncio
    async def test_files_should_reject_an_int_typed_variable_for_the_size_filter(
        self, mock_db
    ):
        """Test the documented break for clients still declaring Int.

        Given:
            A files query declaring its size-filter variable as [Int!], as a
            client written against the pre-BigInt schema would.
        When:
            The query is executed.
        Then:
            It should fail validation naming the expected [BigInt!] type,
            rather than silently truncating at the 32-bit ceiling.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("big", size_in_bytes=_ISSUE_83_SIZE)]

        # Act
        result = await schema.execute(
            "query Files($sizes: [Int!]) {"
            " files(input: [{ sizeInBytes: $sizes }]) { totalCount } }",
            variable_values={"sizes": [1234]},
        )

        # Assert
        assert result.data is None
        assert "expecting type '[BigInt!]'" in result.errors[0].message

    @pytest.mark.parametrize(
        "literal",
        ["true", str(2**63), str(-(2**63) - 1)],
        ids=["boolean", "above-int64-max", "below-int64-min"],
    )
    @pytest.mark.asyncio
    async def test_files_should_reject_a_size_filter_literal_outside_the_scalar(
        self, mock_db, literal
    ):
        """Test the scalar refuses literals it cannot represent.

        Given:
            A size filter literal that is a boolean, or an integer one step
            beyond either end of the 64-bit range.
        When:
            The GraphQL files query is executed with that literal.
        Then:
            It should reject the query outright with a BigInt error rather
            than coercing the value.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("f1", size_in_bytes=1234)]

        # Act
        result = await schema.execute(
            f"{{ files(input: [{{ sizeInBytes: [{literal}] }}]) {{ totalCount }} }}",
        )

        # Assert
        assert result.data is None
        assert "BigInt cannot represent" in result.errors[0].message

    @pytest.mark.parametrize(
        "value", ["6262125716", 1.5], ids=["string", "non-integral-float"]
    )
    @pytest.mark.asyncio
    async def test_files_should_reject_a_non_integer_size_filter_variable(
        self, mock_db, value
    ):
        """Test the scalar refuses non-integer variable values.

        Given:
            A [BigInt!] variable carrying a numeric string or a fractional
            number — the shapes a client that hedged against the 32-bit
            ceiling by stringifying would send.
        When:
            The GraphQL files query is executed with that variable.
        Then:
            It should reject the query with a BigInt error, so the wire form
            stays unambiguously a JSON integer.
        """
        # Arrange
        mock_db.files.docs = [_make_file_doc("f1", size_in_bytes=1234)]

        # Act
        result = await schema.execute(
            "query Files($sizes: [BigInt!]) {"
            " files(input: [{ sizeInBytes: $sizes }]) { totalCount } }",
            variable_values={"sizes": [value]},
        )

        # Assert
        assert result.data is None
        assert "BigInt cannot represent" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_schema_should_type_size_in_bytes_as_big_int_on_both_sides(self):
        """Test the widening reached the input filter as well as the output.

        Given:
            The published GraphQL schema.
        When:
            FileMetadataType and FileMetadataInput are introspected.
        Then:
            Both should name sizeInBytes as BigInt, since widening only the
            output would leave the files it exposes unfilterable by size.
        """
        # Act
        result = await schema.execute(
            """
            {
                output: __type(name: "FileMetadataType") {
                    fields { name type { ...Ref } }
                }
                input: __type(name: "FileMetadataInput") {
                    inputFields { name type { ...Ref } }
                }
            }
            fragment Ref on __Type {
                name ofType { name ofType { name ofType { name } } }
            }
            """
        )

        # Assert
        assert result.errors is None
        output = {f["name"]: f["type"] for f in result.data["output"]["fields"]}
        inputs = {f["name"]: f["type"] for f in result.data["input"]["inputFields"]}
        assert _named_type(output["sizeInBytes"]) == "BigInt"
        assert _named_type(inputs["sizeInBytes"]) == "BigInt"

    @pytest.mark.asyncio
    async def test_schema_should_leave_neighbouring_integer_fields_as_int(self):
        """Test the widening did not spread to unrelated integer fields.

        Given:
            The published GraphQL schema, in which ExtraFileType.fileSize is
            another size-shaped int and totalCount, fileCount and the
            pagination arguments are counts.
        When:
            Those fields and arguments are introspected.
        Then:
            Each should still be Int, since the override is scoped to one
            model field rather than to every int in the schema.
        """
        # Act
        result = await schema.execute(
            """
            {
                extraFile: __type(name: "ExtraFileType") {
                    fields { name type { ...Ref } }
                }
                fileList: __type(name: "FileList") {
                    fields { name type { ...Ref } }
                }
                query: __type(name: "Query") {
                    fields { name type { ...Ref } args { name type { ...Ref } } }
                }
            }
            fragment Ref on __Type {
                name ofType { name ofType { name ofType { name } } }
            }
            """
        )

        # Assert
        assert result.errors is None
        extra_file = {f["name"]: f["type"] for f in result.data["extraFile"]["fields"]}
        file_list = {f["name"]: f["type"] for f in result.data["fileList"]["fields"]}
        query = {f["name"]: f for f in result.data["query"]["fields"]}
        files_args = {a["name"]: a["type"] for a in query["files"]["args"]}
        assert _named_type(extra_file["fileSize"]) == "Int"
        assert _named_type(file_list["totalCount"]) == "Int"
        assert _named_type(query["fileCount"]["type"]) == "Int"
        assert _named_type(files_args["page"]) == "Int"
        assert _named_type(files_args["pageSize"]) == "Int"


def test_checked_in_sdl_should_match_the_generated_schema():
    """Test schema.graphql has not drifted from the Strawberry schema.

    Given:
        The checked-in schema.graphql, which is a generated artifact and the
        contract clients codegen against.
    When:
        The SDL is rendered from the live schema.
    Then:
        The two should be byte-identical, so a type change that skipped
        regeneration cannot ship a stale public contract.
    """
    # Arrange
    sdl_path = Path(__file__).resolve().parent.parent / "schema.graphql"

    # Act
    generated = print_schema(schema) + "\n"

    # Assert
    assert sdl_path.read_text() == generated, (
        "schema.graphql is stale — run `make schema` to regenerate it."
    )
