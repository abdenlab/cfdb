"""Tests for simplified GraphQL files query."""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

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


def _make_file_doc(local_id: str, submission: str = "hubmap") -> dict:
    """Return a minimal file document that satisfies FileMetadataModel."""
    return {
        "id_namespace": "ns",
        "local_id": local_id,
        "project_id_namespace": "ns",
        "project_local_id": "proj",
        "filename": f"{local_id}.bam",
        "submission": submission,
        "data_access_level": "public",
        "dcc": {
            "dcc_name": submission.upper(),
            "dcc_abbreviation": submission,
        },
        "collections": [],
    }


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
