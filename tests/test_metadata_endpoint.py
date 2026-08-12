"""HTTP-boundary tests for the /metadata GraphQL endpoint."""

from __future__ import annotations

import asyncio
import re

import pytest
from mongomock_motor import AsyncMongoMockClient
from starlette.testclient import TestClient

from cfdb import api
from tests.conftest import ISSUE_83_SIZE


@pytest.fixture()
def client(mocker):
    # Mirrors the fixture in test_cors.py: the app binds the real lifespan at
    # import, and the lifespan ensures the operational indexes, so back it
    # with an in-memory mongomock client and disable the workflow subsystem.
    from cfdb.api import main

    mocker.patch.object(
        main, "create_mongodb_client", return_value=AsyncMongoMockClient()
    )
    mocker.patch.object(main.WorkflowProfile, "from_env", return_value=None)
    with TestClient(main.app) as c:
        yield c


@pytest.fixture()
def client_with_large_file(client):
    # Insert through the same database handle the resolvers read, so the size
    # round-trips a real BSON encode/decode rather than the in-memory
    # FakeCollection double, which compares Python values directly. The
    # mongomock-motor wrapper is async over a synchronous store, so a
    # throwaway loop is enough to drive the insert from this sync fixture.
    asyncio.run(
        api.db.files.insert_one(
            {
                "id_namespace": "ns",
                "local_id": "ENCFF502HMX",
                "project_id_namespace": "ns",
                "project_local_id": "proj",
                "filename": "ENCFF502HMX.hic",
                "submission": "encode",
                "data_access_level": "public",
                "size_in_bytes": ISSUE_83_SIZE,
                "dcc": {"dcc_name": "ENCODE", "dcc_abbreviation": "encode"},
                "collections": [],
            }
        )
    )
    return client


@pytest.fixture()
def client_with_accession_file(client):
    # Inserted through the same handle the resolvers read, so the accession
    # filter runs against mongomock's real matcher rather than the
    # FakeCollection double -- which matters for the nested path, since that
    # double resolves dotted paths with dict lookups and cannot traverse the
    # collections array at all.
    asyncio.run(
        api.db.files.insert_one(
            {
                "id_namespace": "ns",
                "local_id": "51108ad5-2345-474c-a99b-0a64456b37bc",
                "project_id_namespace": "ns",
                "project_local_id": "proj",
                "filename": "4DNFIMCJXZKH.fastq.gz",
                "submission": "4dn",
                "data_access_level": "public",
                "accession_id": "4DNFIMCJXZKH",
                "dcc": {"dcc_name": "4DN", "dcc_abbreviation": "4DN_DCIC"},
                "collections": [
                    {
                        "id_namespace": "ns",
                        "local_id": "3d54d990-b73c-44a6-99b2-9054692004d6",
                        "name": "in situ Hi-C Experiment 4DNEXNHE6X77",
                        "accession_id": "4DNEXNHE6X77",
                        "biosamples": [],
                    }
                ],
            }
        )
    )
    return client


def test_metadata_should_match_a_lower_case_accession_over_http(
    client_with_accession_file,
):
    """Test the accession round trip across the whole real stack.

    The unit tests fold on each side against an in-memory double; this
    drives the same invariant through BSON, mongomock's matcher and JSON
    serialization, which is where a stored form and a queried form would
    actually have to meet in production.

    Given:
        A 4DN file stored with the folded accession the sync writes.
    When:
        A lower-case accessionId filter is POSTed to /metadata.
    Then:
        It should return that file with the accession echoed in its stored
        upper-case form.
    """
    # Act
    response = client_with_accession_file.post(
        "/metadata",
        json={
            "query": (
                '{ files(input: [{ accessionId: ["4dnfimcjxzkh"] }])'
                " { totalCount items { filename accessionId } } }"
            )
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["files"]["totalCount"] == 1
    item = body["data"]["files"]["items"][0]
    assert item["filename"] == "4DNFIMCJXZKH.fastq.gz"
    assert item["accessionId"] == "4DNFIMCJXZKH"


def test_metadata_should_match_a_lower_case_collection_accession_over_http(
    client_with_accession_file,
):
    """Test the nested accession filter against real array traversal.

    The collection accession is reached through a dotted path into an
    array, so this depends on Mongo's implicit array traversal in a way
    the file-level filter does not -- and is the riskier half of the
    contract for that reason.

    Given:
        The same file, whose nested collection carries an experiment
        accession.
    When:
        A lower-case collections.accessionId filter is POSTed to /metadata.
    Then:
        It should return that file with the collection accession echoed in
        its stored form.
    """
    # Act
    response = client_with_accession_file.post(
        "/metadata",
        json={
            "query": (
                "{ files(input: [{ collections: "
                '[{ accessionId: ["4dnexnhe6x77"] }] }])'
                " { totalCount items { collections { accessionId } } } }"
            )
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["files"]["totalCount"] == 1
    collections = body["data"]["files"]["items"][0]["collections"]
    assert collections[0]["accessionId"] == "4DNEXNHE6X77"


def test_metadata_should_serve_a_filter_whose_only_accession_is_blank(
    client_with_accession_file,
):
    """Test that a blank accession filter neither errors nor widens.

    Dropping the clause empties the enclosing $or, and MongoDB rejects
    ``{"$or": []}`` with BadValue -- so this has to reach the real matcher
    to mean anything. The in-memory double cannot detect it: its matcher
    reduces an empty $and to ``all([])``, which is True.

    Given:
        A stored 4DN file and a filter whose only accession is whitespace.
    When:
        The query is POSTed to /metadata.
    Then:
        It should return the file with no errors, treating the blank value
        as no constraint rather than as a null match or a server error.
    """
    # Act
    response = client_with_accession_file.post(
        "/metadata",
        json={
            "query": (
                '{ files(input: [{ accessionId: ["   "] }])'
                " { totalCount } }"
            )
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["files"]["totalCount"] == 1


def test_metadata_should_serve_a_filter_with_no_fields_set(
    client_with_accession_file,
):
    """Test the issue #103 reproduction reaches the database.

    ``files(input: [{}])`` is a legal GraphQL document that flattened to
    ``{"$and": []}``, which MongoDB and DocumentDB both reject outright, so
    the query 500ed rather than returning rows.

    Given:
        A stored file and a filter object with no fields set.
    When:
        The reported reproduction is POSTed to /metadata.
    Then:
        It should return the file with no errors.
    """
    # Act
    response = client_with_accession_file.post(
        "/metadata",
        json={"query": "{ files(input: [{}]) { totalCount } }"},
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["files"]["totalCount"] == 1


def test_metadata_should_serve_a_multi_gigabyte_size_as_a_json_number(
    client_with_large_file,
):
    """Test the issue #83 reproduction returns a number over real HTTP.

    Given:
        The 6,262,125,716-byte ENCODE file from issue #83 stored in the
        database.
    When:
        The reported reproduction query is POSTed to /metadata, selecting
        sizeInBytes for that file.
    Then:
        The response body should carry the exact size as an unquoted JSON
        number with no errors — the value a browser client can use directly,
        rather than the null-plus-error the Int scalar produced.
    """
    # Act
    response = client_with_large_file.post(
        "/metadata",
        json={
            "query": (
                "{ files(input: [{ localId: [\"ENCFF502HMX\"] }])"
                " { items { filename sizeInBytes } } }"
            )
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["files"]["items"][0]["sizeInBytes"] == ISSUE_83_SIZE
    # Assert on the raw bytes too: json.loads would silently accept a quoted
    # value, and whether the client receives a number or a string is the
    # decision this scalar makes.
    assert re.search(rf'"sizeInBytes":\s*{ISSUE_83_SIZE}\b', response.text)


def test_metadata_should_filter_on_a_multi_gigabyte_size_over_http(
    client_with_large_file,
):
    """Test a size above the Int ceiling round-trips into the Mongo query.

    Given:
        The same file stored in the database.
    When:
        A /metadata query filters on its size through a [BigInt!] variable.
    Then:
        It should match that file, confirming the widened value survives
        BSON encoding rather than only the GraphQL layer.
    """
    # Act
    response = client_with_large_file.post(
        "/metadata",
        json={
            "query": (
                "query Files($sizes: [BigInt!]) {"
                " files(input: [{ sizeInBytes: $sizes }])"
                " { totalCount items { localId } } }"
            ),
            "variables": {"sizes": [ISSUE_83_SIZE]},
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["files"]["totalCount"] == 1
    assert body["data"]["files"]["items"][0]["localId"] == "ENCFF502HMX"
