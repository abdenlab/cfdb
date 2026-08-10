"""HTTP-boundary tests for the /metadata GraphQL endpoint."""

from __future__ import annotations

import asyncio
import re
from unittest.mock import patch

import pytest
from mongomock_motor import AsyncMongoMockClient
from starlette.testclient import TestClient

from cfdb import api


# The ENCODE .hic file named in issue #83, whose size exceeds the 2**31-1
# ceiling a 32-bit GraphQL Int imposes.
_ISSUE_83_SIZE = 6262125716


@pytest.fixture()
def client():
    # Mirrors the fixture in test_cors.py: the app binds the real lifespan at
    # import, and the lifespan ensures the operational indexes, so back it
    # with an in-memory mongomock client and disable the workflow subsystem.
    from cfdb.api import main

    with (
        patch.object(main, "create_mongodb_client", return_value=AsyncMongoMockClient()),
        patch.object(main.WorkflowProfile, "from_env", return_value=None),
    ):
        with TestClient(main.app) as c:
            yield c


@pytest.fixture()
def large_file(client):
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
                "size_in_bytes": _ISSUE_83_SIZE,
                "dcc": {"dcc_name": "ENCODE", "dcc_abbreviation": "encode"},
                "collections": [],
            }
        )
    )
    return client


def test_metadata_should_serve_a_multi_gigabyte_size_as_a_json_number(large_file):
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
    response = large_file.post(
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
    assert body["data"]["files"]["items"][0]["sizeInBytes"] == _ISSUE_83_SIZE
    # Assert on the raw bytes too: json.loads would silently accept a quoted
    # value, and whether the client receives a number or a string is the
    # decision this scalar makes.
    assert re.search(rf'"sizeInBytes":\s*{_ISSUE_83_SIZE}\b', response.text)


def test_metadata_should_filter_on_a_multi_gigabyte_size_over_http(large_file):
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
    response = large_file.post(
        "/metadata",
        json={
            "query": (
                "query Files($sizes: [BigInt!]) {"
                " files(input: [{ sizeInBytes: $sizes }])"
                " { totalCount items { localId } } }"
            ),
            "variables": {"sizes": [_ISSUE_83_SIZE]},
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["files"]["totalCount"] == 1
    assert body["data"]["files"]["items"][0]["localId"] == "ENCFF502HMX"
