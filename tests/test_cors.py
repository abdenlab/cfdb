"""CORS middleware tests for the FastAPI application."""

from unittest.mock import patch

import pytest
from mongomock_motor import AsyncMongoMockClient
from starlette.testclient import TestClient


@pytest.fixture()
def client():
    # The app binds the real lifespan at import, so the lifespan runs on
    # TestClient enter — and it ensures the operational indexes, which
    # needs a database. Back it with an in-memory mongomock client and
    # disable the workflow subsystem so the CORS test exercises only the
    # middleware, no real MongoDB or wool pool.
    from cfdb.api import main

    with (
        patch.object(main, "create_mongodb_client", return_value=AsyncMongoMockClient()),
        patch.object(main.WorkflowProfile, "from_env", return_value=None),
    ):
        with TestClient(main.app) as c:
            yield c


class TestCORS:
    """CO-001 / CO-002 — CORS middleware allows cross-origin requests."""

    def test_preflight_succeeds(self, client):
        """
        GIVEN an OPTIONS preflight request to /metadata with Origin and
              Access-Control-Request-Method headers,
        WHEN the request is sent,
        THEN the response is 200 with Access-Control-Allow-Origin: *.
        """
        response = client.options(
            "/metadata",
            headers={
                "Origin": "http://example.com",
                "Access-Control-Request-Method": "POST",
            },
        )
        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == "*"

    def test_simple_cors_request(self, client):
        """
        GIVEN a POST request to /metadata with an Origin header and a
              minimal GraphQL body,
        WHEN the request is sent,
        THEN the response includes Access-Control-Allow-Origin: *.
        """
        response = client.post(
            "/metadata",
            json={"query": "{ __typename }"},
            headers={"Origin": "http://example.com"},
        )
        assert response.headers["access-control-allow-origin"] == "*"
