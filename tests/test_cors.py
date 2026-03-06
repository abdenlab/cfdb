"""CORS middleware tests for the FastAPI application."""

from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient


@asynccontextmanager
async def _noop_lifespan(_app):
    yield


@pytest.fixture()
def client():
    with patch("cfdb.api.main.lifespan", _noop_lifespan):
        from cfdb.api.main import app

        with TestClient(app) as c:
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
