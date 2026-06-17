"""Tests for the ``cfdb.ensure_indexes`` operator CLI."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from cfdb import api as cfg
from cfdb import ensure_indexes as ei
from cfdb.indexes import all_index_specs, data_index_specs, operational_index_specs


@pytest.mark.parametrize(
    "scope, builder",
    [
        ("operational", operational_index_specs),
        ("data", data_index_specs),
        ("all", all_index_specs),
    ],
)
def test_main_should_ensure_the_selected_scope(mocker, scope, builder):
    """Test the CLI ensures the index set named by ``--scope``.

    Given:
        The CLI with the Mongo client and applier mocked at the boundary.
    When:
        ``main`` is invoked with a ``--scope`` value.
    Then:
        It should pass that scope's specs to ensure_indexes and exit 0.
    """
    # Arrange
    ensure_mock = mocker.patch.object(
        ei, "ensure_indexes", mocker.AsyncMock(return_value=len(builder()))
    )
    mocker.patch.object(ei, "AsyncIOMotorClient")
    runner = CliRunner()

    # Act
    result = runner.invoke(ei.main, ["--scope", scope])

    # Assert
    assert result.exit_code == 0
    passed_specs = ensure_mock.await_args.args[1]
    assert [s.name for s in passed_specs] == [s.name for s in builder()]


def test_main_should_default_to_all_scope(mocker):
    """Test the CLI ensures every index when no scope is given.

    Given:
        The CLI invoked with no ``--scope`` flag.
    When:
        ``main`` runs.
    Then:
        It should ensure the combined operational + data spec set.
    """
    # Arrange
    ensure_mock = mocker.patch.object(
        ei, "ensure_indexes", mocker.AsyncMock(return_value=1)
    )
    mocker.patch.object(ei, "AsyncIOMotorClient")
    runner = CliRunner()

    # Act
    result = runner.invoke(ei.main, [])

    # Assert
    assert result.exit_code == 0
    passed_specs = ensure_mock.await_args.args[1]
    assert [s.name for s in passed_specs] == [s.name for s in all_index_specs()]


@pytest.mark.asyncio
async def test_run_should_build_a_tls_client_when_tls_enabled(mocker):
    """Test run() honors the MongoDB TLS env config.

    Given:
        ``MONGODB_TLS_ENABLED`` set with a CA path and the applier mocked.
    When:
        ``run`` is awaited.
    Then:
        It should construct the Motor client with TLS + CA file, return
        the ensured count, and close the client.
    """
    # Arrange
    mocker.patch.object(cfg, "MONGODB_TLS_ENABLED", True)
    mocker.patch.object(cfg, "MONGODB_CA_PATH", "/ca.pem")
    mocker.patch.object(cfg, "MONGODB_RETRY_WRITES", False)
    client = mocker.MagicMock()
    factory = mocker.patch.object(ei, "AsyncIOMotorClient", return_value=client)
    mocker.patch.object(ei, "ensure_indexes", mocker.AsyncMock(return_value=3))

    # Act
    count = await ei.run("operational")

    # Assert
    assert count == 3
    _, kwargs = factory.call_args
    assert kwargs.get("tls") is True
    assert kwargs.get("tlsCAFile") == "/ca.pem"
    assert kwargs.get("retryWrites") is False
    client.close.assert_called_once()


@pytest.mark.asyncio
async def test_run_should_build_a_plain_client_when_tls_disabled(mocker):
    """Test run() builds a plaintext client when TLS is off.

    Given:
        ``MONGODB_TLS_ENABLED`` false and the applier mocked.
    When:
        ``run`` is awaited.
    Then:
        It should construct the client without TLS options.
    """
    # Arrange
    mocker.patch.object(cfg, "MONGODB_TLS_ENABLED", False)
    mocker.patch.object(cfg, "MONGODB_RETRY_WRITES", False)
    client = mocker.MagicMock()
    factory = mocker.patch.object(ei, "AsyncIOMotorClient", return_value=client)
    mocker.patch.object(ei, "ensure_indexes", mocker.AsyncMock(return_value=0))

    # Act
    await ei.run("all")

    # Assert
    _, kwargs = factory.call_args
    assert "tls" not in kwargs
    assert kwargs.get("retryWrites") is False


def test_main_should_reject_unknown_scope(mocker):
    """Test an invalid scope is rejected before any DB work.

    Given:
        The CLI invoked with a scope outside the allowed choices.
    When:
        ``main`` runs.
    Then:
        It should exit non-zero and never build a client.
    """
    # Arrange
    client_factory = mocker.patch.object(ei, "AsyncIOMotorClient")
    runner = CliRunner()

    # Act
    result = runner.invoke(ei.main, ["--scope", "bogus"])

    # Assert
    assert result.exit_code != 0
    client_factory.assert_not_called()


def test_main_should_close_the_client(mocker):
    """Test the Mongo client is closed after ensuring indexes.

    Given:
        The CLI with the applier mocked and a stand-in Mongo client.
    When:
        ``main`` completes successfully.
    Then:
        It should close the client it opened.
    """
    # Arrange
    mocker.patch.object(ei, "ensure_indexes", mocker.AsyncMock(return_value=0))
    client = mocker.MagicMock()
    mocker.patch.object(ei, "AsyncIOMotorClient", return_value=client)
    runner = CliRunner()

    # Act
    result = runner.invoke(ei.main, ["--scope", "operational"])

    # Assert
    assert result.exit_code == 0
    client.close.assert_called_once()
