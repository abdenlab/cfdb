"""Tests for the ``cfdb.ensure_indexes`` operator CLI."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

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
