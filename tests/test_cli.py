"""Tests for the cfdb CLI commands."""

from __future__ import annotations

import duckdb
from click.testing import CliRunner

import cfdb.cli as cli_module
from tests.test_dump import make_fourdn_doc


def patch_mongo(mocker, docs: list[dict]):
    """Patch the CLI's MongoClient with a client whose find() yields docs."""
    fake_collection = mocker.MagicMock()
    fake_collection.find.return_value = docs
    fake_client = mocker.MagicMock()
    fake_client.__getitem__.return_value = {"files": fake_collection}
    mocker.patch.object(cli_module, "MongoClient", return_value=fake_client)
    return fake_collection


def test_dump_duckdb_should_fail_when_duckdb_missing(mocker, tmp_path):
    """Test the CLI's missing-extra diagnostic.

    Given:
        An environment where the duckdb module is not importable.
    When:
        cfdb dump duckdb is invoked.
    Then:
        It should exit nonzero and name the cfdb[duckdb] extra.
    """
    # Arrange
    mocker.patch.dict("sys.modules", {"duckdb": None})
    runner = CliRunner()

    # Act
    result = runner.invoke(cli_module.cli, ["dump", "duckdb", str(tmp_path / "out.duckdb")])

    # Assert
    assert result.exit_code == 1
    assert "cfdb[duckdb]" in result.output


def test_dump_duckdb_should_write_database_when_mongo_returns_documents(mocker, tmp_path):
    """Test the CLI wiring from Mongo cursor to DuckDB file.

    Given:
        A MongoClient whose files collection yields one document.
    When:
        cfdb dump duckdb is invoked with a submission filter.
    Then:
        It should query Mongo with that filter and write a DuckDB file
        holding the document.
    """
    # Arrange
    fake_collection = patch_mongo(mocker, [make_fourdn_doc()])
    output = tmp_path / "out.duckdb"
    runner = CliRunner()

    # Act
    result = runner.invoke(
        cli_module.cli, ["dump", "duckdb", str(output), "-s", "4DN"]
    )

    # Assert
    assert result.exit_code == 0, result.output
    fake_collection.find.assert_called_once_with(
        {"submission": {"$in": ["4dn"]}}, batch_size=1000
    )
    con = duckdb.connect(str(output))
    assert con.execute("SELECT count(*) FROM file").fetchone()[0] == 1
    con.close()


def test_dump_duckdb_should_refuse_existing_output_without_force(tmp_path):
    """Test the CLI's overwrite guard.

    Given:
        An OUTPUT path that already exists.
    When:
        cfdb dump duckdb is invoked without --force.
    Then:
        It should exit nonzero without touching the file.
    """
    # Arrange
    output = tmp_path / "out.duckdb"
    output.write_bytes(b"not a database")
    runner = CliRunner()

    # Act
    result = runner.invoke(cli_module.cli, ["dump", "duckdb", str(output)])

    # Assert
    assert result.exit_code == 1
    assert "--force" in result.output
    assert output.read_bytes() == b"not a database"


def test_dump_duckdb_should_overwrite_existing_output_when_forced(mocker, tmp_path):
    """Test the --force overwrite path.

    Given:
        An OUTPUT path that already exists and a Mongo yielding one
        document.
    When:
        cfdb dump duckdb is invoked with --force.
    Then:
        It should replace the file with a valid DuckDB holding the
        document.
    """
    # Arrange
    patch_mongo(mocker, [make_fourdn_doc()])
    output = tmp_path / "out.duckdb"
    output.write_bytes(b"not a database")
    runner = CliRunner()

    # Act
    result = runner.invoke(cli_module.cli, ["dump", "duckdb", str(output), "--force"])

    # Assert
    assert result.exit_code == 0, result.output
    con = duckdb.connect(str(output))
    assert con.execute("SELECT count(*) FROM file").fetchone()[0] == 1
    con.close()


def test_dump_duckdb_should_export_parquet_when_requested(mocker, tmp_path):
    """Test the --to-parquet export path.

    Given:
        A Mongo yielding one document and a --to-parquet directory.
    When:
        cfdb dump duckdb is invoked.
    Then:
        It should name the export in its output and leave Parquet files in
        the directory.
    """
    # Arrange
    patch_mongo(mocker, [make_fourdn_doc()])
    output = tmp_path / "out.duckdb"
    parquet_dir = tmp_path / "parquet"
    runner = CliRunner()

    # Act
    result = runner.invoke(
        cli_module.cli,
        ["dump", "duckdb", str(output), "--to-parquet", str(parquet_dir)],
    )

    # Assert
    assert result.exit_code == 0, result.output
    assert "Exported Parquet" in result.output
    assert list(parquet_dir.glob("*.parquet"))


def test_dump_duckdb_should_keep_database_when_parquet_export_fails(mocker, tmp_path):
    """Test that a failed export never destroys the completed dump.

    Given:
        A Mongo yielding one document and a --to-parquet target inside an
        unwritable directory.
    When:
        cfdb dump duckdb is invoked.
    Then:
        It should exit nonzero naming the export failure while the
        completed .duckdb stays on disk.
    """
    # Arrange
    patch_mongo(mocker, [make_fourdn_doc()])
    output = tmp_path / "out.duckdb"
    readonly_parent = tmp_path / "readonly"
    readonly_parent.mkdir()
    readonly_parent.chmod(0o500)
    parquet_target = readonly_parent / "parquet"
    runner = CliRunner()

    # Act
    try:
        result = runner.invoke(
            cli_module.cli,
            ["dump", "duckdb", str(output), "--to-parquet", str(parquet_target)],
        )
    finally:
        readonly_parent.chmod(0o700)

    # Assert
    assert result.exit_code != 0
    assert "Parquet export" in result.output
    assert "was kept" in result.output
    assert output.exists()


def test_dump_duckdb_should_remove_partial_output_when_dump_fails(mocker, tmp_path):
    """Test partial-output cleanup on a failed dump.

    Given:
        A Mongo yielding two documents with duplicate file keys.
    When:
        cfdb dump duckdb is invoked.
    Then:
        It should exit nonzero, print the DumpError message, and remove the
        partial output file.
    """
    # Arrange
    patch_mongo(mocker, [make_fourdn_doc("file-1"), make_fourdn_doc("file-1")])
    output = tmp_path / "out.duckdb"
    runner = CliRunner()

    # Act
    result = runner.invoke(cli_module.cli, ["dump", "duckdb", str(output)])

    # Assert
    assert result.exit_code == 1
    assert "duplicate file keys" in result.output
    assert not output.exists()
