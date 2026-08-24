"""Tests for the ``cfdb`` operator CLI."""

from __future__ import annotations

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

from cfdb.cli import cli
from cfdb.workflows import purge as purge_module
from cfdb.workflows.keys import cache_key
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.purge import PurgeReport
from tests.test_workflows import FIXTURE_MD5

_BUCKET = "cfdb-test-cli"

#: A key of the retired four-segment shape the sweep reclaims.
_LEGACY_KEY = f"encode/ENCFF732YBO/index/{FIXTURE_MD5}-v2"

#: A current-scheme key, which every sweep must leave alone.
_CURRENT_KEY = cache_key(
    dcc="encode",
    local_id="ENCFF732YBO",
    artifact_kind=ArtifactKind.INDEX,
    md5=FIXTURE_MD5,
    processor_id="tabix-interval",
    processor_version=2,
)

#: Every option on ``purge-legacy-cache`` is bound to one of these. They
#: are cleared for each test because an exported value on the developer's
#: machine would otherwise silently change which store is targeted — and
#: the ambiguity guard would fire on tests that never mention S3.
_BOUND_ENV_VARS = (
    "WORKFLOW_S3_BUCKET",
    "WORKFLOW_S3_PREFIX",
    "AWS_ENDPOINT_URL",
    "AWS_REGION",
    "SYNC_DATA_DIR",
)


@pytest.fixture(autouse=True)
def clear_bound_env(monkeypatch):
    """Remove every environment variable the purge options bind to."""
    for name in _BOUND_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture()
def cache_root(tmp_path):
    """Return a local cache root holding one legacy and one current entry."""
    for key in (_LEGACY_KEY, _CURRENT_KEY):
        path = tmp_path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"artifact")
    return tmp_path


def _invoke(*args, input: str | None = None):
    """Run ``cfdb purge-legacy-cache`` with ``args`` and return the result.

    Tests exercising a sweep pass ``--yes`` to skip the ``--apply``
    confirmation; the prompt itself is covered by its own tests.
    """
    return CliRunner().invoke(cli, ["purge-legacy-cache", *args], input=input)


class TestPurgeLegacyCacheCommand:
    def test_purge_legacy_cache_should_default_to_a_dry_run(self, cache_root):
        """Test that omitting --apply destroys nothing.

        Given:
            A local cache root holding one legacy and one current entry.
        When:
            The command is invoked with --local-root and no --apply.
        Then:
            It should exit 0, report the target and the one match, print
            the dry-run notice, and leave both files on disk — an
            operator cannot destroy a cache by forgetting a flag.
        """
        # Act
        result = _invoke("--local-root", str(cache_root))

        # Assert
        assert result.exit_code == 0
        assert f"Target: {cache_root}" in result.output
        assert "Scanned: 2" in result.output
        assert "Legacy entries: 1" in result.output
        assert "Dry run" in result.output
        assert (cache_root / _LEGACY_KEY).exists()
        assert (cache_root / _CURRENT_KEY).exists()

    def test_purge_legacy_cache_should_delete_the_legacy_entry_when_applied(
        self, cache_root
    ):
        """Test that --apply sweeps the local cache.

        Given:
            The same local cache root.
        When:
            The command is invoked with --local-root and --apply.
        Then:
            It should exit 0, report the deletion, and remove only the
            legacy entry.
        """
        # Act
        result = _invoke("--local-root", str(cache_root), "--apply", "--yes")

        # Assert
        assert result.exit_code == 0
        assert "Deleted: 1" in result.output
        assert "Dry run" not in result.output
        assert not (cache_root / _LEGACY_KEY).exists()
        assert (cache_root / _CURRENT_KEY).exists()

    def test_purge_legacy_cache_should_refuse_when_both_stores_resolve(
        self, cache_root, monkeypatch, mocker
    ):
        """Test that an ambiguous target purges neither store.

        Given:
            WORKFLOW_S3_BUCKET set in the environment alongside an
            explicit --local-root.
        When:
            The command is invoked with --apply.
        Then:
            It should exit non-zero with a usage error and call neither
            sweep. Purging the wrong store is unrecoverable, so the
            command must refuse rather than guess which was meant.
        """
        # Arrange
        monkeypatch.setenv("WORKFLOW_S3_BUCKET", "some-bucket")
        local = mocker.patch.object(purge_module, "purge_local")
        remote = mocker.patch.object(purge_module, "purge_s3")

        # Act
        result = _invoke("--local-root", str(cache_root), "--apply", "--yes")

        # Assert
        assert result.exit_code != 0
        assert "Both an S3 bucket and a local cache root" in result.output
        local.assert_not_called()
        remote.assert_not_called()

    def test_purge_legacy_cache_should_refuse_when_no_store_resolves(self, mocker):
        """Test that the command names the ways to supply a target.

        Given:
            Neither --s3-bucket, --local-root, WORKFLOW_S3_BUCKET, nor
            SYNC_DATA_DIR.
        When:
            The command is invoked.
        Then:
            It should exit non-zero with a usage error listing all four,
            rather than silently sweeping nothing and reporting success.
        """
        # Arrange
        local = mocker.patch.object(purge_module, "purge_local")

        # Act
        result = _invoke()

        # Assert
        assert result.exit_code != 0
        assert "No cache to purge" in result.output
        local.assert_not_called()

    def test_purge_legacy_cache_should_fall_back_to_the_sync_data_dir(
        self, monkeypatch, tmp_path
    ):
        """Test that SYNC_DATA_DIR resolves the documented default root.

        Given:
            SYNC_DATA_DIR pointing at a directory whose cache/ subtree
            holds a legacy entry, and no explicit target.
        When:
            The command is invoked with --apply.
        Then:
            It should sweep $SYNC_DATA_DIR/cache and name that path as
            the target.
        """
        # Arrange
        data_dir = tmp_path / "data"
        (data_dir / "cache").mkdir(parents=True)
        entry = data_dir / "cache" / _LEGACY_KEY
        entry.parent.mkdir(parents=True)
        entry.write_bytes(b"stale")
        monkeypatch.setenv("SYNC_DATA_DIR", str(data_dir))

        # Act
        result = _invoke("--apply", "--yes")

        # Assert
        assert result.exit_code == 0
        assert f"Target: {data_dir / 'cache'}" in result.output
        assert not entry.exists()

    def test_purge_legacy_cache_should_prefer_an_explicit_root_over_the_fallback(
        self, cache_root, monkeypatch, tmp_path
    ):
        """Test that an explicit root wins over the environment default.

        Given:
            SYNC_DATA_DIR set to one directory and --local-root passed
            for another.
        When:
            The command is invoked.
        Then:
            It should target the explicit root, so the flag an operator
            typed beats the one their shell supplied.
        """
        # Arrange
        monkeypatch.setenv("SYNC_DATA_DIR", str(tmp_path / "elsewhere"))

        # Act
        result = _invoke("--local-root", str(cache_root))

        # Assert
        assert result.exit_code == 0
        assert f"Target: {cache_root}" in result.output

    def test_purge_legacy_cache_should_build_no_s3_client_for_a_local_sweep(
        self, cache_root, mocker
    ):
        """Test that the local branch never reaches for AWS.

        Given:
            A local cache root, with the sweep entry points patched.
        When:
            The command is invoked with --local-root.
        Then:
            It should call purge_local and neither purge_s3 nor the
            client factory — constructing a boto3 client in an
            environment with no AWS configuration is a latent failure on
            a path that does not need one.
        """
        # Arrange
        local = mocker.patch.object(
            purge_module, "purge_local", return_value=PurgeReport()
        )
        remote = mocker.patch.object(purge_module, "purge_s3")
        factory = mocker.patch.object(purge_module, "build_s3_client")

        # Act
        result = _invoke("--local-root", str(cache_root))

        # Assert
        assert result.exit_code == 0
        local.assert_called_once()
        remote.assert_not_called()
        factory.assert_not_called()

    def test_purge_legacy_cache_should_sweep_the_configured_bucket_and_prefix(
        self, mocker
    ):
        """Test that the S3 branch wires bucket, prefix, and apply through.

        Given:
            A moto-backed bucket holding a legacy and a current object
            under a dev/ prefix.
        When:
            The command is invoked with --s3-bucket, --s3-prefix, and
            --apply.
        Then:
            It should report the prefixed S3 target and delete only the
            legacy object.
        """
        # Arrange
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket=_BUCKET)
            client.put_object(Bucket=_BUCKET, Key=f"dev/{_LEGACY_KEY}", Body=b"stale")
            client.put_object(Bucket=_BUCKET, Key=f"dev/{_CURRENT_KEY}", Body=b"live")
            mocker.patch.object(purge_module, "build_s3_client", return_value=client)

            # Act
            result = _invoke("--s3-bucket", _BUCKET, "--s3-prefix", "dev", "--apply", "--yes")

            # Assert
            assert result.exit_code == 0
            assert f"Target: s3://{_BUCKET}/dev" in result.output
            assert "Deleted: 1" in result.output
            remaining = {
                obj["Key"]
                for obj in client.list_objects_v2(Bucket=_BUCKET).get("Contents", ())
            }
            assert remaining == {f"dev/{_CURRENT_KEY}"}

    def test_purge_legacy_cache_should_render_an_unprefixed_s3_target(self, mocker):
        """Test that an empty prefix leaves no trailing slash.

        Given:
            An S3 bucket configured with no prefix.
        When:
            The command is invoked.
        Then:
            The target line should read the bare bucket URL, so the
            operator sees exactly the scope that will be swept.
        """
        # Arrange
        mocker.patch.object(purge_module, "build_s3_client", return_value=object())
        mocker.patch.object(purge_module, "purge_s3", return_value=PurgeReport())

        # Act
        result = _invoke("--s3-bucket", _BUCKET)

        # Assert
        assert result.exit_code == 0
        assert f"Target: s3://{_BUCKET}\n" in result.output

    def test_purge_legacy_cache_should_resolve_every_option_from_the_environment(
        self, monkeypatch, mocker
    ):
        """Test that the documented environment bindings are wired.

        Given:
            WORKFLOW_S3_BUCKET, WORKFLOW_S3_PREFIX, AWS_ENDPOINT_URL, and
            AWS_REGION set, with no flags passed.
        When:
            The command is invoked with --apply.
        Then:
            The client factory should receive the endpoint and region and
            the sweep should receive the bucket and prefix — a dropped
            endpoint would silently point a LocalStack sweep at real AWS.
        """
        # Arrange
        monkeypatch.setenv("WORKFLOW_S3_BUCKET", "env-bucket")
        monkeypatch.setenv("WORKFLOW_S3_PREFIX", "staging")
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localstack:4566")
        monkeypatch.setenv("AWS_REGION", "us-west-2")
        factory = mocker.patch.object(
            purge_module, "build_s3_client", return_value=object()
        )
        sweep = mocker.patch.object(
            purge_module, "purge_s3", return_value=PurgeReport()
        )

        # Act
        result = _invoke("--apply", "--yes")

        # Assert
        assert result.exit_code == 0
        factory.assert_called_once_with(
            endpoint_url="http://localstack:4566", region_name="us-west-2"
        )
        assert sweep.call_args.args[1] == "env-bucket"
        assert sweep.call_args.kwargs["prefix"] == "staging"
        assert sweep.call_args.kwargs["apply"] is True

    def test_purge_legacy_cache_should_render_the_reclaimable_size(self, mocker):
        """Test that the report sizes the sweep for an operator.

        Given:
            A sweep reporting a multi-gibibyte byte total.
        When:
            The command is invoked as a dry run.
        Then:
            It should print the thousands-separated byte count alongside
            a two-decimal GiB figure, so the operator can judge the
            reclaim before committing to it.
        """
        # Arrange
        mocker.patch.object(purge_module, "build_s3_client", return_value=object())
        mocker.patch.object(
            purge_module,
            "purge_s3",
            return_value=PurgeReport(
                scanned=9, matched=4, deleted=0, bytes_matched=1_234_567_890
            ),
        )

        # Act
        result = _invoke("--s3-bucket", _BUCKET)

        # Assert
        assert "Scanned: 9" in result.output
        assert "Legacy entries: 4 (1,234,567,890 bytes, 1.15 GiB)" in result.output

    def test_purge_legacy_cache_should_refuse_when_both_stores_resolve_from_the_environment(
        self, monkeypatch, mocker, tmp_path
    ):
        """Test that the ambiguity guard covers the environment-only pairing.

        Given:
            WORKFLOW_S3_BUCKET and SYNC_DATA_DIR both exported and no
            flags at all — the shape a deployed container has, since
            backend.yml sets both on one task.
        When:
            The command is invoked with --apply.
        Then:
            It should exit non-zero and sweep neither store. Resolving the
            local root only when no bucket is configured would let the
            bucket win silently here, deleting from production for an
            operator who meant their local cache.
        """
        # Arrange
        monkeypatch.setenv("WORKFLOW_S3_BUCKET", "prod-bucket")
        monkeypatch.setenv("SYNC_DATA_DIR", str(tmp_path))
        local = mocker.patch.object(purge_module, "purge_local")
        remote = mocker.patch.object(purge_module, "purge_s3")

        # Act
        result = _invoke("--apply", "--yes")

        # Assert
        assert result.exit_code != 0
        assert "Both an S3 bucket and a local cache root" in result.output
        local.assert_not_called()
        remote.assert_not_called()

    def test_purge_legacy_cache_should_name_the_target_before_sweeping(
        self, cache_root, mocker
    ):
        """Test that the target is printed even when the sweep raises.

        Given:
            A sweep that raises part-way, as a partial S3 delete failure
            does.
        When:
            The command is invoked.
        Then:
            The target should already be in the output. The store is
            chosen partly from ambient environment, so an operator must
            be able to see which one was picked without waiting for a
            sweep that may never return.
        """
        # Arrange
        mocker.patch.object(
            purge_module, "purge_local", side_effect=RuntimeError("boom")
        )

        # Act
        result = _invoke("--local-root", str(cache_root))

        # Assert
        assert result.exit_code != 0
        assert f"Target: {cache_root}" in result.output

    def test_purge_legacy_cache_should_abort_when_the_confirmation_is_declined(
        self, cache_root
    ):
        """Test that --apply asks before deleting anything.

        Given:
            A local cache root holding a legacy entry.
        When:
            The command is invoked with --apply and the prompt is
            answered "n".
        Then:
            It should exit non-zero and leave the entry in place. The
            flag is one word away from an irreversible mass delete, so it
            gates on an explicit answer rather than on the flag alone.
        """
        # Act
        result = _invoke("--local-root", str(cache_root), "--apply", input="n\n")

        # Assert
        assert result.exit_code != 0
        assert (cache_root / _LEGACY_KEY).exists()

    def test_purge_legacy_cache_should_warn_when_it_matched_nothing(
        self, cache_root, mocker
    ):
        """Test that a mis-targeted sweep is distinguishable from a clean one.

        Given:
            A sweep that scanned entries but matched none — the shape an
            under-specified --s3-prefix produces.
        When:
            The command is invoked.
        Then:
            It should warn about the prefix. Reporting only "Legacy
            entries: 0" would let an operator tick an environment off the
            migration runbook on the strength of a typo.
        """
        # Arrange
        mocker.patch.object(purge_module, "build_s3_client", return_value=object())
        mocker.patch.object(
            purge_module, "purge_s3", return_value=PurgeReport(scanned=12, matched=0)
        )

        # Act
        result = _invoke("--s3-bucket", _BUCKET)

        # Assert
        assert "matched none" in result.output
        assert "WORKFLOW_S3_PREFIX" in result.output

    def test_purge_legacy_cache_should_warn_when_the_target_held_nothing(
        self, cache_root, mocker
    ):
        """Test that an empty target is called out rather than reported clean.

        Given:
            A sweep that scanned nothing at all — the shape a typo'd
            bucket or an unwritten cache root produces.
        When:
            The command is invoked.
        Then:
            It should warn that the target held nothing.
        """
        # Arrange
        mocker.patch.object(purge_module, "build_s3_client", return_value=object())
        mocker.patch.object(
            purge_module, "purge_s3", return_value=PurgeReport(scanned=0, matched=0)
        )

        # Act
        result = _invoke("--s3-bucket", _BUCKET)

        # Assert
        assert "held nothing" in result.output

    def test_purge_legacy_cache_should_reject_a_local_root_that_does_not_exist(
        self, tmp_path, mocker
    ):
        """Test that a mistyped --local-root is a usage error.

        Given:
            An explicit --local-root naming a directory that is not there.
        When:
            The command is invoked.
        Then:
            It should exit non-zero without sweeping. A typo would
            otherwise produce a zeroed report indistinguishable from an
            already-swept cache.
        """
        # Arrange
        local = mocker.patch.object(purge_module, "purge_local")

        # Act
        result = _invoke("--local-root", str(tmp_path / "absent"))

        # Assert
        assert result.exit_code != 0
        assert "does not exist" in result.output
        local.assert_not_called()
