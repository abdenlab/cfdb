"""Tests for the legacy cache-key purge sweep."""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from moto import mock_aws

from cfdb.workflows.cache import LocalFsCache, S3Cache
from cfdb.workflows.keys import cache_key
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.purge import (
    PurgeReport,
    build_s3_client,
    purge_local,
    purge_s3,
)
from tests.test_workflows import FIXTURE_MD5

_BUCKET = "cfdb-test-purge"

#: A key of the retired four-segment shape, as the pipeline minted before
#: the processor-identity segment existed.
_LEGACY_KEY = f"encode/ENCFF732YBO/index/{FIXTURE_MD5}-v2"

#: The same artifact under the current scheme — must survive every sweep.
_CURRENT_KEY = cache_key(
    dcc="encode",
    local_id="ENCFF732YBO",
    artifact_kind=ArtifactKind.INDEX,
    md5=FIXTURE_MD5,
    processor_id="tabix-interval",
    processor_version=2,
)


@pytest.fixture()
def s3_client():
    """Return a moto-backed boto3 S3 client with one created bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


def _keys_in(client, bucket: str) -> set[str]:
    """Return every object key currently in ``bucket``, across all pages."""
    keys: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        keys.update(obj["Key"] for obj in page.get("Contents", ()))
    return keys


def _seed_local(root: Path, key: str, payload: bytes = b"artifact") -> Path:
    """Write ``payload`` into ``root`` at the cache-key path and return it."""
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _legacy_key(local_id: str) -> str:
    """Return a retired-scheme key for ``local_id``."""
    return f"encode/{local_id}/index/{FIXTURE_MD5}-v2"


class _StubPage(dict):
    """One ``list_objects_v2`` page, shaped as botocore returns it."""


class _StubClient:
    """A minimal S3 client double for the delete-response paths.

    ``purge_s3`` takes its client as a public parameter, so driving it
    with a double stays on the public surface. A double is required
    rather than moto because moto only populates ``Errors`` for versioned
    deletes, which the sweep never issues — the failure mode this covers
    is therefore unreachable through the real mock.
    """

    def __init__(self, keys, delete_responses):
        self._keys = list(keys)
        self._delete_responses = list(delete_responses)
        self.delete_calls: list[list[str]] = []

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        contents = [{"Key": key, "Size": 4} for key in self._keys]
        yield _StubPage(Contents=contents)

    def delete_objects(self, *, Bucket, Delete):
        requested = [entry["Key"] for entry in Delete["Objects"]]
        self.delete_calls.append(requested)
        return self._delete_responses.pop(0)


class TestPurgeReport:
    def test_purge_report_should_default_every_counter_to_zero(self):
        """Test that a fresh report describes an examined-nothing sweep.

        Given:
            No arguments.
        When:
            A PurgeReport is constructed.
        Then:
            All four counters should be zero — this is the value every
            early-return path hands back, so it must read as "nothing
            found" rather than as unset.
        """
        # Act
        report = PurgeReport()

        # Assert
        assert (
            report.scanned,
            report.matched,
            report.deleted,
            report.bytes_matched,
        ) == (0, 0, 0, 0)


class TestBuildS3Client:
    def test_build_s3_client_should_honor_the_supplied_endpoint_override(self):
        """Test that the sweep's client targets the endpoint it is given.

        Given:
            An explicit endpoint_url, as a LocalStack-backed environment
            supplies through AWS_ENDPOINT_URL.
        When:
            build_s3_client is called with it.
        Then:
            It should return an s3 client bound to that endpoint, so a
            dev sweep cannot be redirected at real AWS by the default
            resolver chain.
        """
        # Act & assert
        with mock_aws():
            client = build_s3_client(
                endpoint_url="http://localstack:4566", region_name="us-east-1"
            )

            assert client.meta.service_model.service_name == "s3"
            assert client.meta.endpoint_url == "http://localstack:4566"

    def test_build_s3_client_should_resolve_a_default_endpoint_from_the_region(self):
        """Test that a production sweep needs no endpoint configuration.

        Given:
            A region but no endpoint override.
        When:
            build_s3_client is called.
        Then:
            It should return an s3 client pointed at AWS, so the common
            production invocation works with nothing but a region.
        """
        # Act & assert
        with mock_aws():
            client = build_s3_client(region_name="us-east-1")

            assert client.meta.service_model.service_name == "s3"
            assert "amazonaws.com" in client.meta.endpoint_url


class TestPurgeS3:
    def test_purge_s3_should_delete_legacy_keys_and_keep_current_ones(
        self, s3_client
    ):
        """Test that an applied sweep removes only retired-scheme objects.

        Given:
            A bucket holding one legacy-scheme object and one current
            key derived by cache_key.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should delete the legacy object and leave the current one,
            reporting one match and one deletion.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=_LEGACY_KEY, Body=b"stale")
        s3_client.put_object(Bucket=_BUCKET, Key=_CURRENT_KEY, Body=b"live")

        # Act
        report = purge_s3(s3_client, _BUCKET, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (2, 1, 1)
        assert _keys_in(s3_client, _BUCKET) == {_CURRENT_KEY}

    def test_purge_s3_should_delete_nothing_when_not_applied(self, s3_client):
        """Test that the default dry run reports without deleting.

        Given:
            A bucket holding one legacy-scheme object.
        When:
            purge_s3 runs without apply.
        Then:
            It should report the match and its size but leave the object
            in place, so an operator can size the sweep before committing.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=_LEGACY_KEY, Body=b"stale")

        # Act
        report = purge_s3(s3_client, _BUCKET)

        # Assert
        assert (report.matched, report.deleted, report.bytes_matched) == (1, 0, 5)
        assert _keys_in(s3_client, _BUCKET) == {_LEGACY_KEY}

    def test_purge_s3_should_strip_the_configured_prefix_before_matching(
        self, s3_client
    ):
        """Test that a bucket prefix is not counted as a key segment.

        Given:
            A bucket whose cache lives under a ``dev/`` prefix, holding a
            legacy-scheme entry and a current one.
        When:
            purge_s3 runs with that prefix and apply=True.
        Then:
            It should delete the prefixed legacy entry — the prefix is
            the backend's namespacing, not part of the cache key, so
            counting it would make every key look five-segment and match
            nothing.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=f"dev/{_LEGACY_KEY}", Body=b"stale")
        s3_client.put_object(Bucket=_BUCKET, Key=f"dev/{_CURRENT_KEY}", Body=b"live")

        # Act
        report = purge_s3(s3_client, _BUCKET, prefix="dev", apply=True)

        # Assert
        assert report.deleted == 1
        assert _keys_in(s3_client, _BUCKET) == {f"dev/{_CURRENT_KEY}"}

    def test_purge_s3_should_ignore_objects_outside_the_configured_prefix(
        self, s3_client
    ):
        """Test that a sweep scoped to one prefix leaves its neighbours alone.

        Given:
            Legacy-scheme entries under two environment prefixes.
        When:
            purge_s3 runs against only one of them with apply=True.
        Then:
            It should leave the other environment's entry untouched, so a
            shared bucket can be purged one environment at a time.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=f"dev/{_LEGACY_KEY}", Body=b"stale")
        s3_client.put_object(Bucket=_BUCKET, Key=f"prod/{_LEGACY_KEY}", Body=b"stale")

        # Act
        report = purge_s3(s3_client, _BUCKET, prefix="dev", apply=True)

        # Assert
        assert (report.scanned, report.deleted) == (1, 1)
        assert _keys_in(s3_client, _BUCKET) == {f"prod/{_LEGACY_KEY}"}

    def test_purge_s3_should_keep_a_live_key_when_the_prefix_is_over_specified(
        self, s3_client
    ):
        """Test that a mistyped prefix cannot delete the live cache.

        Given:
            A bucket holding a current-scheme artifact under a "dev"
            prefix, swept with a prefix carrying one segment too many —
            the shape a typo in WORKFLOW_S3_PREFIX produces.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should delete nothing and leave the artifact in place.
            Over-stripping leaves the processor identity in the
            artifact-kind slot, which the legacy predicate rejects;
            without that check this sweep would empty a live cache and
            the deletion is irreversible.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=f"dev/{_CURRENT_KEY}", Body=b"live")

        # Act
        report = purge_s3(s3_client, _BUCKET, prefix="dev/encode", apply=True)

        # Assert
        assert (report.matched, report.deleted) == (0, 0)
        assert _keys_in(s3_client, _BUCKET) == {f"dev/{_CURRENT_KEY}"}

    def test_purge_s3_should_return_an_empty_report_for_an_empty_bucket(
        self, s3_client
    ):
        """Test that a cold or already-swept bucket is a clean no-op.

        Given:
            An empty bucket.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should return a zeroed report without raising, so the
            sweep is safe to run against an environment that has none.
        """
        # Act
        report = purge_s3(s3_client, _BUCKET, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (0, 0, 0)

    def test_purge_s3_should_leave_a_bucket_of_only_current_keys_untouched(
        self, s3_client
    ):
        """Test that a fully-migrated cache survives the sweep.

        Given:
            A bucket holding only current-scheme keys.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should scan them, match none, and leave every object in
            place — the steady state after one successful migration.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=_CURRENT_KEY, Body=b"live")

        # Act
        report = purge_s3(s3_client, _BUCKET, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (1, 0, 0)
        assert _keys_in(s3_client, _BUCKET) == {_CURRENT_KEY}

    def test_purge_s3_should_ignore_objects_that_are_not_cache_entries(self, s3_client):
        """Test that the sweep never deletes objects it does not own.

        Given:
            A bucket mixing a legacy entry with unrelated objects that
            share it — a top-level README and a nested log file.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should delete only the legacy entry while counting every
            object as scanned, so a bucket shared with another workload
            keeps its data.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=_LEGACY_KEY, Body=b"stale")
        s3_client.put_object(Bucket=_BUCKET, Key="README.md", Body=b"notes")
        s3_client.put_object(
            Bucket=_BUCKET, Key=f"logs/2024/01/{FIXTURE_MD5}-v2", Body=b"log"
        )

        # Act
        report = purge_s3(s3_client, _BUCKET, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (3, 1, 1)
        assert _keys_in(s3_client, _BUCKET) == {
            "README.md",
            f"logs/2024/01/{FIXTURE_MD5}-v2",
        }

    @pytest.mark.parametrize("prefix", ["dev", "dev/", "/dev/"])
    def test_purge_s3_should_normalize_the_prefix_decoration(self, s3_client, prefix):
        """Test that a prefix's slashes cannot change what is swept.

        Given:
            A bucket whose cache lives under "dev/", swept with the
            prefix written bare, trailing-slashed, or fully slashed.
        When:
            purge_s3 runs with apply=True for each form.
        Then:
            It should report the same match every time, matching the
            normalization S3Cache applies to the same value.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=f"dev/{_LEGACY_KEY}", Body=b"stale")

        # Act
        report = purge_s3(s3_client, _BUCKET, prefix=prefix, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (1, 1, 1)

    def test_purge_s3_should_not_sweep_a_neighbouring_environment(self, s3_client):
        """Test that a textual prefix match cannot cross environments.

        Given:
            Legacy entries under "dev/", "dev-staging/", and "devops/" —
            prefixes of which one is a strict textual prefix of the
            others.
        When:
            purge_s3 runs with prefix="dev" and apply=True.
        Then:
            It should touch only the "dev/" entry, so purging one
            environment in a shared bucket cannot take its neighbours
            with it.
        """
        # Arrange
        for env in ("dev", "dev-staging", "devops"):
            s3_client.put_object(Bucket=_BUCKET, Key=f"{env}/{_LEGACY_KEY}", Body=b"x")

        # Act
        report = purge_s3(s3_client, _BUCKET, prefix="dev", apply=True)

        # Assert
        assert (report.scanned, report.deleted) == (1, 1)
        assert _keys_in(s3_client, _BUCKET) == {
            f"dev-staging/{_LEGACY_KEY}",
            f"devops/{_LEGACY_KEY}",
        }

    def test_purge_s3_should_report_the_same_totals_dry_and_applied(self, s3_client):
        """Test that the dry run is an honest preview of the real sweep.

        Given:
            A bucket holding legacy entries of known differing sizes
            alongside a current entry.
        When:
            purge_s3 runs first as a dry run and then with apply=True.
        Then:
            Both runs should report the same match count and byte total,
            with only the applied run deleting — so an operator can size
            the sweep before committing to it.
        """
        # Arrange
        s3_client.put_object(Bucket=_BUCKET, Key=_legacy_key("A"), Body=b"1234")
        s3_client.put_object(Bucket=_BUCKET, Key=_legacy_key("B"), Body=b"123456")
        s3_client.put_object(Bucket=_BUCKET, Key=_CURRENT_KEY, Body=b"live")

        # Act
        preview = purge_s3(s3_client, _BUCKET)
        applied = purge_s3(s3_client, _BUCKET, apply=True)

        # Assert
        assert (preview.matched, preview.bytes_matched, preview.deleted) == (2, 10, 0)
        assert (applied.matched, applied.bytes_matched, applied.deleted) == (2, 10, 2)
        assert _keys_in(s3_client, _BUCKET) == {_CURRENT_KEY}

    def test_purge_s3_should_delete_beyond_a_single_request_batch(self, s3_client):
        """Test that a sweep larger than one delete request loses nothing.

        Given:
            A bucket holding more legacy objects than one DeleteObjects
            request accepts, spanning more than one listing page.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should delete every one of them, so neither the pagination
            boundary nor the batch flush strands part of a production
            cache while reporting success.
        """
        # Arrange
        keys = {_legacy_key(f"ENCFF{index:05d}") for index in range(1001)}
        for key in keys:
            s3_client.put_object(Bucket=_BUCKET, Key=key, Body=b"x")

        # Act
        report = purge_s3(s3_client, _BUCKET, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (1001, 1001, 1001)
        assert _keys_in(s3_client, _BUCKET) == set()

    def test_purge_s3_should_not_delete_any_batch_on_a_dry_run(self, s3_client):
        """Test that the mid-sweep flush is gated on apply.

        Given:
            A bucket holding more legacy objects than one delete request
            accepts.
        When:
            purge_s3 runs without apply.
        Then:
            It should match them all, delete none, and leave the bucket
            intact — the batch flush inside the scan loop must respect
            the dry run as much as the final flush does.
        """
        # Arrange
        keys = {_legacy_key(f"ENCFF{index:05d}") for index in range(1001)}
        for key in keys:
            s3_client.put_object(Bucket=_BUCKET, Key=key, Body=b"x")

        # Act
        report = purge_s3(s3_client, _BUCKET)

        # Assert
        assert (report.matched, report.deleted) == (1001, 0)
        assert _keys_in(s3_client, _BUCKET) == keys

    def test_purge_s3_should_raise_when_a_delete_reports_errors(self):
        """Test that a partial failure cannot pass as a completed sweep.

        Given:
            A client whose DeleteObjects reports a per-key failure in the
            response body, as a missing s3:DeleteObject grant produces.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should raise RuntimeError naming the failing key. S3
            reports these failures without raising, so silence is the
            default and an operator would otherwise conclude a cache was
            purged when nothing was.
        """
        # Arrange
        client = _StubClient(
            keys=[_LEGACY_KEY],
            delete_responses=[
                {"Errors": [{"Key": _LEGACY_KEY, "Message": "AccessDenied"}]}
            ],
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="AccessDenied"):
            purge_s3(client, _BUCKET, apply=True)

    def test_purge_s3_should_raise_when_a_later_batch_reports_errors(self):
        """Test that a late failure is not masked by an early success.

        Given:
            A client whose second DeleteObjects call reports an error
            while the first succeeded, over more keys than one batch.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should raise RuntimeError, so a permission or throttling
            failure part-way through a large sweep is surfaced rather
            than averaged away by the batches that worked.
        """
        # Arrange
        keys = [_legacy_key(f"ENCFF{index:05d}") for index in range(1001)]
        client = _StubClient(
            keys=keys,
            delete_responses=[
                {"Deleted": [{"Key": key} for key in keys[:1000]]},
                {"Errors": [{"Key": keys[1000], "Message": "SlowDown"}]},
            ],
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="SlowDown"):
            purge_s3(client, _BUCKET, apply=True)

    def test_purge_s3_should_raise_when_s3_confirms_fewer_keys_than_matched(self):
        """Test that a silent undercount is not reported as success.

        Given:
            A client whose DeleteObjects confirms fewer keys than were
            requested and reports no errors at all.
        When:
            purge_s3 runs with apply=True.
        Then:
            It should raise RuntimeError. A key that was already absent
            still comes back confirmed, so a shortfall means something
            was neither deleted nor complained about, and the report
            would otherwise claim a clean sweep.
        """
        # Arrange
        keys = [_legacy_key("A"), _legacy_key("B")]
        client = _StubClient(
            keys=keys, delete_responses=[{"Deleted": [{"Key": keys[0]}]}]
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="not fully purged"):
            purge_s3(client, _BUCKET, apply=True)

    def test_purge_s3_should_issue_no_delete_request_on_a_dry_run(self):
        """Test that the dry run is side-effect-free at the client boundary.

        Given:
            A recording client over a bucket holding legacy keys.
        When:
            purge_s3 runs without apply.
        Then:
            DeleteObjects should never be invoked — the dry run is proven
            inert at the API call, not merely by the bucket looking
            unchanged afterwards.
        """
        # Arrange
        client = _StubClient(keys=[_LEGACY_KEY], delete_responses=[])

        # Act
        report = purge_s3(client, _BUCKET)

        # Assert
        assert report.matched == 1
        assert client.delete_calls == []

    @pytest.mark.asyncio
    async def test_purge_s3_should_agree_with_the_prefix_s3_cache_writes_under(
        self, s3_client, tmp_path
    ):
        """Test that the sweep strips exactly what the backend prepends.

        Given:
            An artifact written through S3Cache with a configured prefix,
            under a key derived by the current cache_key.
        When:
            purge_s3 sweeps the same bucket with the same prefix.
        Then:
            It should delete nothing and the artifact should still be
            readable through the cache — pinning that the two modules
            agree about the prefix rather than merely looking similar.
        """
        # Arrange
        cache = S3Cache(bucket=_BUCKET, prefix="dev", client=s3_client)
        source = tmp_path / "artifact.tbi"
        source.write_bytes(b"payload")
        await cache.put(_CURRENT_KEY, source)

        # Act
        report = purge_s3(s3_client, _BUCKET, prefix="dev", apply=True)

        # Assert
        assert (report.scanned, report.matched) == (1, 0)
        assert await cache.head(_CURRENT_KEY) is not None


class TestPurgeLocal:
    def test_purge_local_should_delete_legacy_entries_and_keep_current_ones(
        self, tmp_path
    ):
        """Test that an applied sweep removes only retired-scheme entries.

        Given:
            A local cache root holding one legacy-scheme file and one
            current-scheme file.
        When:
            purge_local runs with apply=True.
        Then:
            It should unlink the legacy file and leave the current one.
        """
        # Arrange
        legacy = _seed_local(tmp_path, _LEGACY_KEY)
        current = _seed_local(tmp_path, _CURRENT_KEY)

        # Act
        report = purge_local(tmp_path, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (2, 1, 1)
        assert not legacy.exists()
        assert current.exists()

    def test_purge_local_should_delete_nothing_when_not_applied(self, tmp_path):
        """Test that the default dry run reports without deleting.

        Given:
            A local cache root holding one legacy-scheme file.
        When:
            purge_local runs without apply.
        Then:
            It should report the match and leave the file on disk.
        """
        # Arrange
        legacy = _seed_local(tmp_path, _LEGACY_KEY, b"stale")

        # Act
        report = purge_local(tmp_path)

        # Assert
        assert (report.matched, report.deleted, report.bytes_matched) == (1, 0, 5)
        assert legacy.exists()

    def test_purge_local_should_prune_directories_left_empty(self, tmp_path):
        """Test that the sweep does not leave the retired tree behind.

        Given:
            A cache root whose only content is one legacy-scheme file.
        When:
            purge_local runs with apply=True.
        Then:
            It should remove the now-empty directories under the root,
            leaving the root itself in place.
        """
        # Arrange
        _seed_local(tmp_path, _LEGACY_KEY)

        # Act
        purge_local(tmp_path, apply=True)

        # Assert
        assert tmp_path.is_dir()
        assert list(tmp_path.iterdir()) == []

    def test_purge_local_should_return_an_empty_report_when_root_absent(
        self, tmp_path
    ):
        """Test that a missing cache root is not an error.

        Given:
            A path where no cache root was ever created.
        When:
            purge_local is called on it.
        Then:
            It should return a zeroed report, so running the sweep on a
            deployment that never wrote a local cache is a no-op.
        """
        # Act
        report = purge_local(tmp_path / "never-created")

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (0, 0, 0)

    def test_purge_local_should_return_an_empty_report_when_root_is_a_file(
        self, tmp_path
    ):
        """Test that a mistyped root pointing at a file is inert.

        Given:
            A path that exists but is a regular file rather than a
            directory.
        When:
            purge_local is called on it.
        Then:
            It should return a zeroed report rather than raise, so a
            mistyped --local-root does nothing instead of failing loudly
            part-way through.
        """
        # Arrange
        not_a_root = tmp_path / "cache"
        not_a_root.write_bytes(b"not a directory")

        # Act
        report = purge_local(not_a_root)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (0, 0, 0)
        assert not_a_root.exists()

    def test_purge_local_should_keep_directories_that_still_hold_a_current_entry(
        self, tmp_path
    ):
        """Test that pruning stops at the first surviving artifact.

        Given:
            A legacy entry and a current entry sharing their leading
            dcc and local_id directories.
        When:
            purge_local runs with apply=True.
        Then:
            It should delete the legacy entry while leaving the current
            one and every directory above it, so a sweep cannot orphan a
            live artifact by pruning its parents out from under it.
        """
        # Arrange
        legacy = _seed_local(tmp_path, _LEGACY_KEY)
        current = _seed_local(tmp_path, _CURRENT_KEY)

        # Act
        purge_local(tmp_path, apply=True)

        # Assert
        assert not legacy.exists()
        assert current.exists()
        assert current.parent.is_dir()

    def test_purge_local_should_keep_an_unrelated_empty_directory(self, tmp_path):
        """Test that the sweep prunes only what its own deletions emptied.

        Given:
            A cache root holding one legacy entry alongside an unrelated
            directory that was already empty.
        When:
            purge_local runs with apply=True.
        Then:
            The unrelated directory should survive. The root is
            operator-supplied through --local-root, so a directory the
            sweep never touched is not the sweep's to reclaim.
        """
        # Arrange
        _seed_local(tmp_path, _LEGACY_KEY)
        unrelated = tmp_path / "staging"
        unrelated.mkdir()

        # Act
        report = purge_local(tmp_path, apply=True)

        # Assert
        assert report.deleted == 1
        assert unrelated.is_dir()

    def test_purge_local_should_not_prune_on_a_dry_run(self, tmp_path):
        """Test that a dry run leaves the tree shape untouched.

        Given:
            A cache root whose only content is one legacy entry.
        When:
            purge_local runs without apply.
        Then:
            Every directory should remain, so the preview mutates nothing
            at all rather than merely leaving the files in place.
        """
        # Arrange
        legacy = _seed_local(tmp_path, _LEGACY_KEY)

        # Act
        purge_local(tmp_path)

        # Assert
        assert legacy.exists()
        assert legacy.parent.is_dir()

    def test_purge_local_should_ignore_files_that_are_not_cache_entries(self, tmp_path):
        """Test that the local sweep never deletes what it does not own.

        Given:
            A cache root holding a legacy entry alongside a file at the
            root and a non-cache file at legacy depth.
        When:
            purge_local runs with apply=True.
        Then:
            It should count every file as scanned but delete only the
            legacy entry.
        """
        # Arrange
        legacy = _seed_local(tmp_path, _LEGACY_KEY)
        notes = _seed_local(tmp_path, "encode/ENCFF732YBO/index/notes.txt")
        readme = tmp_path / "README"
        readme.write_bytes(b"cache root")

        # Act
        report = purge_local(tmp_path, apply=True)

        # Assert
        assert (report.scanned, report.matched, report.deleted) == (3, 1, 1)
        assert not legacy.exists()
        assert notes.exists() and readme.exists()

    def test_purge_local_should_not_delete_a_directory_named_like_a_legacy_leaf(
        self, tmp_path
    ):
        """Test that only regular files are treated as cache entries.

        Given:
            A directory whose path spells a complete legacy key, holding
            a file of its own.
        When:
            purge_local runs with apply=True.
        Then:
            It should leave the directory and its contents alone, so a
            path that merely looks like an entry is not removed.
        """
        # Arrange
        impostor = tmp_path / _LEGACY_KEY
        impostor.mkdir(parents=True)
        inner = impostor / "payload"
        inner.write_bytes(b"inner")

        # Act
        report = purge_local(tmp_path, apply=True)

        # Assert
        assert (report.matched, report.deleted) == (0, 0)
        assert inner.exists()

    def test_purge_local_should_match_a_zero_byte_legacy_entry(self, tmp_path):
        """Test that an empty artifact is still reclaimed.

        Given:
            A legacy entry of zero bytes.
        When:
            purge_local runs with apply=True.
        Then:
            It should match and delete it, reporting zero bytes freed —
            a falsy size must not be mistaken for "no entry".
        """
        # Arrange
        legacy = _seed_local(tmp_path, _LEGACY_KEY, b"")

        # Act
        report = purge_local(tmp_path, apply=True)

        # Assert
        assert (report.matched, report.deleted, report.bytes_matched) == (1, 1, 0)
        assert not legacy.exists()

    def test_purge_local_should_report_nothing_on_a_second_sweep(self, tmp_path):
        """Test that the sweep is idempotent.

        Given:
            A cache root already swept once with apply=True.
        When:
            purge_local runs with apply=True again.
        Then:
            It should report nothing matched and raise nothing — the
            re-run recovery the module documents for a partial failure
            depends on this.
        """
        # Arrange
        _seed_local(tmp_path, _LEGACY_KEY)
        _seed_local(tmp_path, _CURRENT_KEY)
        purge_local(tmp_path, apply=True)

        # Act
        report = purge_local(tmp_path, apply=True)

        # Assert
        assert (report.matched, report.deleted) == (0, 0)

    @pytest.mark.asyncio
    async def test_purge_local_should_keep_an_entry_written_by_the_cache_backend(
        self, tmp_path
    ):
        """Test that the sweep agrees with what LocalFsCache writes.

        Given:
            An artifact written through LocalFsCache under a key derived
            by the current cache_key.
        When:
            purge_local runs with apply=True.
        Then:
            It should delete nothing and the artifact should still be
            readable through the cache, pinning that producer and sweep
            share one notion of the current key shape.
        """
        # Arrange
        cache = LocalFsCache(tmp_path)
        source = tmp_path / "source.tbi"
        source.write_bytes(b"payload")
        await cache.put(_CURRENT_KEY, source)

        # Act
        report = purge_local(tmp_path, apply=True)

        # Assert
        assert report.matched == 0
        assert await cache.head(_CURRENT_KEY) is not None

    @settings(max_examples=25, deadline=None)
    @given(
        local_id=st.text(
            alphabet=st.characters(whitelist_categories=("L", "N")),
            min_size=1,
            max_size=16,
        ),
        md5=st.text(alphabet="abcdef0123456789", min_size=32, max_size=32),
        artifact_kind=st.sampled_from(list(ArtifactKind)),
        processor_id=st.sampled_from(["tabix-interval", "bam-index", "passthrough"]),
        version=st.integers(min_value=0, max_value=99),
    )
    def test_purge_local_should_delete_exactly_what_cache_key_no_longer_mints(
        self, tmp_path_factory, local_id, md5, artifact_kind, processor_id, version
    ):
        """Test that the sweep and the deriver agree across the input space.

        Given:
            Any file identity, seeded into a cache root twice — once
            under the key cache_key derives today, and once under its
            retired four-segment analogue.
        When:
            purge_local runs with apply=True.
        Then:
            The retired entry should be gone and the derived one should
            survive, for every draw. This is the property that catches
            the two modules drifting apart: whatever cache_key mints is
            exactly what the sweep must not touch.
        """
        # Arrange
        root = tmp_path_factory.mktemp("cache")
        current = cache_key(
            dcc="encode",
            local_id=local_id,
            artifact_kind=artifact_kind,
            md5=md5,
            processor_id=processor_id,
            processor_version=version,
        )
        retired = f"encode/{local_id}/{artifact_kind.value}/{md5}-v{version}"
        current_path = _seed_local(root, current)
        retired_path = _seed_local(root, retired)

        # Act
        report = purge_local(root, apply=True)

        # Assert
        assert report.deleted == 1
        assert not retired_path.exists()
        assert current_path.exists()

    def test_purge_local_should_not_delete_through_a_symlinked_directory(
        self, tmp_path
    ):
        """Test that the sweep cannot reach outside its own cache root.

        Given:
            A cache root containing a symlink to a directory outside it,
            which itself holds a legacy-shaped entry.
        When:
            purge_local runs applied.
        Then:
            It should match nothing and leave the outside file intact.
            Containment rests entirely on ``Path.rglob`` not descending
            symlinked directories — an implicit default that, if it ever
            changed, would turn an irreversible delete loose on arbitrary
            paths. Pinned here rather than inherited.
        """
        # Arrange
        root = tmp_path / "cache"
        root.mkdir()
        outside = tmp_path / "outside"
        _seed_local(outside, _LEGACY_KEY)
        (root / "encode").symlink_to(outside / "encode", target_is_directory=True)

        # Act
        report = purge_local(root, apply=True)

        # Assert
        assert report.matched == 0
        assert report.deleted == 0
        assert (outside / _LEGACY_KEY).exists()
