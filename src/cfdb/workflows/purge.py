"""Sweep cache entries minted under the retired cache-key scheme.

Issue #109 folded a processor identity into ``workflows.keys.cache_key``.
Every key derived under the old shape
(``{dcc}/{local_id}/{artifact_kind}/{md5}-v{processor_version}``) is
therefore unreachable by construction: the router derives the new
five-segment form and probes that, so the old entries are never read
again and never overwritten.

How much that costs depends on the backend, and the two differ:

- ``LocalFsCache`` has no expiry of any kind, so a retired entry is a
  permanent cost. This sweep is the only thing that reclaims it.
- The deployed S3 cache bucket already carries an ``expire-cached-artifacts``
  lifecycle rule (``cloudformation/workers.yml``) with
  ``CacheArtifactExpirationDays``, default 30 — and its own rationale is
  this one: keys are content-addressed, so expiry is safe because a missing
  artifact is simply re-materialized. The retired population therefore
  self-reclaims within a month with no operator action. Sweeping S3
  *accelerates* that reclaim; it is not required for correctness, and an
  operator weighing an irreversible mass-delete against waiting should know
  waiting is supported.

The sweep also clears the orphaned paired-interval artifacts left by PR
#108 — the incorrect ``.tbi`` files built for ``.bedpe`` / ``bigInteract``
before those formats were re-typed. Those are old-scheme keys too, so
they need no separate pass.

Both cache backends are covered because a deployment runs one or the
other: ``S3Cache`` in the ECS profile, ``LocalFsCache`` everywhere else.
The single description of the retired shape lives in
:func:`cfdb.workflows.keys.is_legacy_cache_key`; nothing here re-derives
it.

**This module is temporary.** It exists to carry one migration across one
deploy of every environment. Once each environment has been swept, this
module, the ``cfdb purge-legacy-cache`` command,
:func:`~cfdb.workflows.keys.is_legacy_cache_key`, and the retired-scheme
constants beside it should all be deleted — along with the artifact-kind
rejection in :func:`~cfdb.workflows.keys.normalize_processor_id`, which
exists only to keep an over-stripped prefix from looking like a retired
key and has no bearing on the key itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cfdb.workflows.cache import _build_s3_client
from cfdb.workflows.keys import is_legacy_cache_key

#: S3 caps a single ``DeleteObjects`` request at 1000 keys.
_S3_DELETE_BATCH = 1000


def build_s3_client(
    *, endpoint_url: str | None = None, region_name: str | None = None
) -> Any:
    """Build the boto3 ``s3`` client :func:`purge_s3` operates through.

    Shares :mod:`cfdb.workflows.cache`'s client factory so the sweep
    resolves ``endpoint_url`` / ``region_name`` exactly the way the cache
    backend it is sweeping does — a LocalStack-backed dev environment
    would otherwise be purged against real AWS. Leave both ``None`` to
    let boto3's default session resolver pick them up.
    """
    return _build_s3_client(endpoint_url=endpoint_url, region_name=region_name)


@dataclass
class PurgeReport:
    """Accounting for one purge run.

    Attributes:
        scanned: Cache entries examined.
        matched: Entries recognised as legacy-scheme keys.
        deleted: Entries actually removed — zero on a dry run, equal to
            ``matched`` on a successful applied run.
        bytes_matched: Total size of the matched entries. Reported on a
            dry run too, so an operator can see what the sweep would
            reclaim before committing to it.
    """

    scanned: int = 0
    matched: int = 0
    deleted: int = 0
    bytes_matched: int = 0


def purge_s3(
    client: Any,
    bucket: str,
    *,
    prefix: str = "",
    apply: bool = False,
) -> PurgeReport:
    """Delete legacy-scheme objects from an ``S3Cache`` bucket.

    Args:
        client: A boto3 ``s3`` client.
        bucket: Bucket holding the workflow cache.
        prefix: The cache's ``WORKFLOW_S3_PREFIX``. Stripped from each
            object key before the legacy-shape test, since the prefix is
            the backend's own namespacing and not part of the cache key.
        apply: When False (the default) nothing is deleted and the report
            describes what would be.

    Returns:
        A :class:`PurgeReport` for the run.
    """
    normalized = prefix.strip("/")
    list_prefix = f"{normalized}/" if normalized else ""
    report = PurgeReport()
    batch: list[str] = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", ()):
            report.scanned += 1
            key = obj["Key"]
            if not is_legacy_cache_key(key[len(list_prefix) :]):
                continue
            report.matched += 1
            report.bytes_matched += obj.get("Size", 0)
            # Only the applied path consumes ``batch``. Accumulating on a
            # dry run would hold one string per matched object for the whole
            # sweep and then discard them — and the dry run is both the
            # default and the one an operator points at the largest,
            # least-swept cache first.
            if not apply:
                continue
            batch.append(key)
            if len(batch) >= _S3_DELETE_BATCH:
                report.deleted += _delete_s3_batch(client, bucket, batch)
                batch = []

    if apply and batch:
        report.deleted += _delete_s3_batch(client, bucket, batch)

    if apply and report.deleted != report.matched:
        # ``DeleteObjects`` with ``Quiet: False`` echoes every key it
        # removed, and a key that was already gone still comes back as
        # deleted — so a shortfall here means S3 neither deleted nor
        # complained about something, and the sweep is not the clean one
        # the report would otherwise claim. Re-running is safe.
        raise RuntimeError(
            f"purge swept {report.matched} legacy keys but S3 confirmed only "
            f"{report.deleted}; the cache was not fully purged"
        )
    return report


def _delete_s3_batch(client: Any, bucket: str, keys: list[str]) -> int:
    """Delete one batch of object keys; return how many S3 confirmed.

    ``DeleteObjects`` reports per-key failures in the response body rather
    than raising, so a partial failure would otherwise pass silently as a
    completed sweep. Raise instead — the sweep is idempotent, so re-running
    it after fixing the cause (usually a missing ``s3:DeleteObject`` grant)
    picks up whatever is left.
    """
    response = client.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": key} for key in keys], "Quiet": False},
    )
    errors = response.get("Errors", ())
    if errors:
        raise RuntimeError(
            f"{len(errors)} of {len(keys)} deletions failed; first was "
            f"{errors[0].get('Key')!r}: {errors[0].get('Message')}"
        )
    return len(response.get("Deleted", ()))


def purge_local(root: Path, *, apply: bool = False) -> PurgeReport:
    """Delete legacy-scheme entries from a ``LocalFsCache`` root.

    Args:
        root: The cache root (``$SYNC_DATA_DIR/cache``). A root that does
            not exist yields an empty report rather than an error — a
            deployment that never wrote a local cache has nothing to
            purge.
        apply: When False (the default) nothing is deleted and the report
            describes what would be.

    Returns:
        A :class:`PurgeReport` for the run. Directories the deletions
        emptied are pruned so the tree does not retain the shape of the
        retired scheme.

    Run this with the API stopped. The pruning below removes a directory
    the moment its last entry goes, which can land between a concurrent
    ``LocalFsCache.put``'s ``mkdir(parents=True)`` and its ``os.replace``
    and fail that workflow. The window is narrow and the sweep is a
    one-off migration step, so quiescing is cheaper than coordinating.
    """
    report = PurgeReport()
    if not root.is_dir():
        return report

    emptied: list[Path] = []
    # ``rglob`` does not descend symlinked directories, which is what keeps
    # the sweep inside ``root``: a symlinked cache subdirectory would
    # otherwise let an irreversible delete reach arbitrary paths. Pinned by
    # ``test_purge_local_should_not_delete_through_a_symlinked_directory``.
    # Iterated lazily — the result is order-independent (matching is
    # per-path and pruning is deferred to ``emptied``), so sorting would
    # only force the whole tree into memory first.
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        report.scanned += 1
        if not is_legacy_cache_key(path.relative_to(root).as_posix()):
            continue
        report.matched += 1
        report.bytes_matched += path.stat().st_size
        if apply:
            path.unlink()
            report.deleted += 1
            emptied.append(path.parent)

    for directory in emptied:
        _prune_empty_ancestors(directory, root)
    return report


def _prune_empty_ancestors(directory: Path, root: Path) -> None:
    """Remove ``directory`` and its now-empty parents, stopping at ``root``.

    Scoped to the ancestors of a deleted entry rather than walking the
    whole tree: a sweep must not remove directories it never touched.
    ``$SYNC_DATA_DIR/cache`` is operator-supplied, so an unrelated empty
    directory under it is not the sweep's to reclaim. Stops at the first
    ancestor that still holds something, and never removes ``root``.

    The ``root in current.parents`` half of the loop condition is
    unreachable for every caller: ``directory`` is always ``path.parent``
    for a ``path`` yielded by ``root.rglob``, so the walk is inside ``root``
    by construction and ``current != root`` alone terminates it. It is kept
    as a containment assertion for an irreversible delete — do not write a
    test for the state it guards, because no caller can reach it.
    """
    current = directory
    while current != root and root in current.parents:
        try:
            if any(current.iterdir()):
                return
            current.rmdir()
        except OSError:
            # Raced with another writer, or never existed. Either way the
            # directory is not ours to reclaim; the artifacts are gone,
            # which is what the sweep promised.
            return
        current = current.parent
