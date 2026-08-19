"""Key derivation for workflow records and cache entries.

Two key shapes are produced:

- `workflow_key` identifies a single preprocessing+indexing workflow for a
  source file. It is the mutex key shared by `/data` and `/index` — only one
  workflow may exist per (dcc, local_id, md5, pipeline_version) tuple in an
  active (pending/running) state at any time.

- `cache_key` identifies a single cacheable artifact produced by a workflow.
  It is scoped per-artifact-kind *and* per-producing-processor so that the
  data artifact and the index artifact land in separate cache entries
  sharing the same source-file lineage, and so that two processors claiming
  the same (file, artifact_kind) pair can never read back each other's
  output.

Keys are content-addressed via `md5` so that an upstream byte change (with
its md5 refreshed in the metadata sync) automatically invalidates cached
artifacts without any explicit purge.
"""

from __future__ import annotations

import re
from typing import Any

from cfdb.workflows.models import ArtifactKind

_MD5_HEX_RE = re.compile(r"^[a-f0-9]{32}$")

#: Leaf shape shared by the current and the retired cache-key schemes:
#: the content address followed by the producing processor's version.
_CACHE_LEAF_RE = re.compile(r"^[a-f0-9]{32}-v\d+$")

#: Segment count of the retired (pre-#109) cache key
#: ``{dcc}/{local_id}/{artifact_kind}/{md5}-v{processor_version}``. The
#: current scheme carries a processor-identity segment and so is one
#: longer; see :func:`is_legacy_cache_key`.
_LEGACY_KEY_SEGMENTS = 4

#: Position of the artifact-kind segment in the retired key.
_LEGACY_KIND_INDEX = 2

#: The legal artifact-kind segment values, as strings. Both key schemes
#: place one here, so it is the segment that tells a cache key apart from
#: an unrelated object sharing the bucket.
_ARTIFACT_KIND_VALUES = frozenset(kind.value for kind in ArtifactKind)


def normalize_dcc(dcc: str) -> str:
    """Canonical DCC form used by both ``workflow_key`` and ``cache_key``.

    Stripping whitespace and lower-casing is shared with ``extract_identity``
    so a record's stored ``dcc`` field matches the substring embedded in
    its ``workflow_key``.
    """
    cleaned = dcc.strip().lower()
    if not cleaned:
        raise ValueError("dcc is required for workflow/cache key derivation")
    return cleaned


def normalize_md5(md5: str) -> str:
    """Canonical md5 hex form used by both ``workflow_key`` and ``cache_key``.

    Strips whitespace, lower-cases, then validates the result is a 32-char
    lowercase hex string. A whitespace-only input is rejected (the strip
    runs BEFORE the empty check) so callers cannot collapse the mutex by
    passing ``"   "``.
    """
    cleaned = md5.strip().lower() if md5 else ""
    if not _MD5_HEX_RE.fullmatch(cleaned):
        raise ValueError(f"md5 must be 32 lowercase hex chars; got {md5!r}")
    return cleaned


def normalize_local_id(local_id: str) -> str:
    """Canonical local_id form. Strips whitespace; preserves case.

    Rejects path-separator and null-byte characters so a malformed value
    can't smuggle into cache paths or shell pipelines as a directory
    segment. Case is preserved because upstream DCCs treat local_ids as
    opaque accessions (case-sensitive).
    """
    cleaned = local_id.strip() if local_id else ""
    if not cleaned:
        raise ValueError("local_id is required for workflow/cache key derivation")
    if "/" in cleaned or "\\" in cleaned or "\x00" in cleaned:
        raise ValueError(f"local_id contains forbidden chars: {local_id!r}")
    return cleaned


def normalize_processor_id(processor_id: str) -> str:
    """Canonical processor-identity form embedded in ``cache_key``.

    Strips whitespace and preserves case. Rejects an empty (or
    whitespace-only) value and any path-separator or null-byte character,
    for the same reason ``normalize_local_id`` does: the value becomes a
    path segment in the cache key, and a stray ``/`` would silently
    restructure the key rather than fail.

    Case is preserved because the default identity is a processor's class
    name (see ``Processor.__init_subclass__``), and folding case would
    merge ``BedProcessor`` with a hypothetical ``BEDProcessor``.

    Two further values are rejected, both because of what they would do to
    :func:`is_legacy_cache_key` rather than to the key itself:

    - ``"."`` and ``".."`` traverse a path segment without containing a
      separator. ``cache.py``'s ``_validate_cache_key`` already refuses
      them at ``put`` / ``head`` time, but that surfaces as a failure deep
      inside a workflow; rejecting here fails at derivation instead.
    - A value equal to an :class:`ArtifactKind` would let an
      over-specified purge prefix strip a live key down to something
      shaped exactly like a retired one — the processor id would land in
      the artifact-kind slot and satisfy that segment's check. See
      :func:`is_legacy_cache_key`.
    """
    cleaned = processor_id.strip() if processor_id else ""
    if not cleaned:
        raise ValueError("processor_id is required for cache key derivation")
    if "/" in cleaned or "\\" in cleaned or "\x00" in cleaned:
        raise ValueError(f"processor_id contains forbidden chars: {processor_id!r}")
    if cleaned in (".", ".."):
        raise ValueError(f"processor_id must not be a path traversal: {processor_id!r}")
    if cleaned in _ARTIFACT_KIND_VALUES:
        raise ValueError(
            f"processor_id must not collide with an artifact kind: "
            f"{processor_id!r} (it would make an over-stripped cache key "
            f"indistinguishable from a retired one)"
        )
    return cleaned


def is_legacy_cache_key(key: str) -> bool:
    """Return True when ``key`` was minted under the retired cache scheme.

    The retired scheme (everything written before issue #109) was
    ``{dcc}/{local_id}/{artifact_kind}/{md5}-v{processor_version}`` — four
    segments, with no processor identity. The current scheme inserts that
    identity ahead of the leaf, so a legacy key is a four-segment key
    whose third segment is an artifact kind and whose leaf is a content
    address plus a version.

    Nothing reads legacy keys any more: every lookup goes through
    :func:`cache_key`, which now derives the five-segment form. This
    predicate is the single description of the retired shape, consumed by
    the ``cfdb purge-legacy-cache`` sweep in
    :mod:`cfdb.workflows.purge`.

    Because that sweep deletes what this returns True for, the checks are
    deliberately narrow — a false positive is unrecoverable data loss,
    while a false negative only leaves a stale object behind. Requiring
    the artifact-kind segment is what makes the predicate safe under a
    mis-specified purge prefix: ``purge_s3`` strips the configured prefix
    before testing, so a prefix carrying one segment too many would
    otherwise reduce a *live* five-segment key to a four-segment one and
    delete it. With this check the processor identity lands in the
    artifact-kind slot and fails; stripping two or more segments leaves
    too few to match at all. :func:`normalize_processor_id` forbids an
    identity equal to an artifact kind, closing the remaining overlap.
    """
    segments = key.split("/")
    if len(segments) != _LEGACY_KEY_SEGMENTS:
        return False
    if not all(segments[:-1]):
        return False
    if segments[_LEGACY_KIND_INDEX] not in _ARTIFACT_KIND_VALUES:
        return False
    return bool(_CACHE_LEAF_RE.fullmatch(segments[-1]))


def extract_identity(file_meta: dict[str, Any]) -> tuple[str, str, str]:
    """Pull canonical (dcc, local_id, md5) from a file metadata dict.

    Returns the values in their canonical normalized form: ``dcc``,
    ``local_id``, and ``md5`` are routed through this module's normalizers
    so the substrings embedded in derived keys agree with the values
    callers persist alongside (e.g., on ``JobRecord.dcc`` /
    ``JobRecord.local_id`` / ``JobRecord.md5``). Callers MAY pass the
    returned triple directly into key derivation, JobRecord construction,
    and Mongo lookups without re-normalizing.

    Expects the DCC abbreviation under ``dcc.dcc_abbreviation`` (matches
    the shape served by the ``file`` / ``files`` Mongo documents); falls
    back to a top-level ``submission`` field only when the ``dcc`` key is
    entirely absent (un-enriched documents). Raises ``ValueError`` if any
    required field is missing or malformed.

    The fallback deliberately does NOT fire when ``dcc`` is present but
    malformed — a non-dict ``dcc`` value or a dict missing/empty
    ``dcc_abbreviation`` indicates a buggy producer, not an un-enriched
    doc, and the silent fallback would mask routing surprises (two
    records with conflicting DCC shapes could alias to the same cache
    slot).
    """
    if "dcc" in file_meta:
        dcc_doc = file_meta["dcc"]
        if not isinstance(dcc_doc, dict):
            raise ValueError(
                "file_meta['dcc'] must be a dict; got "
                f"{type(dcc_doc).__name__} — refusing silent fallback to "
                "top-level 'submission' to avoid routing aliases"
            )
        dcc_raw = dcc_doc.get("dcc_abbreviation")
        if not dcc_raw:
            raise ValueError(
                "file_meta['dcc'] is present but 'dcc_abbreviation' is "
                "missing or empty — refusing silent fallback to top-level "
                "'submission' to avoid routing aliases"
            )
    else:
        dcc_raw = file_meta.get("submission")
    local_id_raw = file_meta.get("local_id")
    md5_raw = file_meta.get("md5")
    if not (dcc_raw and local_id_raw and md5_raw):
        raise ValueError(
            "file_meta missing one of dcc.dcc_abbreviation / local_id / md5"
        )
    return (
        normalize_dcc(dcc_raw),
        normalize_local_id(local_id_raw),
        normalize_md5(md5_raw),
    )


def workflow_key(
    dcc: str,
    local_id: str,
    md5: str,
    pipeline_version: int,
) -> str:
    """Build the mutex key for a source file's preprocessing workflow.

    Args:
        dcc: DCC abbreviation (case-insensitive).
        local_id: The file's local identifier within the DCC.
        md5: MD5 hex digest of the upstream file bytes.
        pipeline_version: Monotonically-increasing version of the workflow
            implementation. Bumping this value invalidates all in-flight
            jobs and forces fresh workflows.

    Returns:
        A stable string key of the form
        ``{dcc}/{local_id}/{md5}/v{pipeline_version}``.
    """
    if pipeline_version < 0:
        raise ValueError("pipeline_version must be non-negative")
    return (
        f"{normalize_dcc(dcc)}/"
        f"{normalize_local_id(local_id)}/"
        f"{normalize_md5(md5)}/"
        f"v{pipeline_version}"
    )


def cache_key(
    dcc: str,
    local_id: str,
    artifact_kind: ArtifactKind,
    md5: str,
    processor_id: str,
    processor_version: int,
) -> str:
    """Build the cache key for a single workflow output artifact.

    Args:
        dcc: DCC abbreviation (case-insensitive).
        local_id: The file's local identifier within the DCC.
        artifact_kind: Which artifact kind (data or index) this key
            addresses.
        md5: MD5 hex digest of the upstream file bytes.
        processor_id: Stable identity of the processor that produced the
            artifact (``Processor.processor_id``). Two processors claiming
            the same ``(file, artifact_kind)`` pair derive different keys
            because of this segment, whatever their versions are.
        processor_version: Monotonically-increasing version of the processor
            implementation that produced the artifact. Bumping this value
            invalidates cached outputs for the corresponding processor
            without affecting other processors' artifacts.

    Returns:
        A stable string key of the form
        ``{dcc}/{local_id}/{artifact_kind}/{processor_id}/{md5}-v{processor_version}``.

    Note:
        ``cache_key`` deliberately does NOT include ``pipeline_version``.
        Bumping ``pipeline_version`` invalidates the in-flight mutex
        (``workflow_key`` changes, so ``claim_workflow`` sees no active
        row for the new key) but does NOT invalidate cached artifacts.
        To force fresh cache entries for a single processor's outputs,
        bump that processor's ``processor_version`` instead.

    Note:
        ``processor_id`` is what keeps two processors apart, and it is
        carried here rather than left to the registry because the version
        alone cannot do the job: ``TabixIntervalProcessor`` and
        ``BamIndexProcessor`` both sit at version 2 and stayed apart only
        because their ``supported_formats`` happen to be disjoint (issue
        #109). Keys minted before this segment existed are recognised by
        :func:`is_legacy_cache_key` and swept by ``cfdb
        purge-legacy-cache``; nothing derives or reads them any more.
    """
    if processor_version < 0:
        raise ValueError("processor_version must be non-negative")
    return (
        f"{normalize_dcc(dcc)}/"
        f"{normalize_local_id(local_id)}/"
        f"{artifact_kind.value}/"
        f"{normalize_processor_id(processor_id)}/"
        f"{normalize_md5(md5)}-v{processor_version}"
    )
