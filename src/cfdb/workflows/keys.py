"""Key derivation for workflow records and cache entries.

Two key shapes are produced:

- `workflow_key` identifies a single preprocessing+indexing workflow for a
  source file. It is the mutex key shared by `/data` and `/index` — only one
  workflow may exist per (dcc, local_id, md5, pipeline_version) tuple in an
  active (pending/running) state at any time.

- `cache_key` identifies a single cacheable artifact produced by a workflow.
  It is scoped per-artifact-kind so that the data artifact and the index
  artifact land in separate cache entries sharing the same source-file
  lineage.

Keys are content-addressed via `md5` so that an upstream byte change (with
its md5 refreshed in the metadata sync) automatically invalidates cached
artifacts without any explicit purge.
"""

from __future__ import annotations

import re
from typing import Any

from cfdb.workflows.models import ArtifactKind

_MD5_HEX_RE = re.compile(r"^[a-f0-9]{32}$")


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
    processor_version: int,
) -> str:
    """Build the cache key for a single workflow output artifact.

    Args:
        dcc: DCC abbreviation (case-insensitive).
        local_id: The file's local identifier within the DCC.
        artifact_kind: Which artifact kind (data or index) this key
            addresses.
        md5: MD5 hex digest of the upstream file bytes.
        processor_version: Monotonically-increasing version of the processor
            implementation that produced the artifact. Bumping this value
            invalidates cached outputs for the corresponding processor
            without affecting other processors' artifacts.

    Returns:
        A stable string key of the form
        ``{dcc}/{local_id}/{artifact_kind}/{md5}-v{processor_version}``.

    Note:
        ``cache_key`` deliberately does NOT include ``pipeline_version``.
        Bumping ``pipeline_version`` invalidates the in-flight mutex
        (``workflow_key`` changes, so ``claim_workflow`` sees no active
        row for the new key) but does NOT invalidate cached artifacts.
        To force fresh cache entries for a single processor's outputs,
        bump that processor's ``processor_version`` instead.

    Warning:
        The key identifies the processor only by ``processor_version``, not
        by which processor it is. Two processors that claim the same
        ``(file, artifact_kind)`` pair at equal ``processor_version`` derive
        the *same* key and would read back each other's artifacts as cache
        hits -- a wrong answer rather than a miss. This holds today only
        because each pair is claimed by at most one processor, which is a
        property of the current registry and not of this function. Fold a
        processor identity (class name, or a registry-assigned id) into the
        key before landing a second processor for any pair. The paired
        interval formats make this concrete: ``.bedpe`` and ``bigInteract``
        files carry ``index`` artifacts built by ``TabixIntervalProcessor``
        before they were re-typed, so a future paired-interval processor is
        exactly the case that would collide.
    """
    if processor_version < 0:
        raise ValueError("processor_version must be non-negative")
    return (
        f"{normalize_dcc(dcc)}/"
        f"{normalize_local_id(local_id)}/"
        f"{artifact_kind.value}/"
        f"{normalize_md5(md5)}-v{processor_version}"
    )
