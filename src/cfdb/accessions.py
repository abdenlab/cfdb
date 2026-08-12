"""Normalization for the cross-DCC ``accession_id`` field.

``accession_id`` gives every DCC one queryable name for the identifier
users actually recognize -- ``4DNFIMCJXZKH`` for a 4DN file,
``ENCSR918ZSJ`` for an ENCODE experiment -- independent of where each DCC
happens to keep it (4DN buries it in ``persistent_id``; ENCODE stores it
as ``local_id``).

Callers expect that lookup to be case-insensitive, and the usual way to
get that -- a collation-bearing index -- is unavailable here. Amazon
DocumentDB 5.0, which backs the deployed environments, supports neither
the ``Case Insensitive`` index property nor ``cursor.collation()`` (both
land only in DocumentDB 8.0), and a case-insensitive ``$regex`` cannot
use an index at all. A collation-based implementation would also *pass*
against a developer's local MongoDB and fail only once deployed.

So the field is normalized rather than collated: it is stored already
folded to :func:`normalize_accession`'s output, and filter values are
folded the same way at the API boundary, leaving an ordinary indexed
equality match that behaves identically on MongoDB and DocumentDB.

Both sides MUST route through this one function. If the ingest form and
the query form ever diverge, nothing raises -- documents simply become
permanently unmatchable.

Upper case is the fold direction because it is the form both DCCs
already publish, so the stored value stays the display value and no
second field is needed to recover it.
"""

from __future__ import annotations

__all__ = ["normalize_accession"]


def normalize_accession(value: str | None) -> str | None:
    """Fold an accession into its canonical stored/queried form.

    Args:
        value: An accession in any case, optionally surrounded by
            whitespace. ``None`` and blank strings are accepted.

    Returns:
        The accession stripped and upper-cased, or ``None`` when
        ``value`` is ``None``, blank, or whitespace-only. Returning
        ``None`` rather than ``""`` keeps an absent accession out of the
        index and out of ``distinctValues``, matching how the other
        optional model fields treat "no value".
    """
    if value is None:
        return None
    normalized = value.strip().upper()
    return normalized or None
