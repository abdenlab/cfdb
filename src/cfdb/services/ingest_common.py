"""Ingest helpers shared by the REST-API DCC loaders (ENCODE, HuBMAP).

DCC-neutral pieces both loaders need: parsing a timeout budget from the
environment, and deriving a compression term from a filename suffix. They
lived in the ENCODE module until HuBMAP needed them too; keeping them in a
neutral home means a refactor of either loader cannot silently change the
other's behavior.
"""

import os
from urllib.parse import unquote, urlsplit


def timeout_from_env(name: str, default: int) -> int:
    """Parse a positive-int timeout env var, failing loudly at import.

    A bare ``int(os.getenv(...))`` raises a bare ``ValueError`` from deep in
    the import machinery when an operator sets the variable to something
    non-numeric, naming neither the variable nor the value. Zero or negative
    is rejected too: aiohttp treats a non-positive total as "no timeout",
    which would silently turn a misconfiguration into an unbounded request.
    """
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name}={raw!r} is not a valid integer"
        ) from exc
    if parsed <= 0:
        raise ValueError(f"Environment variable {name}={parsed} must be > 0")
    return parsed


# Compression suffix to EDAM CV term ID, verified against OLS4: format:3989 is
# "GZIP format" (extensions gz, gzip), format:3615 is "bgzip" (bgz). Beware
# format:3990, which adjoins them and is "AVI", not the bzip2 term it looks
# like. Neither the ENCODE metadata TSV (verified against its live 59-column
# header) nor the HuBMAP files index carries a compression field, so the
# filename suffix is the only indicator.
#
# Only .gz and .starch actually occur in the released ENCODE corpus; the rest
# are defensive. The suffix cannot distinguish BGZF from plain gzip — ENCODE
# publishes no .bgz and roughly a quarter of its .gz files are BGZF — so
# format:3989 here means "gzip-family stream", not "not bgzip".
COMPRESSION_SUFFIX_TO_EDAM = {
    ".gz": "format:3989",
    ".gzip": "format:3989",
    ".tgz": "format:3989",  # gzip-compressed tar; same bytes as .tar.gz
    ".bgz": "format:3615",
}

# Suffixes that mark a file as compressed but that no EDAM term can express.
# EDAM has no bzip2, xz, zstd or starch term. .zip is here despite having one
# (format:3987) because a ZIP holds many members with many formats, so there is
# no single post-decompression file_format to pair it with, and the tabix
# processor rejects the PK magic outright — calling it a named compression
# would invite a consumer to decompress and feed it onward. .starch is a
# BEDOPS-compressed BED archive whose file_format already maps to plain BED.
#
# These resolve to None rather than to UNCOMPRESSED: they are compressed, so
# claiming otherwise would be worse than admitting we cannot say.
UNMAPPABLE_COMPRESSION_SUFFIXES = (".bz2", ".xz", ".zst", ".zip", ".starch")

# Longest suffix first, so a more specific suffix is never shadowed by a
# shorter one it ends with.
_COMPRESSION_SUFFIXES: tuple[tuple[str, str | None], ...] = tuple(
    sorted(
        list(COMPRESSION_SUFFIX_TO_EDAM.items())
        + [(suffix, None) for suffix in UNMAPPABLE_COMPRESSION_SUFFIXES],
        key=lambda item: len(item[0]),
        reverse=True,
    )
)

# Sentinel for "no compression beyond what file_format already implies",
# mirroring the value 4DN carries from its upstream C2M2 datapackage. It is
# NOT a claim that the bytes are uncompressed: BAM, bigWig and bigBed are
# internally compressed and carry this value, because their file_format
# names the container rather than its decompressed contents.
# Distinct from None, which means no determination was possible at all.
UNCOMPRESSED = ""


def derive_compression_format(filename_or_url: str | None) -> str | None:
    """
    Derive the EDAM compression term ID from a filename or download URL.

    Reports only compression that is *extrinsic* to the declared file format —
    a wrapper the filename reveals and file_format does not. A .bam is not
    reported as compressed even though BGZF underlies it, because its
    file_format already names that container; a .starch is not reported as
    uncompressed, because its file_format names the BED it decompresses to.

    Args:
        filename_or_url: A bare filename or a full download URL.

    Returns:
        The EDAM CV term ID for the compression format (e.g. "format:3989"
        for gzip); "" when the name carries no compression suffix; or None
        when no determination is possible — either because no name was
        available, or because the name ends in a compression suffix no EDAM
        term can express.
    """
    value = (filename_or_url or "").strip()
    if not value:
        return None

    # Reduce to a basename. Query strings and fragments are URL syntax, so they
    # are stripped only when the value actually is a URL — in a bare filename a
    # '?' or '#' is an ordinary character and must not truncate the name.
    if "://" in value:
        basename = unquote(urlsplit(value).path).rsplit("/", 1)[-1]
    else:
        basename = value.rsplit("/", 1)[-1]
    basename = basename.lower()
    if not basename:
        # A directory-style URL carries no evidence either way. Reporting it
        # as uncompressed would be a positive claim about a file we cannot see.
        return None

    for suffix, term_id in _COMPRESSION_SUFFIXES:
        if basename.endswith(suffix):
            return term_id

    return UNCOMPRESSED
