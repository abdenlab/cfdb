"""ENCODE metadata TSV client and CFDB transformation service.

Fetches the released-experiment metadata TSV from ENCODE and transforms
each row into a CFDB file document.

Metadata URL
------------
https://www.encodeproject.org/metadata/?type=Experiment&status=released

Field Mapping (ENCODE TSV → CFDB)
----------------------------------

File
~~~~
File accession                  → local_id, accession_id (case-folded)
File download URL               → access_url, filename (derived)
File download URL               → compression_format (suffix-derived; the TSV
                                  carries no compression column, and the field
                                  is omitted when undetermined)
File format                     → file_format (EDAM-mapped)
Output type                     → data_type (EDAM-mapped), output_type
Assay                           → assay_type (OBI-mapped)
Size                            → size_in_bytes
md5sum                          → md5
File Status                     → status
Experiment date released        → creation_time
File accession                  → persistent_id (derived URL)
File assembly                   → genome_assembly
Genome annotation               → genome_annotation
File format type                → output_type_detail
Biological replicate(s)         → biological_replicates
Technical replicate(s)          → technical_replicates

Enriched File
~~~~~~~~~~~~~
Read length                     → extra.encode.read_length
Mapped read length              → extra.encode.mapped_read_length
Run type                        → extra.encode.run_type
Paired end                      → extra.encode.paired_end
Paired with                     → extra.encode.paired_with
Index of                        → extra.encode.index_of
Derived from                    → extra.encode.derived_from
Controlled by                   → extra.encode.controlled_by
s3_uri                          → extra.encode.s3_uri
Azure URL                       → extra.encode.azure_url
File analysis title             → extra.encode.file_analysis_title
File analysis status            → extra.encode.file_analysis_status
Audit WARNING                   → extra.encode.audit_warning
Audit NOT_COMPLIANT             → extra.encode.audit_not_compliant
Audit ERROR                     → extra.encode.audit_error

Collection
~~~~~~~~~~
Experiment accession            → collections[].local_id, name, persistent_id,
                                  accession_id (case-folded)
Lab                             → collections[].lab
Assay                           → collections[].experiment_type
Experiment target               → collections[].experiment_target
Library made from               → collections[].analyte_class

Enriched Collection
~~~~~~~~~~~~~~~~~~~
Project                         → collections[].extra.encode.project
Platform                        → collections[].extra.encode.platform
dbxrefs                         → collections[].extra.encode.dbxrefs
RBNS protein concentration      → collections[].extra.encode.rbns_protein_concentration

Biosample
~~~~~~~~~
Biosample term name             → collections[].biosamples[].local_id
Biosample term id / term name   → collections[].biosamples[].anatomy

Enriched Biosample
~~~~~~~~~~~~~~~~~~
Biosample type                  → …biosamples[].extra.encode.biosample_type
Biosample treatments            → …biosamples[].extra.encode.biosample_treatments
Biosample treatments amount     → …biosamples[].extra.encode.biosample_treatments_amount
Biosample treatments duration   → …biosamples[].extra.encode.biosample_treatments_duration
Biosample genetic mods (*)      → …biosamples[].extra.encode.biosample_genetic_modifications
Library made from               → …biosamples[].extra.encode.library_made_from
Library depleted in             → …biosamples[].extra.encode.library_depleted_in
Library extraction method       → …biosamples[].extra.encode.library_extraction_method
Library lysis method            → …biosamples[].extra.encode.library_lysis_method
Library crosslinking method     → …biosamples[].extra.encode.library_crosslinking_method
Library strand specific         → …biosamples[].extra.encode.library_strand_specific
Library fragmentation method    → …biosamples[].extra.encode.library_fragmentation_method
Library size range              → …biosamples[].extra.encode.library_size_range

(*) Full TSV column: "Biosample genetic modifications methods/categories/
    targets/gene targets/site coordinates/zygosity"

Subject
~~~~~~~
Donor(s)                        → collections[].biosamples[].subjects[].local_id
Biosample organism              → collections[].biosamples[].subjects[].taxonomy

DCC
~~~
Static / config-derived:          dcc.id, dcc.dcc_name, dcc.dcc_abbreviation,
                                  dcc.dcc_description, dcc.contact_email,
                                  dcc.contact_name, dcc.dcc_url,
                                  dcc.project_id_namespace, dcc.project_local_id
"""

import asyncio
import logging
import os
import re
from typing import AsyncGenerator, Optional
from urllib.parse import unquote, urlsplit

import aiohttp

from cfdb.accessions import normalize_accession
from cfdb.dcc_registry import get_dcc_config
from cfdb.services.ontology_mappings import (
    get_assay_type,
    get_data_type,
    get_file_format,
    get_taxonomy,
)

logger = logging.getLogger(__name__)


def _timeout_from_env(name: str, default: int) -> int:
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


#: Environment variable overriding the metadata download budget, and the
#: default applied when it is unset. Resolved per call rather than at import:
#: a malformed value should fail the sync that reads it, not the import of
#: this module, which would take the whole API down over a knob only the
#: sync uses.
_METADATA_TIMEOUT_ENV = "ENCODE_METADATA_TIMEOUT_SECONDS"
_METADATA_TIMEOUT_DEFAULT_SECONDS = 3600


async def fetch_encode_metadata() -> AsyncGenerator[dict, None]:
    """
    Fetch all released experiment files from ENCODE metadata TSV endpoint.

    Uses the /metadata/ endpoint which returns a single TSV file containing
    all matching records, avoiding the need for paginated JSON API calls.
    The response is streamed line-by-line to avoid loading the full TSV
    (hundreds of MB) into memory.

    Yields:
        Dicts keyed by TSV column names for each file row
    """
    config = get_dcc_config("encode")
    api_base = config["api_base"]

    metadata_url = f"{api_base}/metadata/?type=Experiment&status=released"

    headers = {
        "User-Agent": "cfdb/1.0",
    }

    total_fetched = 0

    # Bounds the entire streamed response rather than inactivity: every row is
    # transformed and inserted as it streams, so the wall clock tracks insert
    # throughput against the target database, not network latency. Ten minutes
    # was not enough against DocumentDB -- the sync aborted around 230,000 of
    # ~810,000 rows and, because the DCC is cleared before reloading, left the
    # corpus smaller than it started.
    timeout_seconds = _timeout_from_env(
        _METADATA_TIMEOUT_ENV, _METADATA_TIMEOUT_DEFAULT_SECONDS
    )

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                metadata_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            ) as response:
                if response.status != 200:
                    logger.error(f"ENCODE metadata API error: HTTP {response.status}")
                    raise Exception(
                        f"ENCODE metadata API error: HTTP {response.status}"
                    )

                # Stream line-by-line to keep memory usage constant
                header = None
                async for line_bytes in response.content:
                    line = line_bytes.decode("utf-8").rstrip("\r\n")
                    if not line:
                        continue

                    if header is None:
                        header = line.split("\t")
                        continue

                    fields = line.split("\t")
                    row = dict(zip(header, fields))
                    yield row
                    total_fetched += 1

                    if total_fetched % 50000 == 0:
                        logger.info(
                            f"Parsed {total_fetched} ENCODE metadata rows..."
                        )

                logger.info(
                    f"ENCODE metadata fetch complete: {total_fetched} rows"
                )

        except asyncio.TimeoutError:
            # Re-raised unchanged so the caller still sees a TimeoutError; the
            # log line is the point. Without it the failure is a bare
            # traceback that says nothing about how far the load got, which
            # is the one fact that distinguishes "the budget is too small"
            # from "the endpoint is down".
            logger.error(
                f"ENCODE metadata fetch timed out after {total_fetched} rows "
                f"({timeout_seconds}s budget); the files collection is left "
                f"partially loaded. Raise {_METADATA_TIMEOUT_ENV} and re-run "
                "the sync."
            )
            raise

        except aiohttp.ClientError as e:
            logger.error(f"ENCODE metadata API network error: {e}")
            raise Exception(f"ENCODE metadata API network error: {e}")


def _nonempty(value: str | None) -> str | None:
    """Return None for empty/whitespace strings, otherwise stripped value."""
    if not value or not value.strip():
        return None
    return value.strip()


def _parse_int(value: str | None) -> int | None:
    """Parse a string to int, returning None on failure."""
    v = _nonempty(value)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        return None


# Compression suffix to EDAM CV term ID, verified against OLS4: format:3989 is
# "GZIP format" (extensions gz, gzip), format:3615 is "bgzip" (bgz). Beware
# format:3990, which adjoins them and is "AVI", not the bzip2 term it looks
# like. The ENCODE metadata TSV carries no compression column (verified against
# the live 59-column header), so the filename suffix is the only indicator.
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
# mirroring the value 4DN and HuBMAP carry from their upstream C2M2
# datapackages. It is NOT a claim that the bytes are uncompressed: BAM, bigWig
# and bigBed are internally compressed and carry this value, because their
# file_format names the container rather than its decompressed contents.
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
    value = _nonempty(filename_or_url)
    if value is None:
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


def _extract_donor_ids(donors_str: str | None) -> list[str]:
    """
    Extract donor accession IDs from the Donor(s) TSV field.

    The field contains comma-separated paths like "/human-donors/ENCDO000AAD/".

    Returns:
        List of donor accession IDs (e.g., ["ENCDO000AAD"])
    """
    v = _nonempty(donors_str)
    if v is None:
        return []
    ids = []
    for part in v.split(","):
        part = part.strip()
        # Extract accession from path like /human-donors/ENCDO000AAD/
        match = re.search(r"/([A-Z0-9]+)/?$", part)
        if match:
            ids.append(match.group(1))
        elif part:
            ids.append(part)
    return ids


def transform_to_c2m2(row: dict) -> Optional[dict]:
    """
    Transform an ENCODE metadata TSV row to C2M2-compatible document for MongoDB.

    Args:
        row: Dict keyed by TSV column names

    Returns:
        C2M2-compatible dict for insertion into files collection, or None if invalid
    """
    config = get_dcc_config("encode")
    id_namespace = config["id_namespace"]

    # Required fields
    accession = _nonempty(row.get("File accession"))
    if not accession:
        logger.warning("Skipping ENCODE row without File accession")
        return None

    # File format and access URL
    file_format_raw = _nonempty(row.get("File format")) or ""
    access_url = _nonempty(row.get("File download URL"))

    # Derive filename from access URL or accession
    if access_url:
        filename = access_url.rsplit("/", 1)[-1]
    else:
        filename = f"{accession}.{file_format_raw}" if file_format_raw else accession

    # Map file format to EDAM
    file_format_edam = get_file_format(file_format_raw)

    # Map output_type to EDAM data type
    output_type = _nonempty(row.get("Output type")) or ""
    data_type_edam = get_data_type(output_type)

    # Map assay to OBI assay type
    assay = _nonempty(row.get("Assay")) or ""
    assay_type_obi = get_assay_type(assay)

    # Parse size
    size_in_bytes = _parse_int(row.get("Size"))

    # Build the C2M2-compatible document
    doc = {
        "submission": "encode",
        "id_namespace": id_namespace,
        "local_id": accession,
        # Duplicates local_id for ENCODE, which stores the accession there.
        # The point of the separate field is cross-DCC uniformity: 4DN's
        # local_id is an opaque UUID, so one accession_id input works for
        # both. Folded so it matches what the GraphQL layer folds filters to.
        "accession_id": normalize_accession(accession),
        "filename": filename,
        "size_in_bytes": size_in_bytes,
        "md5": _nonempty(row.get("md5sum")),
        "sha256": None,
        "access_url": access_url,
        "status": _nonempty(row.get("File Status")),
        "data_access_level": "public",
        "creation_time": _nonempty(row.get("Experiment date released")),
        "persistent_id": f"https://www.encodeproject.org/files/{accession}/",
    }

    # Add compression format if it could be determined. Derived from the
    # download URL alone: when the row has no URL, `filename` is synthesized
    # from the accession and cannot carry a compression suffix, so deriving
    # from it would assert "uncompressed" on no evidence. Left absent rather
    # than set to None so an undetermined file does not surface as a null in
    # distinctValues, which the compressionFormat filter cannot select.
    compression_format_edam = derive_compression_format(access_url)
    if compression_format_edam is not None:
        doc["compression_format"] = compression_format_edam

    # Add file format if mapped
    if file_format_edam:
        doc["file_format"] = file_format_edam

    # Add data type if mapped
    if data_type_edam:
        doc["data_type"] = data_type_edam

    # Add assay type if mapped
    if assay_type_obi:
        doc["assay_type"] = assay_type_obi

    # --- Build collections with biosamples and subjects ---
    experiment_accession = _nonempty(row.get("Experiment accession"))
    biosample_term_id = _nonempty(row.get("Biosample term id"))
    biosample_term_name = _nonempty(row.get("Biosample term name"))
    biosample_type = _nonempty(row.get("Biosample type"))
    biosample_organism = _nonempty(row.get("Biosample organism"))
    donors_raw = _nonempty(row.get("Donor(s)"))

    if biosample_term_name:
        # Build anatomy from biosample term
        anatomy = None
        if biosample_term_id:
            anatomy = {"id": biosample_term_id, "name": biosample_term_name}

        # Build subjects from donor(s) and organism
        donor_ids = _extract_donor_ids(donors_raw)
        taxonomy = get_taxonomy(biosample_organism)

        subjects = []
        for donor_id in donor_ids:
            subject = {
                "id_namespace": id_namespace,
                "local_id": donor_id,
                "project_id_namespace": id_namespace,
                "project_local_id": "ENCODE",
            }
            if taxonomy:
                subject["taxonomy"] = taxonomy
            subjects.append(subject)

        # Build biosample extra fields
        biosample_extra = {}
        if biosample_type:
            biosample_extra["biosample_type"] = biosample_type

        # Treatment fields
        treatments = _nonempty(row.get("Biosample treatments"))
        if treatments:
            biosample_extra["biosample_treatments"] = treatments
        treatments_amount = _nonempty(row.get("Biosample treatments amount"))
        if treatments_amount:
            biosample_extra["biosample_treatments_amount"] = treatments_amount
        treatments_duration = _nonempty(row.get("Biosample treatments duration"))
        if treatments_duration:
            biosample_extra["biosample_treatments_duration"] = treatments_duration

        # Genetic modifications (single compound column in TSV)
        genetic_mods = _nonempty(
            row.get(
                "Biosample genetic modifications methods/categories/targets/"
                "gene targets/site coordinates/zygosity"
            )
        )
        if genetic_mods:
            biosample_extra["biosample_genetic_modifications"] = genetic_mods

        # Library metadata on biosample
        _add_extra(biosample_extra, "library_made_from", row.get("Library made from"))
        _add_extra(
            biosample_extra, "library_depleted_in", row.get("Library depleted in")
        )
        _add_extra(
            biosample_extra,
            "library_extraction_method",
            row.get("Library extraction method"),
        )
        _add_extra(
            biosample_extra, "library_lysis_method", row.get("Library lysis method")
        )
        _add_extra(
            biosample_extra,
            "library_crosslinking_method",
            row.get("Library crosslinking method"),
        )
        _add_extra(
            biosample_extra,
            "library_strand_specific",
            row.get("Library strand specific"),
        )
        _add_extra(
            biosample_extra,
            "library_fragmentation_method",
            row.get("Library fragmentation method"),
        )
        _add_extra(
            biosample_extra, "library_size_range", row.get("Library size range")
        )

        # Build biosample
        biosample = {
            "id_namespace": id_namespace,
            "local_id": f"biosample:{biosample_term_name}",
            "project_id_namespace": id_namespace,
            "project_local_id": "ENCODE",
            "subjects": subjects,
        }
        if anatomy:
            biosample["anatomy"] = anatomy
        if biosample_extra:
            biosample["extra"] = {"encode": biosample_extra}

        # Build collection extra (experiment-level fields)
        collection_encode_extra = {}
        _add_extra(collection_encode_extra, "project", row.get("Project"))
        _add_extra(collection_encode_extra, "platform", row.get("Platform"))
        _add_extra(collection_encode_extra, "dbxrefs", row.get("dbxrefs"))
        _add_extra(
            collection_encode_extra,
            "rbns_protein_concentration",
            row.get("RBNS protein concentration"),
        )

        # Build collection — keyed by experiment accession, fallback to biosample
        if experiment_accession:
            collection_local_id = experiment_accession
            collection_name = experiment_accession
            collection_persistent_id = (
                f"https://www.encodeproject.org/experiments/{experiment_accession}/"
            )
        else:
            collection_local_id = f"biosample:{biosample_term_name}"
            collection_name = biosample_term_name
            collection_persistent_id = None

        collection = {
            "id_namespace": id_namespace,
            "local_id": collection_local_id,
            "name": collection_name,
            "biosamples": [biosample],
            "subjects": subjects,
        }
        # Only the experiment-keyed branch has an accession. The
        # ``biosample:``-keyed fallback collection is synthesized locally and
        # names no ENCODE experiment, so it is left unset rather than given a
        # fabricated accession.
        if experiment_accession:
            collection["accession_id"] = normalize_accession(experiment_accession)
        if collection_persistent_id:
            collection["persistent_id"] = collection_persistent_id
        if anatomy:
            collection["anatomy"] = [anatomy]
        # Promote lab to top-level collection field
        lab = _nonempty(row.get("Lab"))
        if lab:
            collection["lab"] = lab
        # Promoted collection-level fields
        if assay:
            collection["experiment_type"] = assay
        _add_extra(collection, "experiment_target", row.get("Experiment target"))
        _add_extra(collection, "analyte_class", row.get("Library made from"))

        if collection_encode_extra:
            collection["extra"] = {"encode": collection_encode_extra}

        doc["collections"] = [collection]
    else:
        doc["collections"] = []

    # --- Promoted file-level fields (top-level) ---
    _add_extra(doc, "genome_assembly", row.get("File assembly"))
    _add_extra(doc, "genome_annotation", row.get("Genome annotation"))
    if output_type:
        doc["output_type"] = output_type
    _add_extra(doc, "output_type_detail", row.get("File format type"))
    _add_extra(doc, "biological_replicates", row.get("Biological replicate(s)"))
    _add_extra(doc, "technical_replicates", row.get("Technical replicate(s)"))

    # --- Build file-level extra dict (DCC-specific fields) ---
    extra = {}

    # Replicate/sequencing metadata
    _add_extra(extra, "read_length", row.get("Read length"))
    _add_extra(extra, "mapped_read_length", row.get("Mapped read length"))
    _add_extra(extra, "run_type", row.get("Run type"))
    _add_extra(extra, "paired_end", row.get("Paired end"))
    _add_extra(extra, "paired_with", row.get("Paired with"))
    _add_extra(extra, "index_of", row.get("Index of"))
    _add_extra(extra, "derived_from", row.get("Derived from"))

    # File-level provenance
    _add_extra(extra, "controlled_by", row.get("Controlled by"))
    _add_extra(extra, "s3_uri", row.get("s3_uri"))
    _add_extra(extra, "azure_url", row.get("Azure URL"))

    # Analysis metadata
    _add_extra(extra, "file_analysis_title", row.get("File analysis title"))
    _add_extra(extra, "file_analysis_status", row.get("File analysis status"))

    # Audit fields
    _add_extra(extra, "audit_warning", row.get("Audit WARNING"))
    _add_extra(extra, "audit_not_compliant", row.get("Audit NOT_COMPLIANT"))
    _add_extra(extra, "audit_error", row.get("Audit ERROR"))

    if extra:
        doc["extra"] = {"encode": extra}

    # Build DCC record inline
    doc["dcc"] = {
        "id": "cfde_registry_dcc:encode",
        "dcc_name": "ENCODE",
        "dcc_abbreviation": "ENCODE",
        "dcc_description": "Encyclopedia of DNA Elements",
        "contact_email": "encode-help@lists.stanford.edu",
        "contact_name": "ENCODE DCC",
        "dcc_url": "https://www.encodeproject.org",
        "project_id_namespace": id_namespace,
        "project_local_id": "ENCODE",
    }

    return doc


def _add_extra(extra: dict, key: str, value: str | None) -> None:
    """Add a non-empty value to the extra dict."""
    v = _nonempty(value)
    if v is not None:
        extra[key] = v


def build_encode_dcc_record() -> dict:
    """
    Build DCC collection record for ENCODE.

    Returns:
        DCC document for insertion into dcc collection
    """
    config = get_dcc_config("encode")
    id_namespace = config["id_namespace"]

    return {
        "submission": "encode",
        "id": "cfde_registry_dcc:encode",
        "dcc_name": "ENCODE",
        "dcc_abbreviation": "ENCODE",
        "dcc_description": "The Encyclopedia of DNA Elements (ENCODE) Consortium is an ongoing international collaboration of research groups funded by the National Human Genome Research Institute (NHGRI). The goal of ENCODE is to build a comprehensive parts list of functional elements in the human genome.",
        "contact_email": "encode-help@lists.stanford.edu",
        "contact_name": "ENCODE DCC",
        "dcc_url": "https://www.encodeproject.org",
        "project_id_namespace": id_namespace,
        "project_local_id": "ENCODE",
    }
