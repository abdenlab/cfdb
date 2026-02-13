"""ENCODE metadata TSV client and C2M2 transformation service."""

import logging
import re
from typing import AsyncGenerator, Optional

import aiohttp

from cfdb.dcc_registry import get_dcc_config
from cfdb.services.ontology_mappings import (
    get_assay_type,
    get_data_type,
    get_file_format,
    get_taxonomy,
)

logger = logging.getLogger(__name__)


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

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                metadata_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=600),
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
            biosample["extra"] = biosample_extra

        # Build collection extra (experiment-level fields)
        collection_encode_extra = {}
        _add_extra(
            collection_encode_extra,
            "experiment_target",
            row.get("Experiment target"),
        )
        _add_extra(collection_encode_extra, "project", row.get("Project"))
        _add_extra(collection_encode_extra, "lab", row.get("Lab"))
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
        if collection_persistent_id:
            collection["persistent_id"] = collection_persistent_id
        if anatomy:
            collection["anatomy"] = [anatomy]
        if collection_encode_extra:
            collection["extra"] = {"encode": collection_encode_extra}

        doc["collections"] = [collection]
    else:
        doc["collections"] = []

    # --- Build file-level extra dict ---
    extra = {}

    # File metadata
    _add_extra(extra, "assembly", row.get("File assembly"))
    _add_extra(extra, "file_format_type", row.get("File format type"))
    if output_type:
        extra["output_type"] = output_type

    # Replicate/sequencing metadata
    _add_extra(extra, "biological_replicates", row.get("Biological replicate(s)"))
    _add_extra(extra, "technical_replicates", row.get("Technical replicate(s)"))
    _add_extra(extra, "read_length", row.get("Read length"))
    _add_extra(extra, "mapped_read_length", row.get("Mapped read length"))
    _add_extra(extra, "run_type", row.get("Run type"))
    _add_extra(extra, "paired_end", row.get("Paired end"))
    _add_extra(extra, "paired_with", row.get("Paired with"))
    _add_extra(extra, "index_of", row.get("Index of"))
    _add_extra(extra, "derived_from", row.get("Derived from"))

    # File-level provenance
    _add_extra(extra, "genome_annotation", row.get("Genome annotation"))
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
        doc["extra"] = extra

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
