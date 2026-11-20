# HuBMAP Files Search API Dataloader

Field mapping from HuBMAP's file-grain search index to the CFDB data model. HuBMAP is a `rest_api` DCC: `cfdb sync hubmap` builds documents directly from the live index — no C2M2 zip download, no TSV load, no Rust materializer run, and no post-load enrichment or pruning stages. The file query filters on `data_access_level: public` server-side, which is what retired the pruning pass the C2M2 path needed: the index holds only public files today, and the filter keeps that true of the load if the index ever admits others.

## Source Endpoints

| Endpoint | Role | Pagination |
|----------|------|------------|
| `POST https://search.api.hubmapconsortium.org/v3/files/search` | One Elasticsearch document per file; the primary stream | `search_after` on `file_uuid.keyword`, batches of 1000 |
| `POST https://search.api.hubmapconsortium.org/v3/portal/search` | One document per dataset; prefetched once into an in-memory map keyed by `uuid` | `search_after` on `_id`, batches of 1000 |

The files index is ~9.9M documents, 98% of them `.tif` microscopy tiles, so the loader applies a configurable extension allowlist **server-side** (a `terms` filter on `file_extension.keyword`) and the excluded files never cross the wire.

Organ codes need no request: they map through `HUBMAP_ORGAN_TO_UBERON` in `services/ontology_mappings.py`, a checked-in snapshot of the UBKG organs endpoint (`ontology.api.hubmapconsortium.org/organs?application_context=HUBMAP`, taken 2026-11-20) with UBERON labels from OLS4. See [Collection Mapping](#collection-mapping).

Every search page is attempted up to three times, with backoff, on a transport error, a timeout, or a 429/5xx, since re-sending the same `search_after` page is idempotent. A 200 that is partial — failed shards, an upstream `timed_out`, or no hits envelope — is treated the same way rather than walked past, because Elasticsearch answers 200 with whatever the healthy shards returned and the walk would otherwise resume past documents it never received. A failure that outlasts the attempts raises.

Two results are refused outright rather than loaded. A files walk that drains cleanly with no documents raises instead of clearing the slice: a mapping change on the index, or an allowlist that matches nothing, returns a well-formed empty page, and for a submission one stream owns an empty answer is a fault, never a result. And the first files page asks for an exact total (`track_total_hits`), so a walk that ends with fewer documents than the index reported raises. An empty dataset map is refused the same way, before the cutover lock, so the previous slice keeps being served.

## Entity Construction

There is no entity matching. The C2M2 path had to join its TSVs to the Search API through heuristics — collection `persistent_id` against `doi_url`, donor uuid substring-matched inside subject `local_id`, files matched by filename basename within a dataset — and each heuristic was a place for records to fall through. The files index names its parent dataset and donors directly, so every entity is constructed, not matched:

| CFDB Entity | Built from |
|-------------|-----------|
| File | The files-index document itself; `local_id` is `file_uuid` |
| Collection | The file's parent dataset: `local_id` is `dataset_uuid`, one collection per file |
| Subject | The file's `donors[]`: `local_id` is the donor uuid |
| Anatomy | The file's `organs[]`, with UBERON terms from the checked-in organ table |

Dataset-level context the files index does not carry — donor demographics, pipeline, assay metadata, genome assembly — is prefetched from the dataset index (~10k documents) into memory and joined by `dataset_uuid` during transformation, so documents land fully populated in a single insert pass. A dataset missing from that map degrades its files (no demographics, pipeline, or assembly) rather than dropping them.

The `id_namespace` is `https://hubmapconsortium.org` (configured in `dcc_registry.py`), mirroring the ENCODE convention of using the portal URL rather than the C2M2 tag-URI. Every sync clears and rebuilds the HuBMAP slice, so the namespace change has no continuity cost.

## Extension Allowlist

`HUBMAP_FILE_EXTENSIONS` — comma-separated, case-insensitive, leading dot optional (`h5ad, .MTX` and `.h5ad,.mtx` configure the same thing). Unset applies the default below. Set-but-empty skips the HuBMAP sync entirely with a warning and leaves the existing HuBMAP slice in place, the same outcome an empty `ENCODE_ANNOTATION_TYPES` has for ENCODE's annotation phases; draining an empty stream instead would read as an authoritative empty result and delete the slice.

Default: `.h5ad`, `.h5mu`, `.h5`, `.hdf5`, `.gz`, `.mtx`, `.arrow`, `.parquet`, `.bed`, `.narrowpeak`, `.bam`, `.sf`, `.snap`, `.csv`, `.tsv` — 51,816 files as of 2026-11-20, against the 9.7M `.tif` tiles the default excludes. It is not purely genomics, because an outer extension cannot say what a file holds:

- `.csv` (32,396) and `.tsv` (8,809) are mostly imaging-derived tables — roughly 27k of the `.csv` files come from Cell DIVE, CODEX, and MIBI segmentation pipelines.
- `.gz` (5,872) is mostly not genomics either: 2,723 are Cell DIVE `.nii.gz` NIfTI masks and 2,524 are salmon `aux_info/fld.gz`, which map to no `file_format`; about 500 are compressed MTX, BED, or CSV.
- `.bcf` is deliberately **not** in the default. Every `.bcf` in the index (1,851) is a CODEX raw-acquisition sidecar (`raw/Cyc1_reg1/1.bcf`), not binary VCF. `bcf` maps to no format either, so an operator who opts them in gets files with no `file_format` rather than ones labelled as variant calls.

## File Mapping

| CFDB Field | Source | Notes |
|------------|--------|-------|
| `local_id` | `file_uuid` | HuBMAP issues no file accessions; `accession_id` is explicitly null ([#102](https://github.com/abdenlab/cfdb/issues/102)) |
| `filename` | `rel_path` basename | Full path kept on `extra.hubmap.rel_path` |
| `size_in_bytes` | `size` | Stored as int64 — the C2M2 TSV path stored it as a string, which left size filters inert |
| `md5` / `sha256` | `md5_checksum` / `sha256_checksum` | Real per-file checksums; the C2M2 TSVs lacked or mistyped both |
| `file_format` | filename, compression suffix stripped | EDAM-mapped from the inner extension, so `x.fastq.gz` maps to FASTQ rather than nothing |
| `compression_format` | filename suffix | Same derivation and absent-when-undetermined contract as ENCODE |
| `access_url` | `https://assets.hubmapconsortium.org/{dataset_uuid}/{rel_path}` | Path segments percent-encoded. `/data` streams it directly, without DRS resolution. Sampled URLs resolve without authentication, but not every one does: at least one Published CODEX dataset (`HBM366.XKPX.554`) answers 403, some root-level `*-metadata.tsv` files 404, and samples have drawn transient 5xx. `/data` returns those as their own status (403, 404, 502), checked before any byte is sent. The assets host also throttles bursts and can block a client by IP, after which every file answers 403 — so a 403 may mean the client is blocked, not that the file is restricted — and it answers 403 to curl's default user agent, so check reachability by hand with another agent |
| `status` | `dataset_status` | e.g. `"Published"` |
| `data_access_level` | `data_access_level` | `"public"` on every document in this index |
| `genome_assembly` | dataset `ingest_metadata.workflow_description` | Regex-extracted and canonicalized; mirrored on `extra.hubmap.genome_assembly`. See below. |
| `extra.hubmap.is_data_product` | `is_data_product` | The index serializes it as the strings `"True"`/`"False"`; parsed to a bool, and left absent when missing or unrecognized rather than defaulted to `false` |

## Collection Mapping

One collection per file: its parent dataset.

| CFDB Field | Source |
|------------|--------|
| `local_id` | `dataset_uuid` |
| `name` | `dataset_hubmap_id` (e.g. `HBM339.XXWC.842`) |
| `persistent_id` | `https://portal.hubmapconsortium.org/browse/dataset/{dataset_uuid}` |
| `experiment_type` | `dataset_type` |
| `analyte_class` | `analyte_class` |
| `anatomy[]` | `organs[]` — UBERON id and UBERON label from the checked-in organ table by `code`. The name is the UBERON label (`left kidney`), not UBKG's display term (`Kidney (Left)`) or the document's own `label`, which drifts between files for the same code (`Main Bronchus (Right)` vs `Bronchus (Right)`); either would give one id more than one name across files and DCCs |

The table is a static snapshot rather than a per-sync UBKG fetch for two reasons: a fetch that failed would have stripped anatomy from the whole slice for a sync, and UBKG's display terms are not UBERON labels. UBKG's `organ_uberon` is not always UBERON either: the lateral organs (left/right knee, mammary gland, tonsil) carry FMA ids there, so the table maps them to the side-neutral UBERON parent UBKG names under `category` (e.g. `RN` Knee (Right) → `UBERON:0001465` knee), losing the side rather than storing an id that does not resolve in UBERON. `PR` (Prostate) maps to `UBERON:0000079` male reproductive system, UBKG's own mapping, kept verbatim. A code the table does not know — one HuBMAP adds later — is skipped and counted rather than given a fabricated id, and the sync logs the unmapped codes until the table is extended.

### `extra.hubmap` (`EnrichedHubmapCollection`), joined from the dataset index

| CFDB Field | Source | Type |
|------------|--------|------|
| `pipeline` | `pipeline` | string |
| `processing` | `processing` | string |
| `group_name` | `group_name` | string |
| `visualization` | `visualization` | bool |
| `vitessce_hints` | `vitessce-hints` | string[] |
| `metadata` | `metadata` | dict — full assay-specific metadata, stored verbatim |
| `dataset_status` | files-index `dataset_status` | string |

## Subject Mapping

One subject per entry in the file's `donors[]`, with `local_id` the donor uuid and `taxonomy` fixed to Homo sapiens (HuBMAP is human-only). Demographics come from the dataset index, joined by donor uuid: from each dataset's `donor.mapped_metadata` and from every entry of its `donors[].mapped_metadata`, so a donor of a pooled dataset joins even when no dataset names it singly. HuBMAP returns most fields as single-element lists, which are unwrapped to scalars. A file whose dataset or donor is absent from the dataset index still loads, without that context, and the sync counts and logs these join misses. A document too malformed to transform is skipped and counted rather than failing the sync.

Stored on `subjects[].extra.hubmap` (`EnrichedHubmapSubject`):

| CFDB Field | Type | Example |
|------------|------|---------|
| `sex` | string | `"Female"` |
| `race` | string | `"White"` |
| `age_value` / `age_unit` | float / string | `67.0`, `"years"` |
| `height_value` / `height_unit` | float / string | `170.0`, `"cm"` |
| `weight_value` / `weight_unit` | float / string | `80.0`, `"kg"` |
| `body_mass_index_value` / `body_mass_index_unit` | float / string | `28.5`, `"kg/m2"` |
| `cause_of_death` | string | `"Anoxia"` |
| `death_event` | string | `"Cardiac death"` |
| `mechanism_of_injury` | string | `"Blunt injury"` |
| `medical_history` | string[] | `["Diabetes", "Hypertension"]` |
| `social_history` | string[] | `["Smoking"]` |

## Genome Assembly Extraction

The Search API has no structured `genome_assembly` field; assembly is extracted by regex from the dataset's free-text `ingest_metadata.workflow_description`. Recognized patterns and their canonical forms:

| Pattern | Canonical |
|---------|-----------|
| `hg38`, `GRCh38` | `GRCh38` |
| `hg19`, `GRCh37` | `GRCh37` |
| `mm10` | `GRCm38` |
| `GRCm39` | `GRCm39` |

HuBMAP is human-only, so the vast majority resolve to `GRCh38`. If no pattern matches, the field is omitted (not defaulted).

## What's Not Available

| Field | Status | Notes |
|-------|--------|-------|
| `accession_id` | Not issued | HuBMAP has no file accessions; files are keyed by uuid ([#102](https://github.com/abdenlab/cfdb/issues/102)) |
| `creation_time` | Not mapped | The index carries only `last_modified_at`, which is a modification time, not a creation time; mapping it would misstate what the field means |
| Index sidecars (`.bai`, `.tbi`) | Not ingested | Not in the allowlist; `/index` builds indexes for the 92 public `.bam` files and the BED/narrowPeak files through the workflow path |
| Per-file sequencing metrics | Not available | The files index carries checksums and size but no per-file QC |
| Biosamples | Not built | The files index names organs and donors but no sample-level entities; anatomy lands on the collection |

## Data Flow

```text
fetch_dataset_metadata_bulk(deadline)   # dataset index -> {dataset_uuid: context}   (outside the cutover lock)
  │                                     # organ codes map through the checked-in table: no fetch
  └─> [cutover lock]
        fetch_file_metadata(allowlist, deadline)   # files index, filtered server-side
          └─ transform_file_to_cfdb(...)           # one fully-populated document per file
               └─ insert into files                # single pass, deferred stale clear
      ensure_indexes(files)                        # runs even when the stream fails
```

The whole pipeline is `_sync_hubmap` in `services/sync.py`, sharing `_ingest_rows` with the ENCODE path. The prefetch runs before the cutover lock is taken, because the lock gates the whole read surface and the prefetch touches no database state. Every fetch shares one deadline: `HUBMAP_SYNC_TIMEOUT_SECONDS` (default 1800), clamped to end five minutes before the sync lock would turn stale. That clamp matters because the sync lock's one-hour staleness counts from the start of the whole multi-DCC run and HuBMAP runs last, after 4DN and ENCODE, so a budget counted from HuBMAP's own start could outlive the lock and admit a second sync; clamped, the late fetch fails instead, which the deferred clear survives. The stale HuBMAP slice is cleared only once replacement rows are in hand, so an upstream failure before the first row leaves the previous corpus being served rather than empty.

Two costs remain, shared with ENCODE's identical in-lock design and tracked in [#129](https://github.com/abdenlab/cfdb/issues/129). The file stream holds the cutover lock for the roughly two minutes it takes, and every API request for every DCC that waits longer than the 60-second cutover timeout fails, so a HuBMAP sync is a brief read outage across the whole API. And a failure after the first batch still leaves a partial slice — loudly, with indexes ensured — which only a staged load swapped in under a short lock would make atomic.
