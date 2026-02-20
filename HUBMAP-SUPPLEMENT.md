# HuBMAP API Enrichment

Field mapping from HuBMAP Search API to the CFDB data model. Enrichment runs during sync as a single bulk Elasticsearch query against `POST /v3/portal/search`, paginated with `search_after` in batches of 1000.

## Entity Matching

| CFDB Entity | Match Key (CFDB) | Match Key (HuBMAP) |
|-------------|------------------|---------------------|
| Collection | `collection.persistent_id` | `doi_url` |
| Subject | `subject.local_id` contains UUID | `donor.uuid` |
| File | `file.filename` (basename) | `files[].rel_path` (basename) |

Collections are the primary join point. One HuBMAP dataset = one C2M2 collection = many files. Subject matching uses the donor UUID embedded in the C2M2 subject `local_id`. File matching joins through the parent collection's DOI, then matches individual files by filename.

## Collection Enrichment

Stored on `collection.extra.hubmap` (`EnrichedHubmapCollection`). Runs pre-materialization.

| CFDB Field | HuBMAP Source Path | Type | Example |
|------------|-------------------|------|---------|
| `dataset_type` | `dataset_type` | string | `"RNAseq"`, `"ATACseq [SnapATAC]"` |
| `pipeline` | `pipeline` | string | `"Salmon"`, `"SnapATAC"` |
| `processing` | `processing` | string | `"raw"`, `"processed"` |
| `group_name` | `group_name` | string | `"Stanford TMC"` |
| `analyte_class` | `analyte_class` | string | `"RNA"`, `"DNA"` |
| `visualization` | `visualization` | bool | `true` |
| `vitessce_hints` | `vitessce-hints` | string[] | `["rna", "is_sc"]` |
| `metadata` | `metadata` | dict | Full assay-specific metadata (~130 keys, varies by assay type) |

### Assay-Specific Metadata Keys

The `metadata` dict is stored verbatim. Common keys for sequencing assays:

| Key | Example |
|-----|---------|
| `acquisition_instrument_model` | `"NovaSeq6000"` |
| `acquisition_instrument_vendor` | `"Illumina"` |
| `sequencing_read_format` | `"150/8/8/150"` |
| `library_layout` | `"paired-end"` |
| `rnaseq_assay_method` | `"NEBNext Ultra II RNA Lib Prep Kit"` |
| `library_average_fragment_size` | `350` |
| `sequencing_read_percent_q30` | `94.27` |

## Subject Enrichment

Stored on `subject.extra.hubmap` (`EnrichedHubmapSubject`). Runs pre-materialization.

Source path is `donor.mapped_metadata.<field>`. HuBMAP returns some fields as single-element lists (e.g., `["Female"]`); these are unwrapped to scalar values during enrichment.

| CFDB Field | HuBMAP Source Key | Type | Example |
|------------|------------------|------|---------|
| `age_value` | `age_value` | float | `67.0` |
| `age_unit` | `age_unit` | string | `"years"` |
| `sex` | `sex` | string | `"Female"` |
| `race` | `race` | string | `"White"` |
| `body_mass_index_value` | `body_mass_index_value` | float | `28.5` |
| `body_mass_index_unit` | `body_mass_index_unit` | string | `"kg/m2"` |
| `cause_of_death` | `cause_of_death` | string | `"Anoxia"` |
| `death_event` | `death_event` | string | `"Cardiac death"` |
| `mechanism_of_injury` | `mechanism_of_injury` | string | `"Blunt injury"` |
| `medical_history` | `medical_history` | string[] | `["Diabetes", "Hypertension"]` |
| `social_history` | `social_history` | string[] | `["Smoking"]` |
| `height_value` | `height_value` | float | `170.0` |
| `height_unit` | `height_unit` | string | `"cm"` |
| `weight_value` | `weight_value` | float | `80.0` |
| `weight_unit` | `weight_unit` | string | `"kg"` |

## File Enrichment

Runs post-materialization against the `files` collection. `data_access_level` is stored at the top level of the file document; other fields go on `file.extra`.

| CFDB Field | HuBMAP Source Path | Type | Notes |
|------------|-------------------|------|-------|
| `data_access_level` | `data_access_level` | string | Top-level field. `"public"` or `"protected"`. Inherited from parent dataset — one value per dataset applies to all its files. |
| `extra.hubmap.genome_assembly` | `ingest_metadata.workflow_description` | string | Regex-extracted. Normalized: `hg38` -> `GRCh38`, `hg19` -> `GRCh37`, `mm10` -> `GRCm38`. Inherited from parent dataset. |
| `extra.hubmap.is_data_product` | `files[].is_data_product` | bool | Per-file, matched by filename basename. |
| `extra.hubmap.rel_path` | `files[].rel_path` | string | Per-file. Enables constructing the assets URL: `https://assets.hubmapconsortium.org/{dataset_uuid}/{rel_path}` |

### Genome Assembly Extraction

The HuBMAP Search API has no structured `genome_assembly` field. Assembly is extracted by regex from the `ingest_metadata.workflow_description` free-text field, which mentions the reference genome in ~851 datasets. Recognized patterns and their canonical forms:

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
| Index files (`.bai`, `.tbi`, `.crai`) | Not available | Raw sequencing data is protected; processed datasets don't retain BAMs. No `/index/hubmap/{id}` route. |
| Per-file sequencing metrics | Not available | `files[]` has `rel_path`, `size`, `type`, `edam_term` but no per-file QC or sequencing metrics. |
| Structured genome assembly | Not available | Only in prose `workflow_description`. See extraction logic above. |

## Sync Hooks

All enrichment uses a single bulk fetch from the Search API. No per-entity API calls.

| Hook | Phase | Function |
|------|-------|----------|
| Collection + Subject enrichment | Pre-materialization | `_enrich_hubmap_collections_and_subjects()` |
| File enrichment | Post-materialization | `_enrich_hubmap_files()` |

### Data Flow

```text
fetch_dataset_metadata_bulk()          # Single bulk Elasticsearch query
  │                                    # Returns dict keyed by doi_url
  │
  ├─> _enrich_hubmap_collections_and_subjects()   [pre-materialization]
  │     ├─ Match collection.persistent_id == doi_url
  │     │  └─ Write EnrichedHubmapCollection -> collection.extra.hubmap
  │     └─ Match subject.local_id contains donor.uuid
  │        └─ Write EnrichedHubmapSubject -> subject.extra.hubmap
  │
  ├─> _materialize_files()                         [Rust materializer]
  │
  └─> _enrich_hubmap_files()                       [post-materialization]
        ├─ Join files -> collections -> doi_url
        ├─ Set data_access_level (top-level)
        └─ Set genome_assembly, is_data_product, rel_path (on extra.hubmap)
```
