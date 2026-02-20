# 4DN API Enrichment

Field mapping from the 4D Nucleome (4DN) Search API and C2M2 datapackage to the CFDB data model. 4DN uses the C2M2 ZIP pipeline (download, extract, load into raw collections) followed by a Rust materializer and two Python API enrichment passes.

## Data Sources

| Source | URL | What It Provides |
|--------|-----|------------------|
| C2M2 ZIP | `cfde-drc.s3.amazonaws.com/4DN/C2M2/` | Raw tables: file, collection, biosample, subject, ontology terms, junction tables |
| 4DN Search API | `https://data.4dnucleome.org/search/` | File metadata (genome_assembly, file_type, extra_files), experiment metadata, biosource tiers |

## Entity Matching

| CFDB Entity | Match Key (CFDB) | Match Key (4DN API) |
|-------------|------------------|---------------------|
| File | `persistent_id` contains `4DNF*` | `accession` (e.g., `4DNFI1234ABC`) |
| Collection | `persistent_id` contains `4DNEX*` or `4DNES*` | `accession` (e.g., `4DNEXH4ZUIH6`) |
| Biosource tier | `extra.fourdn.biosource_name` | `Biosource.display_title` |

Accessions are extracted from persistent_id URLs via regex: `4DNF[A-Z0-9]+` for files, `4DNE[A-Z][A-Z0-9]+` for experiments.

## Materialization (Rust)

The Rust materializer (`materialize/src/main.rs`) denormalizes raw C2M2 tables into a single `files` collection. This runs for all DCCs, not just 4DN.

### C2M2 Table Joins

The materializer builds the full entity graph for each file:

```text
file
├── dcc                            ── via submission field
├── project                        ── via project FK (id_namespace, local_id)
├── file_format (FileFormat)       ── via file_format ID -> EDAM lookup
├── data_type (DataType)           ── via data_type ID -> EDAM lookup
├── assay_type (AssayType)         ── via assay_type ID -> OBI lookup
└── collections[]                  ── via file_in_collection junction
    ├── anatomy[]                  ── via collection_anatomy junction -> UBERON lookup
    ├── subjects[]                 ── via subject_in_collection junction
    │   ├── race[]                 ── via subject_race junction
    │   └── taxonomy               ── via subject_role_taxonomy -> NCBI lookup
    └── biosamples[]               ── via biosample_in_collection junction
        ├── anatomy                ── via anatomy ID -> UBERON lookup
        └── subjects[]             ── via biosample_from_subject junction
            ├── age_at_sampling    ── from junction table field
            ├── race[]             ── via subject_race junction
            └── taxonomy           ── via subject_role_taxonomy -> NCBI lookup
```

### File Format Enrichment

For files with ambiguous container formats (HDF5, plain text) or missing `file_format`, the materializer derives a specific format from the filename extension. Stored on `extra.fourdn.enriched_file_format`.

| Extension | Enriched Format | Triggered When |
|-----------|----------------|----------------|
| `.mcool` | `mcool` | file_format is empty, `format:3590` (HDF5), or `format:2330` (plain text) |
| `.cool` | `cool` | same |
| `.hic` | `hic` | same |
| `.pairs`, `.pairs.gz` | `pairs` | same |
| `.r3d` | `r3d` | same |
| `.nd2` | `nd2` | same |
| `.flex` | `flex` | same |
| `.spt` | `spt` | same |
| `.matrix` | `matrix` | same |

## Collection Enrichment (Pre-Materialization)

Stored on `collection.extra.fourdn` (`EnrichedFourdnCollection`), with `lab` promoted to `collection.lab`. Fetched from 4DN Search API by querying `ExperimentHiC`, `ExperimentSeq`, `ExperimentDamid`, and `ExperimentChiapet` types. Paginated at 1000 records/page with 100ms rate limiting.

### Direct Fields

| CFDB Field | 4DN API Path | Type | Example |
|------------|-------------|------|---------|
| `display_title` | `display_title` | string | `"in situ Hi-C on GM12878"` |
| `status` | `status` | string | `"released"` |
| `date_created` | `date_created` | string | `"2017-04-14T12:58:19.000000+00:00"` |

### Object Fields (display_title extracted)

These API fields return objects; the materializer extracts `display_title`:

| CFDB Field | 4DN API Path | Type | Example |
|------------|-------------|------|---------|
| `experiment_type` | `experiment_type.display_title` | string | `"in situ Hi-C"` |
| `digestion_enzyme` | `digestion_enzyme.display_title` | string | `"DpnII"` |

`lab` is extracted from `lab.display_title` and promoted to the top-level `collection.lab` field (e.g., `"Erez Lieberman Aiden, Baylor"`).

### Array Field

| CFDB Field | 4DN API Path | Type | Example |
|------------|-------------|------|---------|
| `targeted_factor` | `targeted_factor[].display_title` | string[] | `["CTCF protein"]` |

### Protocol Scalar Fields

All stored as `Optional[str]`:

| CFDB Field | 4DN API Path | Description |
|------------|-------------|-------------|
| `crosslinking_method` | `crosslinking_method` | e.g., `"1% Formaldehyde"` |
| `crosslinking_temperature` | `crosslinking_temperature` | e.g., `"25"` |
| `crosslinking_time` | `crosslinking_time` | e.g., `"10 min"` |
| `ligation_temperature` | `ligation_temperature` | e.g., `"25"` |
| `ligation_volume` | `ligation_volume` | e.g., `"1.2 mL"` |
| `ligation_time` | `ligation_time` | e.g., `"4 hours"` |
| `digestion_temperature` | `digestion_temperature` | e.g., `"37"` |
| `digestion_time` | `digestion_time` | e.g., `"overnight"` |
| `tagging_method` | `tagging_method` | Transposase tagging details |
| `fragmentation_method` | `fragmentation_method` | e.g., `"sonication"` |
| `biotin_removed` | `biotin_removed` | Biotin removal info |
| `library_prep_kit` | `library_prep_kit` | e.g., `"NEBNext Ultra II"` |
| `average_fragment_size` | `average_fragment_size` | e.g., `"300"` |
| `fragment_size_range` | `fragment_size_range` | e.g., `"200-400"` |

## File Enrichment (Post-Materialization)

Stored on `file.extra.fourdn` (`EnrichedFourdnFile`). Fetched from 4DN Search API by querying `FileProcessed` and `FileFastq` types. Paginated at 1000 records/page with 100ms rate limiting.

### Direct File Fields

| CFDB Field | 4DN API Path | Type | Example |
|------------|-------------|------|---------|
| `extra.fourdn.genome_assembly` | `genome_assembly` | string | `"GRCh38"` |
| `extra.fourdn.file_type` | `file_type` | string | `"contact matrix"` |
| `extra.fourdn.file_type_detailed` | `file_type_detailed` | string | `"contact matrix (mcool)"` |

### Track and Facet Info Fields

Extracted from `track_and_facet_info` sub-object:

| CFDB Field | 4DN API Path | Type | Example |
|------------|-------------|------|---------|
| `extra.fourdn.condition` | `track_and_facet_info.condition` | string | `"untreated"` |
| `extra.fourdn.biosource_name` | `track_and_facet_info.biosource_name` | string | `"GM12878"` |
| `extra.fourdn.dataset` | `track_and_facet_info.dataset` | string | `"Rao et al. (2014)"` |
| `extra.fourdn.experiment_type` | `track_and_facet_info.experiment_type` | string | `"in situ Hi-C"` |
| `extra.fourdn.assay_info` | `track_and_facet_info.assay_info` | string | `"DpnII, bio"` |
| `extra.fourdn.replicate_info` | `track_and_facet_info.replicate_info` | string | `"Biorep 1, Techrep 1"` |

### Derived Field

| CFDB Field | Source | Type | Notes |
|------------|--------|------|-------|
| `extra.fourdn.cell_line_tier` | `Biosource` type query | string | Derived by looking up `biosource_name` in a separate Biosource tier query. Values: `"Tier 1"`, `"Tier 2"`. ~17 classified cell lines. |

### Index Files

Stored on `extra.fourdn.extra_files` as `ExtraFile[]`:

| CFDB Field | 4DN API Path | Type | Example |
|------------|-------------|------|---------|
| `href` | `extra_files[].href` | string | `"/files-processed/4DNFI.../@@download/4DNFI....px2"` |
| `md5sum` | `extra_files[].md5sum` | string | `"d41d8cd98f00b204e9800998ecf8427e"` |
| `file_size` | `extra_files[].file_size` | int | `1048576` |
| `file_format` | `extra_files[].file_format` | string | `"pairs_px2"`, `"bai"` |

These index files are served via `GET /index/4dn/{local_id}`.

## Sync Hooks

| Hook | Phase | Function |
|------|-------|----------|
| Collection enrichment | Pre-materialization | `_enrich_4dn_collections()` |
| File + biosource enrichment | Post-materialization | `_enrich_4dn_api_metadata()` |

### Data Flow

```text
C2M2 ZIP (download + extract)
  │
  ├─> Load raw tables into MongoDB (file, collection, biosample, subject, junctions, ontologies)
  │     └─ Mark all 4DN files: data_access_level = "public"
  │
  ├─> _enrich_4dn_collections()                    [pre-materialization]
  │     ├─ fetch_experiment_metadata_bulk()
  │     │    └─ Query ExperimentHiC, ExperimentSeq, ExperimentDamid, ExperimentChiapet
  │     └─ Match collection.persistent_id -> 4DNEX* accession
  │        ├─ Write EnrichedFourdnCollection -> collection.extra.fourdn
  │        └─ Promote lab -> collection.lab
  │
  ├─> _materialize_files()                          [Rust materializer]
  │     ├─ Denormalize all C2M2 joins into files collection
  │     └─ Detect enriched_file_format from filename extensions
  │
  └─> _enrich_4dn_api_metadata()                    [post-materialization]
        ├─ fetch_file_metadata_bulk()
        │    └─ Query FileProcessed, FileFastq
        ├─ fetch_biosource_tiers()
        │    └─ Query Biosource (Tier 1, Tier 2)
        └─ Match files.persistent_id -> 4DNF* accession
           └─ Write genome_assembly, file_type, extra_files, cell_line_tier -> files.extra.fourdn
```
