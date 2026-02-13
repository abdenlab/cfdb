# ENCODE Metadata Mapping

Field mapping from the ENCODE metadata TSV to the CFDB data model. ENCODE does not use C2M2 — all data is fetched from the ENCODE REST API and pre-materialized directly into the `files` collection, bypassing the C2M2 load and Rust materializer steps.

## Data Source

| Source | URL | What It Provides |
|--------|-----|------------------|
| ENCODE metadata TSV | `GET /metadata/?type=Experiment&status=released` | Streaming TSV of all released experiment files (~700k rows, hundreds of MB) |

The TSV is streamed line-by-line to keep memory usage constant. Each row represents one file with its experiment, biosample, library, and donor metadata denormalized inline.

## Ontology Mappings

ENCODE uses human-readable strings for file formats, assay types, output types, and organisms. These are mapped to ontology CV terms via lookup tables in `ontology_mappings.py`.

### File Format -> EDAM

40+ mappings from ENCODE `File format` strings to EDAM format CV terms.

| ENCODE Format | EDAM ID | EDAM Name |
|---------------|---------|-----------|
| `fastq` | `format:1930` | FASTQ |
| `bam` | `format:2572` | BAM |
| `cram` | `format:3462` | CRAM |
| `bed` | `format:3003` | BED |
| `narrowPeak` | `format:3613` | NarrowPeak |
| `broadPeak` | `format:3614` | BroadPeak |
| `bigWig` | `format:3006` | bigWig |
| `bigBed` | `format:3004` | bigBed |
| `vcf` | `format:3016` | VCF |
| `gtf` | `format:2306` | GTF |
| `tsv` | `format:3475` | TSV |
| `hdf5` | `format:3590` | HDF5 |

### Output Type -> EDAM Data

70+ mappings from ENCODE `Output type` strings to EDAM data CV terms.

| ENCODE Output Type | EDAM ID | EDAM Name |
|--------------------|---------|-----------|
| `reads` | `data:0924` | Sequence trace |
| `alignments` | `data:0863` | Sequence alignment |
| `peaks` | `data:3002` | Annotation track |
| `signal` | `data:2884` | Plot |
| `gene quantifications` | `data:2603` | Expression data |
| `methylation state at CpG` | `data:1772` | Methylation data |
| `variant calls` | `data:3498` | Sequence variations |
| `contact matrix` | `data:2082` | Matrix |

### Assay -> OBI

80+ mappings from ENCODE `Assay` strings to OBI assay type CV terms.

| ENCODE Assay | OBI ID | OBI Name |
|--------------|--------|----------|
| `ATAC-seq` | `OBI:0002039` | ATAC-seq |
| `DNase-seq` | `OBI:0001853` | DNase-seq |
| `ChIP-seq` | `OBI:0000716` | ChIP-seq |
| `RNA-seq` | `OBI:0001271` | RNA-seq |
| `Hi-C` | `OBI:0002042` | Hi-C |
| `Micro-C` | `OBI:0003102` | Micro-C |
| `WGBS` | `OBI:0001863` | whole genome bisulfite sequencing |
| `CUT&RUN` | `OBI:0003003` | CUT&RUN |
| `CUT&Tag` | `OBI:0003004` | CUT&Tag |
| `eCLIP` | `OBI:0002111` | eCLIP |
| `scRNA-seq` | `OBI:0002631` | single-cell RNA-seq |
| `snATAC-seq` | `OBI:0002762` | snATAC-seq |

### Organism -> NCBI Taxonomy

| ENCODE Organism | NCBI ID | Species |
|-----------------|---------|---------|
| `Homo sapiens` / `human` | `NCBI:txid9606` | Homo sapiens |
| `Mus musculus` / `mouse` | `NCBI:txid10090` | Mus musculus |
| `Drosophila melanogaster` | `NCBI:txid7227` | Drosophila melanogaster |
| `Caenorhabditis elegans` | `NCBI:txid6239` | Caenorhabditis elegans |

## File-Level Fields

### Core C2M2 Fields (top-level on file document)

| CFDB Field | ENCODE TSV Column | Type | Notes |
|------------|-------------------|------|-------|
| `local_id` | `File accession` | string | ENCODE accession (e.g., `ENCFF001ABC`) |
| `id_namespace` | — | string | Constant: `https://www.encodeproject.org` |
| `filename` | `File download URL` | string | Basename extracted from URL |
| `access_url` | `File download URL` | string | Full HTTPS download URL |
| `persistent_id` | `File accession` | string | `https://www.encodeproject.org/files/{accession}/` |
| `size_in_bytes` | `Size` | int | Parsed from string |
| `md5` | `md5sum` | string | |
| `status` | `File Status` | string | e.g., `"released"` |
| `creation_time` | `Experiment date released` | string | ISO date |
| `data_access_level` | — | string | Constant: `"public"` (all released ENCODE files) |
| `file_format` | `File format` | FileFormat | Mapped via `FILE_FORMAT_TO_EDAM` |
| `data_type` | `Output type` | DataType | Mapped via `OUTPUT_TYPE_TO_EDAM` |
| `assay_type` | `Assay` | AssayType | Mapped via `ASSAY_TITLE_TO_OBI` |

### DCC Record (inline on each file)

Set to a constant ENCODE DCC document:

| Field | Value |
|-------|-------|
| `id` | `cfde_registry_dcc:encode` |
| `dcc_name` | `ENCODE` |
| `dcc_abbreviation` | `ENCODE` |
| `dcc_url` | `https://www.encodeproject.org` |

## Collection + Biosample + Subject Construction

ENCODE files don't arrive with C2M2 collection/biosample/subject records. These are synthesized from the TSV's biosample and donor columns and embedded inline on each file document.

### Collection

One collection per unique biosample term, embedded on `file.collections[]`:

| CFDB Field | ENCODE TSV Column | Notes |
|------------|-------------------|-------|
| `local_id` | `Biosample term name` | Prefixed: `biosample:{name}` |
| `name` | `Biosample term name` | e.g., `"GM12878"`, `"heart"` |
| `anatomy[]` | `Biosample term id` + `Biosample term name` | `{id, name}` object |
| `biosamples[]` | — | Single biosample (see below) |
| `subjects[]` | `Donor(s)` | Subject records (see below) |

### Biosample

One biosample per file, nested inside the collection:

| CFDB Field | ENCODE TSV Column | Notes |
|------------|-------------------|-------|
| `local_id` | `Biosample term name` | Prefixed: `biosample:{name}` |
| `anatomy` | `Biosample term id` + `Biosample term name` | `{id, name}` object |
| `subjects[]` | `Donor(s)` | Same subjects as collection |
| `extra.biosample_type` | `Biosample type` | e.g., `"primary cell"`, `"tissue"`, `"cell line"` |
| `extra.biosample_treatments` | `Biosample treatments` | Treatment details |
| `extra.biosample_treatments_amount` | `Biosample treatments amount` | Dosage |
| `extra.biosample_treatments_duration` | `Biosample treatments duration` | Duration |
| `extra.biosample_genetic_modifications` | `Biosample genetic modifications methods/categories/targets/gene targets/site coordinates/zygosity` | Compound column |

### Subject

One subject per donor accession, nested inside collection and biosample:

| CFDB Field | ENCODE TSV Column | Notes |
|------------|-------------------|-------|
| `local_id` | `Donor(s)` | Extracted from path: `/human-donors/ENCDO000AAD/` -> `ENCDO000AAD` |
| `taxonomy` | `Biosample organism` | Mapped via `ORGANISM_TO_NCBI_TAXONOMY` |

## File Extra Fields

All stored on `file.extra` (`EnrichedFile`). Every field is `Optional[str]`.

### File Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.assembly` | `File assembly` |
| `extra.file_type` | `File type` |
| `extra.file_format_type` | `File format type` |
| `extra.output_type` | `Output type` |

### Experiment Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.experiment_accession` | `Experiment accession` |
| `extra.experiment_target` | `Experiment target` |
| `extra.project` | `Project` |

### Library Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.library_made_from` | `Library made from` |
| `extra.library_depleted_in` | `Library depleted in` |
| `extra.library_extraction_method` | `Library extraction method` |
| `extra.library_lysis_method` | `Library lysis method` |
| `extra.library_crosslinking_method` | `Library crosslinking method` |
| `extra.library_strand_specific` | `Library strand specific` |
| `extra.library_fragmentation_method` | `Library fragmentation method` |
| `extra.library_size_range` | `Library size range` |

### Sequencing / Replicate Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.biological_replicates` | `Biological replicate(s)` |
| `extra.technical_replicates` | `Technical replicate(s)` |
| `extra.read_length` | `Read length` |
| `extra.mapped_read_length` | `Mapped read length` |
| `extra.run_type` | `Run type` |
| `extra.paired_end` | `Paired end` |
| `extra.paired_with` | `Paired with` |
| `extra.index_of` | `Index of` |
| `extra.derived_from` | `Derived from` |

### Provenance Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.lab` | `Lab` |
| `extra.platform` | `Platform` |
| `extra.dbxrefs` | `dbxrefs` |
| `extra.genome_annotation` | `Genome annotation` |
| `extra.controlled_by` | `Controlled by` |
| `extra.s3_uri` | `s3_uri` |
| `extra.azure_url` | `Azure URL` |

### Analysis Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.file_analysis_title` | `File analysis title` |
| `extra.file_analysis_status` | `File analysis status` |
| `extra.rbns_protein_concentration` | `RBNS protein concentration` |

### Audit Fields

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.audit_warning` | `Audit WARNING` |
| `extra.audit_not_compliant` | `Audit NOT_COMPLIANT` |
| `extra.audit_error` | `Audit ERROR` |

## Sync Flow

ENCODE sync bypasses the C2M2 ZIP pipeline entirely. Files are pre-materialized during ingest.

| Hook | Phase | Function |
|------|-------|----------|
| Full ingest | Replaces C2M2 load + materialize | `_sync_encode()` |

### Data Flow

```text
fetch_encode_metadata()                 # Streaming TSV from ENCODE API
  │                                     # Yields one dict per row
  │
  └─> transform_to_c2m2(row)            # Per-row transformation
        ├─ Map File format -> EDAM      # ontology_mappings.get_file_format()
        ├─ Map Output type -> EDAM      # ontology_mappings.get_data_type()
        ├─ Map Assay -> OBI             # ontology_mappings.get_assay_type()
        ├─ Map Organism -> NCBI         # ontology_mappings.get_taxonomy()
        ├─ Build collection + biosample + subjects inline
        ├─ Build extra dict (40+ fields)
        └─ Insert into files collection (batches of 1000)
```

No post-ingest enrichment pass. All metadata is captured during the initial TSV transformation.
