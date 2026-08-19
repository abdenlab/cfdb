# ENCODE Metadata Mapping

Field mapping from the ENCODE metadata TSVs to the CFDB data model. ENCODE does not use C2M2 — all data is fetched from the ENCODE REST API and pre-materialized directly into the `files` collection, bypassing the C2M2 load and Rust materializer steps.

Two TSVs are ingested: released **experiments** and released **annotations** of configured types. They share most of their mapping; the sections below describe the experiment TSV, and [Annotation Mapping](#annotation-mapping) states every way an annotation row differs.

## Data Source

| Source | URL | What It Provides |
|--------|-----|------------------|
| ENCODE experiment metadata TSV | `GET /metadata/?type=Experiment&status=released` | Streaming TSV of all released experiment files (~700k rows, hundreds of MB), 59 columns |
| ENCODE annotation metadata TSV | `GET /metadata/?type=Annotation&status=released&annotation_type=<type>` | Streaming TSV of all released files of one annotation type, 32 columns. Requested once per configured type |

The TSV is streamed line-by-line to keep memory usage constant. Each row represents one file with its experiment/dataset, biosample, library, and donor metadata denormalized inline.

### Which annotation types are ingested

`ENCODE_ANNOTATION_TYPES` — comma-separated `annotation_type` values. Unset yields the default allowlist below; set to an empty value to disable annotation ingest entirely, which logs a warning since `ENCODE_METADATA_TIMEOUT_SECONDS` reads an empty value the other way, as "unset, use the default". Entries are trimmed, blanks dropped, and repeats collapsed — one ingest phase runs per distinct entry, and since ENCODE files are written with `insert_many` into a collection with no unique key, a duplicated token would otherwise load every file of that type twice.

| `annotation_type` | Datasets | Files | Formats | Assemblies |
|---|---|---|---|---|
| `candidate Cis-Regulatory Elements` | 6,230 | 12,448 | `bed bed9+`, `bigBed bed9+`, `bed bed3+`, `bigBed bed3+`, `bigBed bed9` | GRCh38, mm10, hg19 |
| `element gene regulatory interaction predictions` | 1,543 | 16,692 | `bed bed3+`, `bedpe`, `bigInteract`, `bed bed3` | GRCh38 |

This is an allowlist rather than a filter applied after the fact. ENCODE publishes 580,910 annotation datasets, 86% of them footprints; ingesting the type space wholesale and pruning later would be a corpus-scale mistake to undo.

One request per type, not one request carrying repeated `annotation_type` parameters: a failure or timeout on one type then costs only that type, and each response stays small enough for the portal to assemble without a gateway timeout.

## Ontology Mappings

ENCODE uses human-readable strings for file formats, assay types, output types, and organisms. These are mapped to ontology CV terms via lookup tables in `ontology_mappings.py`.

### File Format -> EDAM

38 mappings from ENCODE `File format` strings to EDAM format CV terms.

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
| `bedpe` | `cfdb:bedpe` | bedpe |
| `bigInteract` | `cfdb:biginteract` | bigInteract |
| `vcf` | `format:3016` | VCF |
| `gtf` | `format:2306` | GTF |
| `tsv` | `format:3475` | TSV |
| `hdf5` | `format:3590` | HDF5 |

#### Minted (non-EDAM) format terms

`cfdb:` marks a term minted here because EDAM has none (verified against OLS4). The alternative — aliasing an unrepresented format onto the nearest EDAM term — is what produced the starch/BED conflation behind #69 and #72: the format becomes indistinguishable from the one it was aliased to, and the processor claiming that term picks it up and mangles it.

`bedpe` and `bigInteract` both pair two loci per record. Routed into the BED tabix pipeline they would be sorted and indexed by the first locus alone, committing a cached artifact that looks successful and is wrong — and the byte-sniff guard from #71 does not catch it, since both are plaintext or gzip. The distinct **name** is what does the work: processor lookup keys on `file_format.name`, and neither name is in any processor's `supported_formats`, so `GET /data/...` streams the raw upstream file rather than a mangled index. Tileset endpoints that understand these formats are planned separately.

**`bigInteract` loses a working index in the meantime.** The two formats were not equally broken before this change. A `bedpe` routed through tabix produced an index that was simply wrong — the second mate of every record was unindexed and invisible. A `bigInteract` routed through `bigBedToBed` produced one that was *range-coherent*: the leading three columns are a genuine interval, so the index answered range queries correctly while silently degrading each interaction to a single locus. Re-typing both means `GET /index/...` now returns 404 for either, so `bigInteract` files that previously returned a usable-if-degraded index return nothing at all, and that applies to `bigInteract` already in the experiment corpus as well as to newly ingested annotation files. The tradeoff is accepted deliberately: a 404 states the truth, where the degraded index quietly answered a question the caller did not ask. The tileset work is what restores the capability.

This applies to `.bedpe` already in the ENCODE experiment corpus, not only to annotation files — but it reaches no further than ENCODE. `FILE_FORMAT_TO_EDAM` and `get_file_format` have exactly one consumer, the ENCODE transform; 4DN and HuBMAP take `file_format` from their upstream C2M2 datapackage or portal API and never consult the table. A 4DN `.bedpe` whose upstream declares BED therefore still carries `file_format.name == "BED"`, is still claimed by `TabixIntervalProcessor`, and still gets an index built from its first mate. Closing that means routing incoming formats from every DCC through the same table, or refusing a `.bedpe` filename at the processor regardless of declared format; both are follow-up work.

**Cache artifacts left behind.** Files of these two formats that were indexed before the re-typing still have their incorrect `.tbi` artifacts in the workflow cache. Nothing reads them — `lookup_for` returns `None` and the router bails before probing the cache. The hazard they posed is closed: `cache_key` used to identify a processor only by its `processor_version`, so a future paired-interval processor sharing a version number with `TabixIntervalProcessor` would have derived the same key and read those stale artifacts back as cache hits. Issue #109 folded the producing processor's identity into the key, so that collision is now impossible rather than merely unlikely, and the stranded artifacts — which are keyed under the retired scheme — are swept by `cfdb purge-legacy-cache`.

### Output Type -> EDAM Data

53 mappings from ENCODE `Output type` strings to EDAM data CV terms.

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
| `candidate Cis-Regulatory Elements` | `data:1255` | Sequence features |
| `elements reference` | `data:1255` | Sequence features |
| `element gene links` | `data:0006` | Data |
| `thresholded element gene links` | `data:0006` | Data |
| `thresholded links` | `data:0006` | Data |

The last five are the complete `Output type` domain of the two ingested annotation types, verified against the live TSVs. Without them every annotation file would carry `data_type: null`. The element/gene link types resolve to the generic `data:0006` because EDAM has no term for a predicted regulatory relationship between two loci — the pre-existing `chromatin interactions` entry already made that call the same way.

### Assay -> OBI

63 mappings from ENCODE `Assay` strings to OBI assay type CV terms.

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
| `accession_id` | `File accession` | string | The same accession, case-folded to upper case. Duplicates `local_id` for ENCODE, which stores the accession there; the separate field exists for cross-DCC uniformity, since 4DN's `local_id` is an opaque UUID. Folded so an `accessionId` filter matches in any casing, which means it can legitimately differ from `local_id` in case. |
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
| `compression_format` | `File download URL` | string | EDAM term ID derived from the URL's filename suffix via `COMPRESSION_SUFFIX_TO_EDAM`; `""` when no compression suffix is present; **omitted** when compressed in a format no EDAM term names (`UNMAPPABLE_COMPRESSION_SUFFIXES`) or when the URL carries no filename. Derived from the URL only — never from the accession-synthesized `filename`, which cannot carry a suffix. `format:3989` means gzip-family and does not exclude BGZF. The TSV has no compression column. |
| `data_type` | `Output type` | DataType | Mapped via `OUTPUT_TYPE_TO_EDAM` |
| `assay_type` | `Assay` | AssayType | Mapped via `ASSAY_TITLE_TO_OBI` |

### DCC Record (inline on each file)

Set to a constant ENCODE DCC document:

| Field | Value |
|-------|-------|
| `id` | `cfde_registry_dcc:encode` |
| `dcc_name` | `ENCODE` |
| `dcc_abbreviation` | `ENCODE` |
| `dcc_description` | `"The Encyclopedia of DNA Elements (ENCODE) Consortium..."` |
| `contact_email` | `encode-help@lists.stanford.edu` |
| `contact_name` | `ENCODE DCC` |
| `dcc_url` | `https://www.encodeproject.org` |
| `project_id_namespace` | `https://www.encodeproject.org` |
| `project_local_id` | `ENCODE` |

## Collection + Biosample + Subject Construction

ENCODE files don't arrive with C2M2 collection/biosample/subject records. These are synthesized from the TSV's experiment, biosample, and donor columns and embedded inline on each file document.

### Collection

One collection per unique experiment accession, embedded on `file.collections[]`:

| CFDB Field | ENCODE TSV Column | Notes |
|------------|-------------------|-------|
| `local_id` | `Experiment accession` | e.g., `"ENCSR000AAA"` |
| `accession_id` | `Experiment accession` | The same accession, case-folded to upper case |
| `name` | `Experiment accession` | Same as `local_id` |
| `persistent_id` | `Experiment accession` | `https://www.encodeproject.org/experiments/{accession}/` |
| `anatomy[]` | `Biosample term id` + `Biosample term name` | `{id, name}` object |
| `biosamples[]` | — | Single biosample (see below) |
| `subjects[]` | `Donor(s)` | Subject records (see below) |
| `extra.encode` | — | Experiment-level metadata (see below) |

**Fallback**: if `Experiment accession` is missing, falls back to biosample-keyed collection (`biosample:{name}`). That fallback collection is synthesized locally and names no ENCODE experiment, so it carries no `accession_id` rather than a fabricated one. Note also that the whole collection block is gated on `Biosample term name`: a row with an experiment accession but no biosample term produces no collection at all, so that experiment's accession is queryable nowhere.

#### Collection Lab (top-level)

`lab` is promoted to a top-level `Collection` field (stored on `collection.lab`), sourced from the `Lab` TSV column (e.g., `"Bradley Bernstein, Broad"`).

#### Collection Extra (`extra.encode`)

Experiment-level fields stored on `collection.extra.encode` (`EnrichedEncodeCollection`):

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.encode.experiment_target` | `Experiment target` |
| `extra.encode.project` | `Project` |
| `extra.encode.platform` | `Platform` |
| `extra.encode.dbxrefs` | `dbxrefs` |
| `extra.encode.rbns_protein_concentration` | `RBNS protein concentration` |
| `extra.encode.annotation_type` | `Annotation type` (annotation TSV only) |
| `extra.encode.software_used` | `Software used` (annotation TSV only) |
| `extra.encode.encyclopedia_version` | `Encyclopedia Version` (annotation TSV only) |

### Biosample

One biosample per file, nested inside the collection:

| CFDB Field | ENCODE TSV Column | Notes |
|------------|-------------------|-------|
| `local_id` | `Biosample term name` | Prefixed: `biosample:{name}` |
| `anatomy` | `Biosample term id` + `Biosample term name` | `{id, name}` object |
| `subjects[]` | `Donor(s)` | Same subjects as collection |
| `extra.encode.biosample_type` | `Biosample type` | e.g., `"primary cell"`, `"tissue"`, `"cell line"` |
| `extra.encode.life_stage` | `Life stage` | Annotation TSV only. e.g., `"embryonic"`, `"adult"`, `"young adult"`, `"unknown"` |
| `extra.encode.age` | `Age` | Annotation TSV only. Kept as the upstream string — the released corpus contains `"2-4"` and `"unknown"` alongside decimals, and distinguishes `"10.5"` from `"10.50"` |
| `extra.encode.age_units` | `Age units` | Annotation TSV only. `year` / `month` / `week` / `day`; blank when `age` is absent or a sentinel |
| `extra.encode.biosample_treatments` | `Biosample treatments` | Treatment details |
| `extra.encode.biosample_treatments_amount` | `Biosample treatments amount` | Dosage |
| `extra.encode.biosample_treatments_duration` | `Biosample treatments duration` | Duration |
| `extra.encode.biosample_genetic_modifications` | `Biosample genetic modifications methods/categories/targets/gene targets/site coordinates/zygosity` | Compound column |
| `extra.encode.library_made_from` | `Library made from` | e.g., `"RNA"`, `"DNA"` |
| `extra.encode.library_depleted_in` | `Library depleted in` | e.g., `"rRNA"` |
| `extra.encode.library_extraction_method` | `Library extraction method` | |
| `extra.encode.library_lysis_method` | `Library lysis method` | |
| `extra.encode.library_crosslinking_method` | `Library crosslinking method` | |
| `extra.encode.library_strand_specific` | `Library strand specific` | |
| `extra.encode.library_fragmentation_method` | `Library fragmentation method` | |
| `extra.encode.library_size_range` | `Library size range` | |

### Subject

One subject per donor accession, nested inside collection and biosample:

| CFDB Field | ENCODE TSV Column | Notes |
|------------|-------------------|-------|
| `local_id` | `Donor(s)` | Extracted from path: `/human-donors/ENCDO000AAD/` -> `ENCDO000AAD` |
| `taxonomy` | `Biosample organism` | Mapped via `ORGANISM_TO_NCBI_TAXONOMY` |

## File Extra Fields

All stored on `file.extra.encode` (`EnrichedEncodeFile`). Every field is `Optional[str]`. Only file-scoped fields remain here — experiment-level and library-level fields have been moved to `collection.extra.encode` and `biosample.extra.encode` respectively.

### File Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.encode.assembly` | `File assembly` |
| `extra.encode.file_format_type` | `File format type` |
| `extra.encode.output_type` | `Output type` |

### Sequencing / Replicate Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.encode.biological_replicates` | `Biological replicate(s)` |
| `extra.encode.technical_replicates` | `Technical replicate(s)` |
| `extra.encode.read_length` | `Read length` |
| `extra.encode.mapped_read_length` | `Mapped read length` |
| `extra.encode.run_type` | `Run type` |
| `extra.encode.paired_end` | `Paired end` |
| `extra.encode.paired_with` | `Paired with` |
| `extra.encode.index_of` | `Index of` |
| `extra.encode.derived_from` | `Derived from` |

### Provenance / Access Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.encode.genome_annotation` | `Genome annotation` |
| `extra.encode.controlled_by` | `Controlled by` |
| `extra.encode.s3_uri` | `s3_uri` (annotation TSV: `S3 URL`) |
| `extra.encode.azure_url` | `Azure URL` |
| `extra.encode.organism` | `Biosample organism` (annotation TSV: `Organism`) |
| `extra.encode.annotation_type` | `Annotation type` (annotation TSV only) |

### Analysis Metadata

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.encode.file_analysis_title` | `File analysis title` |
| `extra.encode.file_analysis_status` | `File analysis status` |

### Audit Fields

| CFDB Field | ENCODE TSV Column |
|------------|-------------------|
| `extra.encode.audit_warning` | `Audit WARNING` |
| `extra.encode.audit_not_compliant` | `Audit NOT_COMPLIANT` |
| `extra.encode.audit_error` | `Audit ERROR` |

## Annotation Mapping

Everything above applies to an annotation row too, except as stated here. `transform_annotation_to_c2m2` renames the annotation TSV's columns to their experiment equivalents, runs the shared transformation, then applies the annotation-only columns — so one mapping serves both TSVs.

### Renamed columns

Applied via `ANNOTATION_COLUMN_ALIASES` in `encode.py`. Verified against the live headers of both ingested types (both 32 columns, identical to each other). Every other shared column already agrees by name.

| Annotation TSV | Experiment TSV | Lands on |
|---|---|---|
| `Dataset accession` | `Experiment accession` | `collections[].local_id`, `accession_id`, `name`, `persistent_id` |
| `Assay term name` | `Assay` | `assay_type`, `collections[].experiment_type` |
| `Assembly` | `File assembly` | `genome_assembly` |
| `Dataset date released` | `Experiment date released` | `creation_time` |
| `S3 URL` | `s3_uri` | `extra.encode.s3_uri` |
| `Organism` | `Biosample organism` | `extra.encode.organism`, `subjects[].taxonomy` |

### Annotation-only columns

| Annotation TSV | Lands on |
|---|---|
| `Annotation type` | `extra.encode.annotation_type` **and** `collections[].extra.encode.annotation_type` |
| `Software used` | `collections[].extra.encode.software_used` |
| `Encyclopedia Version` | `collections[].extra.encode.encyclopedia_version` |
| `Targets` | `collections[].experiment_target` |
| `Life stage` | `…biosamples[].extra.encode.life_stage`, when the row names a biosample |
| `Age` | `…biosamples[].extra.encode.age`, when the row names a biosample |
| `Age units` | `…biosamples[].extra.encode.age_units`, when the row names a biosample |

`annotation_type` is stored on the file as well as the dataset. The dataset is where the property belongs, but filtering *files* by it is the actual use case — "give me the cCRE files" — and routing that through a collection subdocument on every query is not worth avoiding one duplicated string.

`Targets` reuses the scalar `experiment_target` rather than adding a parallel list field; the TSV hands over a string either way. Note that ENCODE does not order it — both `"CTCF-human, H3K4me3-human"` and `"H3K4me3-human, CTCF-human"` occur in the released corpus — so an equality filter on the whole value matches one permutation only.

`Life stage` / `Age` / `Age units` go to the biosample, not to `Subject.age_at_sampling`. They are biosample-scoped in ENCODE's own model (resolved from the donor upstream), which is why the annotation TSV publishes them despite having no `Donor(s)` column — and `age_at_sampling` is a float in years, which could represent neither the `"2-4"` ranges nor the `"unknown"` sentinels the corpus contains.

The biosample is the destination, so a row that names no `Biosample term name` has nowhere to put them and they are dropped. That is exactly the row the annotation transform relaxed `require_biosample` for, so the caveat matters more here than on the experiment path: an annotation row with donor traits but no biosample loses them. Nothing was lost against the corpus as last checked — of the biosample-less rows in the two configured annotation types, none published any of the three — and inventing a biosample to hold them would be worse than dropping them. `test_transform_annotation_to_c2m2_should_drop_donor_traits_with_no_biosample` pins the behavior.

### Experiment-only columns

Absent from the annotation TSV, and therefore **unset** on annotation documents rather than derived: all eight `Library *`, all six `Biosample genetic modifications *`, `Biosample treatments *`, `Biological`/`Technical replicate(s)`, `Donor(s)`, `Experiment target`, `File analysis title`/`status`, `File format type`, `Genome annotation`, `Index of`, `Read length`, `Mapped read length`, `Run type`, `Paired end`, `Paired with`, `Platform`, `RBNS protein concentration`, `Controlled by`.

Two consequences worth naming:

- **No subjects.** With no `Donor(s)` column there is nothing to key a `Subject` on, so annotation documents carry `collections[].subjects == []` and no donor is fabricated. The organism that would have reached `subjects[].taxonomy` is on `extra.encode.organism` instead — which is the only way the organism of a multi-organism result set (the released cCREs span *Homo sapiens* and *Mus musculus*) is recoverable without inspecting filenames.
- **The dataset does not require a biosample.** Unlike the experiment path, the annotation collection is built from `Dataset accession` alone. 48 released cCRE files name no biosample term; gating on one would leave 24 dataset accessions unqueryable. Its `persistent_id` points at `/annotations/`, not `/experiments/`.

## Sync Flow

ENCODE sync bypasses the C2M2 ZIP pipeline entirely. Files are pre-materialized during ingest.

| Hook | Phase | Function |
|------|-------|----------|
| Full ingest | Replaces C2M2 load + materialize | `_sync_encode()` |

### Data Flow

`_sync_encode` runs one phase for released experiments plus one per configured `annotation_type`:

```text
_sync_encode()
  ├─ clear ENCODE data + upsert the DCC record  (inside the cutover lock)
  │
  ├─ phase "experiment"
  │     fetch_encode_metadata()               # Streaming TSV from ENCODE API
  │       └─> transform_to_c2m2(row)          # Yields one dict per row
  │
  ├─ phase "annotation[<type>]"  × one per configured type
  │     fetch_encode_annotation_metadata(type)
  │       └─> transform_annotation_to_c2m2(row)
  │             └─ rename columns, then the shared transformation
  │
  └─ each transformation:
        ├─ Map File format -> EDAM      # ontology_mappings.get_file_format()
        ├─ Derive compression -> EDAM   # from the download URL's suffix
        ├─ Map Output type -> EDAM      # ontology_mappings.get_data_type()
        ├─ Map Assay -> OBI             # ontology_mappings.get_assay_type()
        ├─ Map Organism -> NCBI         # ontology_mappings.get_taxonomy()
        ├─ Build collection + biosample + subjects inline
        └─ Insert into files collection (batches of 1000)
```

**Phase isolation.** A phase that raises is logged and recorded, and the remaining phases still run — a transient failure on one stream does not cost the corpus the others. A stream that dies *mid-flight* (the shape an `asyncio.TimeoutError` against the metadata budget takes) has its trailing partial batch committed rather than discarded, and still reports the rows it loaded, so the per-phase counts, the summary tallies and the collection all agree. The indexes are ensured unconditionally afterwards, so whatever did load is servable without a full collection scan. The sync then fails, naming the phases that broke: reporting a clean sync over a partially loaded collection would be worse than a visible failure.

**Each phase replaces only its own slice.** `files` is not cleared corpus-wide before the fan-out. Each phase owns a slice — experiments are the documents with no `extra.encode.annotation_type`, each annotation phase the documents carrying its type — and deletes that slice only once it has replacement rows in hand. A phase that fails before delivering any row, the shape a portal 504 or an exhausted download budget takes, therefore leaves its previous rows being served instead of emptying them; clearing up front meant a failed experiment phase left the API serving the ~29k annotation documents as the entire corpus. A stream that drains cleanly with no rows is the opposite case and does clear, so a type ENCODE stopped publishing stops being served. A phase that dies partway through still leaves its own slice partially loaded — only loading into a shadow collection and swapping on success would close that, and it is not part of this change.

**One budget for the fan-out.** `ENCODE_METADATA_TIMEOUT_SECONDS` (default 3600) bounds the whole sync, not each stream within it. The budget is resolved once and every phase shares a single deadline, each receiving whatever remains; a phase that finds the budget spent fails immediately rather than opening a request it has no time to finish. Per-stream budgets would have multiplied the ceiling by the number of phases — three by default, and unbounded as the allowlist grows — while the whole fan-out sits inside the cutover lock that gates the read surface. That matters beyond availability: the sync lock treats itself as abandoned after one hour (`STALE_LOCK_THRESHOLD`) with nothing refreshing `started_at`, so a run that outlives the threshold admits a second sync, which clears the ENCODE corpus while the first is still inserting into it.

**Queryability.** `extra.encode.annotation_type` and `extra.encode.organism` are indexed by `materialized_files_index_specs()` — the ENCODE sync writes `files` directly and never reaches the Rust materializer that owns the rest, so nothing else would create them — and both are in `ALLOWED_DISTINCT_FIELDS`, so a client can enumerate the available annotation types rather than having to know ENCODE's exact spelling in advance.

No post-ingest enrichment pass. All metadata is captured during the initial TSV transformation.
