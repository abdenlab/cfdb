# Common Fund Database

CFDB is a Python package for querying and serving C2M2 (Crosscut Metadata Model) file metadata from Common Fund Data Coordinating Centers (DCCs).

## Installation

```bash
pip install git+https://github.com/abdenlab/cfdb.git
```

Requires Python 3.10 or later.

## Setup

### Prerequisites

- **Docker** - For running MongoDB and the API

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SYNC_API_KEY` | API key for the sync endpoint (required - API won't start without it) | - |
| `SYNC_DATA_DIR` | Directory for downloaded sync data files | - |
| `CFDB_API_URL` | Base URL for the cfdb API | `http://localhost:8000` |
| `DATABASE_URL` | MongoDB connection string | `mongodb://localhost:27017` |
| `MONGODB_TLS_ENABLED` | Enable X.509 certificate authentication (production) | `false` |
| `MONGODB_CERT_PATH` | Path to client certificate bundle | `/etc/cfdb/certs/client-bundle.pem` |
| `MONGODB_CA_PATH` | Path to CA certificate | `/etc/cfdb/certs/ca.pem` |

### Quick Start

```bash
# 1. Start MongoDB (restores sample data and creates indexes)
make mongodb

# 2. Start the API server
make api

# 3. (Optional) Sync latest DCC metadata
curl -X POST -H "X-API-Key: dev-sync-key" http://localhost:8000/sync
```

This starts:
- MongoDB on port 27017 (with indexes)
- GraphQL/REST API on port 8000

### Production Deployment (TLS/X.509)

For production, MongoDB uses TLS encryption with X.509 certificate authentication:

```bash
# 1. Generate certificates (customize hostname/IP as needed)
./certs/generate-certs.sh mongodb.example.com 10.0.1.50

# Or use environment variables
MONGODB_HOSTNAME=mongodb.example.com MONGODB_IP=10.0.1.50 ./certs/generate-certs.sh

# 2. Start MongoDB with TLS
make mongodb-prod

# 3. Start API with client certificate
make api-prod
```

The certificate script generates:
- `certs/ca/ca.pem` - CA certificate (deploy to all containers)
- `certs/server/mongodb-server-bundle.pem` - MongoDB server certificate
- `certs/clients/cfdb-api-bundle.pem` - API client certificate
- `certs/clients/cfdb-materializer-bundle.pem` - Materializer client certificate

Run `./certs/generate-certs.sh --help` for full usage information.

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make mongodb` | Build and start MongoDB with sample data and indexes |
| `make api` | Build and start the API container |
| `make materialize-files` | Manually materialize all file metadata (usually done via sync) |
| `make materialize-dcc DCC=hubmap` | Materialize a single DCC |
| `make certs` | Generate TLS certificates for production |
| `make mongodb-prod` | Start MongoDB with TLS/X.509 authentication |
| `make api-prod` | Start API with X.509 client certificate |

### Sync Workflow

The sync endpoint (`POST /sync`) handles the full data refresh:

1. Downloads C2M2 datapackages from DCCs
2. Loads data into underlying MongoDB collections
3. Runs the Rust materializer to create the fully-joined `files` collection

The materializer is included in the API Docker image and runs automatically after each DCC sync.

## API Usage

### GraphQL Endpoint

**URL:** `POST /metadata`

Query file metadata using GraphQL. The API exposes two queries:

#### `files` Query

Returns a paginated list of files matching the input criteria.

```graphql
query {
  files(
    input: [FileMetadataInput]
    page: Int = 0
    pageSize: Int = 100
  ) {
    idNamespace
    localId
    filename
    sizeInBytes
    dcc {
      dccAbbreviation
      dccName
    }
    fileFormat {
      name
    }
    collections {
      name
      biosamples {
        anatomy {
          name
        }
      }
    }
  }
}
```

```bash
# Query all files (first page)
curl -X POST http://localhost:8000/metadata \
  -H "Content-Type: application/json" \
  -d '{"query": "{ files { filename sizeInBytes dcc { dccAbbreviation } } }"}'

# Query files with pagination
curl -X POST http://localhost:8000/metadata \
  -H "Content-Type: application/json" \
  -d '{"query": "{ files(page: 0, pageSize: 10) { filename } }"}'

# Query files from a specific DCC
curl -X POST http://localhost:8000/metadata \
  -H "Content-Type: application/json" \
  -d '{"query": "{ files(input: [{ dcc: [{ dccAbbreviation: [\"4DN\"] }] }]) { filename dcc { dccAbbreviation } } }"}'
```

#### `file` Query

Returns a single file by its MongoDB ObjectId.

```graphql
query {
  file(id: "507f1f77bcf86cd799439011") {
    filename
    accessUrl
  }
}
```

```bash
curl -X POST http://localhost:8000/metadata \
  -H "Content-Type: application/json" \
  -d '{"query": "{ file(id: \"507f1f77bcf86cd799439011\") { filename accessUrl } }"}'
```

#### Data Model

The API serves file metadata following the C2M2 data model. Below is the complete schema.

##### FileMetadataModel

The central entity representing a stable digital asset.

| Field | Type | Description |
|-------|------|-------------|
| `id_namespace` | string | CFDE-cleared identifier for the top-level data space (PK part 1) |
| `local_id` | string | Identifier unique within the namespace (PK part 2) |
| `dcc` | DCC | The Data Coordinating Center that produced this file |
| `collections` | Collection[] | Collections containing this file |
| `project` | Project? | The primary project within which this file was created |
| `project_id_namespace` | string | Project namespace (FK part 1) |
| `project_local_id` | string | Project local ID (FK part 2) |
| `persistent_id` | string? | Permanent URI or compact ID |
| `creation_time` | string? | ISO 8601 timestamp |
| `size_in_bytes` | int? | File size |
| `sha256` | string? | SHA-256 checksum (preferred) |
| `md5` | string? | MD5 checksum (if SHA-256 unavailable) |
| `filename` | string | Filename without path |
| `file_format` | FileFormat? | EDAM CV term for digital format |
| `compression_format` | string? | EDAM CV term for compression (e.g., gzip) |
| `data_type` | DataType? | EDAM CV term for data type |
| `assay_type` | AssayType? | OBI CV term for experiment type |
| `analysis_type` | string? | OBI CV term for analysis type |
| `mime_type` | string? | MIME type |
| `bundle_collection_id_namespace` | string? | Bundle collection namespace |
| `bundle_collection_local_id` | string? | Bundle collection local ID |
| `dbgap_study_id` | string? | dbGaP study ID for access control |
| `access_url` | string? | DRS URI or publicly accessible URL |
| `status` | string? | Dataset status (e.g., "Published", "QA") - HuBMAP specific |
| `data_access_level` | string? | Access level: public, consortium, or protected - HuBMAP specific |
| `extra` | EnrichedFile? | DCC-specific file metadata (see EnrichedFile) |

##### DCC

A Common Fund program or Data Coordinating Center.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | CFDE-CC issued identifier |
| `dcc_name` | string | Human-readable label |
| `dcc_abbreviation` | string | Short display label |
| `dcc_description` | string? | Human-readable description |
| `contact_email` | string | Primary technical contact email |
| `contact_name` | string | Primary technical contact name |
| `dcc_url` | string | DCC website URL |
| `project_id_namespace` | string | Project namespace |
| `project_local_id` | string | Project local ID |

##### Collection

A grouping of files, biosamples, and/or subjects.

| Field | Type | Description |
|-------|------|-------------|
| `id_namespace` | string | Collection namespace (PK part 1) |
| `local_id` | string | Collection local ID (PK part 2) |
| `biosamples` | Biosample[] | Biosamples in this collection |
| `subjects` | Subject[] | Subjects (donors) directly in this collection |
| `anatomy` | Anatomy[] | Anatomy terms associated with this collection |
| `persistent_id` | string? | Permanent URI |
| `creation_time` | string? | ISO 8601 timestamp |
| `abbreviation` | string? | Short display label |
| `name` | string | Human-readable label |
| `description` | string? | Human-readable description |
| `extra` | EnrichedCollection? | DCC-specific collection metadata (see EnrichedCollection) |

##### Biosample

A tissue sample or other physical specimen.

| Field | Type | Description |
|-------|------|-------------|
| `id_namespace` | string | Biosample namespace (PK part 1) |
| `local_id` | string | Biosample local ID (PK part 2) |
| `project_id_namespace` | string | Project namespace (FK part 1) |
| `project_local_id` | string | Project local ID (FK part 2) |
| `persistent_id` | string? | Permanent URI |
| `creation_time` | string? | ISO 8601 timestamp |
| `sample_prep_method` | string? | OBI CV term for preparation method |
| `anatomy` | Anatomy? | UBERON CV term for anatomical origin |
| `biofluid` | string? | UBERON/InterLex term for fluid origin |
| `subjects` | Subject[] | Subjects (donors) from which this biosample was derived |
| `extra` | EnrichedBiosample? | DCC-specific biosample metadata (see EnrichedBiosample) |

##### Anatomy

An UBERON (Uber-anatomy ontology) CV term.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | UBERON CV term identifier |
| `name` | string | Human-readable label |
| `description` | string? | Human-readable description |

##### FileFormat

An EDAM CV 'format:' term describing digital format.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | EDAM format term identifier |
| `name` | string | Human-readable label |
| `description` | string? | Human-readable description |

##### DataType

An EDAM CV 'data:' term describing the type of data.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | EDAM data term identifier |
| `name` | string | Human-readable label |
| `description` | string? | Human-readable description |

##### AssayType

An OBI (Ontology for Biomedical Investigations) CV term describing experiment types.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | OBI CV term identifier |
| `name` | string | Human-readable label |
| `description` | string? | Human-readable description |

##### Subject

A human or organism from which biosamples are derived.

| Field | Type | Description |
|-------|------|-------------|
| `id_namespace` | string | Subject namespace (PK part 1) |
| `local_id` | string | Subject local ID (PK part 2) |
| `project_id_namespace` | string | Project namespace (FK part 1) |
| `project_local_id` | string | Project local ID (FK part 2) |
| `persistent_id` | string? | Permanent URI |
| `creation_time` | string? | ISO 8601 timestamp |
| `granularity` | string? | CFDE CV term (single organism, cell line, microbiome, etc.) |
| `sex` | string? | NCIT CV term for biological sex |
| `ethnicity` | string? | NCIT CV term for self-reported ethnicity |
| `age_at_enrollment` | float? | Age in years when enrolled in primary project |
| `age_at_sampling` | float? | Age in years when biosample was taken |
| `race` | string[] | CFDE CV terms for self-identified race(s) |
| `taxonomy` | NCBITaxonomy? | NCBI taxonomy for the subject's organism |

##### NCBITaxonomy

An NCBI Taxonomy term for organism classification.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | NCBI Taxonomy Database ID (e.g., NCBI:txid9606) |
| `name` | string | Taxonomy name (e.g., "Homo sapiens") |
| `clade` | string? | Phylogenetic level (e.g., species, genus) |
| `description` | string? | Human-readable description |

##### Project

A node in the C2M2 project hierarchy.

| Field | Type | Description |
|-------|------|-------------|
| `id_namespace` | string | Project namespace (PK part 1) |
| `local_id` | string | Project local ID (PK part 2) |
| `name` | string | Human-readable label |
| `abbreviation` | string? | Short display label |
| `description` | string? | Human-readable description |
| `persistent_id` | string? | Permanent URI or compact ID |

##### EnrichedFile

DCC-specific file-level metadata. Union of fields from 4DN materializer, 4DN API enrichment, and ENCODE ingest.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `enriched_file_format` | string? | 4DN materializer | Derived format (mcool, hic, pairs, etc.) |
| `genome_assembly` | string? | 4DN API | Reference genome (e.g., "GRCh38") |
| `file_type` | string? | 4DN API | Semantic file type (e.g., "contact matrix") |
| `file_type_detailed` | string? | 4DN API | Detailed type (e.g., "contact matrix (mcool)") |
| `condition` | string? | 4DN API | Experimental condition |
| `biosource_name` | string? | 4DN API | Cell line or tissue name |
| `dataset` | string? | 4DN API | Dataset description |
| `experiment_type` | string? | 4DN API | Experiment type |
| `assay_info` | string? | 4DN API | Assay details |
| `replicate_info` | string? | 4DN API | Replicate details |
| `cell_line_tier` | string? | 4DN API | Cell line tier (Tier 1/Tier 2) |
| `extra_files` | ExtraFile[]? | 4DN API | Associated index files |
| `assembly` | string? | ENCODE | Genome assembly (GRCh38, mm10, etc.) |
| `file_format_type` | string? | ENCODE | narrowPeak, broadPeak, etc. |
| `output_type` | string? | ENCODE | Original ENCODE output type |
| `experiment_accession` | string? | ENCODE | Parent experiment accession |
| `experiment_target` | string? | ENCODE | ChIP-seq target, etc. |
| `project` | string? | ENCODE | ENCODE project phase |
| `lab` | string? | ENCODE | Lab/PI name |
| `platform` | string? | ENCODE | Sequencing platform |
| `dbxrefs` | string? | ENCODE | External cross-references |
| `genome_annotation` | string? | ENCODE | Genome annotation version |
| `controlled_by` | string? | ENCODE | Control file accessions |
| `s3_uri` | string? | ENCODE | S3 storage path |
| `azure_url` | string? | ENCODE | Azure storage URL |
| `file_analysis_title` | string? | ENCODE | Analysis pipeline name |
| `file_analysis_status` | string? | ENCODE | Analysis pipeline status |
| `biological_replicates` | string? | ENCODE | Biological replicate number(s) |
| `technical_replicates` | string? | ENCODE | Technical replicate number(s) |
| `read_length` | string? | ENCODE | Sequencing read length |
| `mapped_read_length` | string? | ENCODE | Mapped read length |
| `run_type` | string? | ENCODE | single-ended/paired-ended |
| `paired_end` | string? | ENCODE | 1 or 2 for paired reads |
| `paired_with` | string? | ENCODE | Paired file accession |
| `index_of` | string? | ENCODE | Indexed file accession |
| `derived_from` | string? | ENCODE | Upstream file accessions |
| `library_made_from` | string? | ENCODE | RNA, DNA, etc. |
| `library_depleted_in` | string? | ENCODE | rRNA, etc. |
| `library_extraction_method` | string? | ENCODE | Extraction method |
| `library_lysis_method` | string? | ENCODE | Lysis method |
| `library_crosslinking_method` | string? | ENCODE | Crosslinking method |
| `library_strand_specific` | string? | ENCODE | Strand specificity |
| `library_fragmentation_method` | string? | ENCODE | Fragmentation method |
| `library_size_range` | string? | ENCODE | Library size range |
| `rbns_protein_concentration` | string? | ENCODE | RBNS protein concentration |
| `audit_warning` | string? | ENCODE | Audit warnings |
| `audit_not_compliant` | string? | ENCODE | Audit non-compliance |
| `audit_error` | string? | ENCODE | Audit errors |

##### ExtraFile

An associated index or auxiliary file from 4DN.

| Field | Type | Description |
|-------|------|-------------|
| `href` | string? | Relative URL path on 4DN data portal |
| `md5sum` | string? | MD5 checksum |
| `file_size` | int? | File size in bytes |
| `file_format` | string? | Format identifier (e.g., "pairs_px2", "bai") |

##### EnrichedCollection

DCC-specific collection-level metadata from 4DN experiment API.

| Field | Type | Description |
|-------|------|-------------|
| `display_title` | string? | Experiment display name |
| `experiment_type` | string? | Experiment type (e.g., "in situ Hi-C") |
| `targeted_factor` | string[]? | Target proteins/marks (e.g., ["CTCF protein"]) |
| `digestion_enzyme` | string? | Restriction enzyme (e.g., "DpnII") |
| `lab` | string? | Lab/PI name |
| `crosslinking_method` | string? | Crosslinking method |
| `crosslinking_temperature` | string? | Crosslinking temperature |
| `crosslinking_time` | string? | Crosslinking time |
| `ligation_temperature` | string? | Ligation temperature |
| `ligation_volume` | string? | Ligation volume |
| `ligation_time` | string? | Ligation time |
| `digestion_temperature` | string? | Digestion temperature |
| `digestion_time` | string? | Digestion time |
| `tagging_method` | string? | Tagging method (DamID) |
| `fragmentation_method` | string? | Fragmentation method |
| `biotin_removed` | string? | Whether biotin was removed |
| `library_prep_kit` | string? | Library prep kit used |
| `average_fragment_size` | string? | Average fragment size |
| `fragment_size_range` | string? | Fragment size range |
| `status` | string? | Experiment status |
| `date_created` | string? | Experiment creation date |

##### EnrichedBiosample

DCC-specific biosample-level metadata from ENCODE.

| Field | Type | Description |
|-------|------|-------------|
| `biosample_type` | string? | primary cell, tissue, cell line, etc. |
| `biosample_treatments` | string? | Treatment details |
| `biosample_treatments_amount` | string? | Treatment amount |
| `biosample_treatments_duration` | string? | Treatment duration |
| `biosample_genetic_modifications` | string? | CRISPR, RNAi, etc. |

#### Query Mechanics

The GraphQL API uses an implicit OR/AND clause system for building MongoDB queries.

**How It Works:**

1. **Lists become OR clauses**: Multiple values in an array are combined with `$or`
2. **Dict keys become AND clauses**: Multiple fields in an object are combined with `$and`

##### Simple Query - Single Value

```graphql
query {
  files(input: [{ filename: ["data.csv"] }]) {
    filename
  }
}
```

MongoDB query:
```json
{ "filename": "data.csv" }
```

##### OR Query - Multiple Values in a List

Find files with either filename:

```graphql
query {
  files(input: [{ filename: ["data.csv", "results.tsv"] }]) {
    filename
  }
}
```

MongoDB query:
```json
{ "$or": [{ "filename": "data.csv" }, { "filename": "results.tsv" }] }
```

##### AND Query - Multiple Fields

Find files matching both criteria:

```graphql
query {
  files(input: [{
    filename: "data.csv",
    dcc: { dccAbbreviation: ["4DN"] }
  }]) {
    filename
    dcc { dccAbbreviation }
  }
}
```

MongoDB query:
```json
{
  "$and": [
    { "filename": "data.csv" },
    { "dcc.dcc_abbreviation": "4DN" }
  ]
}
```

##### Combined OR/AND Query

Find files from 4DN OR HuBMAP with specific file formats:

```graphql
query {
  files(input: [{
    dcc: [
      { dccAbbreviation: ["4DN"] },
      { dccAbbreviation: ["HuBMAP"] }
    ],
    fileFormat: { name: "FASTQ" }
  }]) {
    filename
    dcc { dccAbbreviation }
    fileFormat { name }
  }
}
```

MongoDB query:
```json
{
  "$and": [
    { "$or": [
      { "dcc.dcc_abbreviation": "4DN" },
      { "dcc.dcc_abbreviation": "HuBMAP" }
    ]},
    { "file_format.name": "FASTQ" }
  ]
}
```

##### Nested Entity Query

Find files from biosamples with specific anatomy:

```graphql
query {
  files(input: [{
    collections: {
      biosamples: {
        anatomy: { name: "heart" }
      }
    }
  }]) {
    filename
    collections {
      biosamples {
        anatomy { name }
      }
    }
  }
}
```

##### Pagination

Use `page` and `pageSize` parameters:

```graphql
query {
  files(page: 0, pageSize: 50) {
    filename
  }
}
```

#### Entity Relationships

The data model uses MongoDB aggregation pipelines to join related entities:

```
file
├── dcc (DCC) ─────────────────── via submission field
├── project (Project) ─────────── via project FK
├── file_format (FileFormat) ──── via file_format ID
├── data_type (DataType) ──────── via data_type ID
├── assay_type (AssayType) ────── via assay_type ID
└── collections[] (Collection) ── via file_in_collection
    ├── anatomy[] (Anatomy) ───── via collection_anatomy
    ├── subjects[] (Subject) ──── via subject_in_collection
    │   └── taxonomy (NCBITaxonomy) ── via subject_role_taxonomy
    └── biosamples[] (Biosample) ─ via biosample_in_collection
        ├── anatomy (Anatomy) ──── via anatomy ID
        └── subjects[] (Subject) ─ via biosample_from_subject
            └── taxonomy (NCBITaxonomy) ── via subject_role_taxonomy
```

Cross-reference tables:
- `file_in_collection` - Links files to collections
- `biosample_in_collection` - Links biosamples to collections
- `subject_in_collection` - Links subjects directly to collections
- `biosample_from_subject` - Links biosamples to their source subjects
- `collection_anatomy` - Links anatomy terms to collections
- `subject_role_taxonomy` - Links subjects to NCBI taxonomy terms

### GraphiQL IDE

**URL:** `GET /metadata`

Visit [http://localhost:8000/metadata](http://localhost:8000/metadata) in your browser to access GraphiQL, an interactive IDE for exploring and testing GraphQL queries.

Features:
- **Schema Documentation** - Browse all available types, fields, and their descriptions
- **Query Editor** - Write queries with syntax highlighting and error detection
- **Autocomplete** - Get field suggestions as you type (Ctrl+Space)
- **Query History** - Access previously executed queries
- **Response Viewer** - See formatted JSON results

### File Streaming Endpoint

**URL:** `GET /data/{dcc}/{local_id}` | `HEAD /data/{dcc}/{local_id}`

Stream file contents from DCCs via HTTPS. Supports both GET (download) and HEAD (metadata only) requests.

**Path Parameters:**
- `dcc` - DCC abbreviation (e.g., `4dn`, `hubmap`) - case insensitive
- `local_id` - The file's unique ID within the DCC

**Headers:**
- `Range` (optional) - Supports `bytes=start-end` for partial content requests

**Response Codes:**
| Code | Description |
|------|-------------|
| 200 | Full file content (GET) or file metadata (HEAD) |
| 206 | Partial content (Range request) |
| 400 | Invalid DCC or Range header |
| 403 | File requires authentication (consortium/protected access) |
| 404 | File not found |
| 501 | No supported access method (e.g., Globus-only files) |
| 502 | Upstream service error |
| 504 | Service timeout |

**Example:**

```bash
# Check file availability (HEAD request)
curl -I http://localhost:8000/data/4dn/abc123

# Download a 4DN file
curl -O http://localhost:8000/data/4dn/abc123

# Download with Range header
curl -H "Range: bytes=0-1023" http://localhost:8000/data/hubmap/xyz789
```

### Index File Streaming Endpoint

**URL:** `GET /index/{dcc}/{local_id}` | `HEAD /index/{dcc}/{local_id}`

Stream index files (e.g., `.px2`, `.bai`) associated with DCC data files. Index files are typically used to determine byte ranges for tiling visualizations — fetch the index first, then stream specific byte ranges from `/data/{dcc}/{local_id}`.

**Path Parameters:**
- `dcc` - DCC abbreviation (e.g., `4dn`) - case insensitive
- `local_id` - The file's unique ID within the DCC

**Headers:**
- `Range` (optional) - Supports `bytes=start-end` for partial content requests

**Response Codes:**
| Code | Description |
|------|-------------|
| 200 | Full index file content (GET) or file metadata (HEAD) |
| 206 | Partial content (Range request) |
| 400 | Invalid DCC or Range header |
| 404 | File not found or no index file available |
| 502 | Upstream service error |

**Example:**

```bash
# Check index file availability (HEAD request)
curl -I http://localhost:8000/index/4dn/4DNFIG5NX1EC

# Download the index file
curl -O http://localhost:8000/index/4dn/4DNFIG5NX1EC

# Use with data endpoint for tiling visualization
# 1. Fetch index to determine byte ranges
curl -o index.px2 http://localhost:8000/index/4dn/4DNFIG5NX1EC
# 2. Stream specific byte ranges from the data file
curl -H "Range: bytes=0-65535" http://localhost:8000/data/4dn/4DNFIG5NX1EC
```

### Sync Endpoint

**URL:** `POST /sync`

Trigger a sync of C2M2 datapackages from DCCs. Requires API key authentication.

**Behavior:**

- **Single sync at a time** - Only one sync task can run at a time. Concurrent requests return `409 Conflict`.
- **Background execution** - The endpoint returns immediately with a `202 Accepted` response while the sync runs in the background.
- **Sync process** - For each DCC, the sync: downloads the datapackage, extracts it, clears existing DCC data, loads new data, materializes files, then cleans up temporary files.
- **Materialization** - After loading each DCC's data, the Rust materializer runs to create the denormalized `files` collection with all joins pre-computed. This is incremental - only the synced DCC's files are updated.
- **Database cutover** - During the clear/load phase, API requests (GraphQL queries and file streaming) are briefly blocked to ensure data consistency. Requests wait for the cutover to complete before proceeding.

**Headers:**
- `X-API-Key` (required) - API key matching `SYNC_API_KEY` environment variable

**Query Parameters:**
- `dccs` (optional, repeatable) - DCC names to sync. If omitted, syncs all DCCs.

**Response Codes:**
| Code | Description |
|------|-------------|
| 202 | Sync started successfully |
| 401 | Invalid API key |
| 409 | A sync is already in progress |
| 500 | Server configuration error |

**Example:**

```bash
# Sync all DCCs
curl -X POST -H "X-API-Key: your-key" http://localhost:8000/sync

# Sync specific DCCs
curl -X POST -H "X-API-Key: your-key" "http://localhost:8000/sync?dccs=4dn&dccs=hubmap"
```

### Sync Status Endpoint

**URL:** `GET /sync/{task_id}`

Check the status of a sync task.

**Path Parameters:**
- `task_id` - The task ID returned when starting a sync

**Response:**
```json
{
  "task_id": "abc-123",
  "status": "running",
  "dcc_names": ["4dn", "hubmap"],
  "started_at": "2024-01-15T10:30:00",
  "completed_at": null
}
```

**Response Codes:**
| Code | Description |
|------|-------------|
| 200 | Task status returned |
| 404 | Task not found |

**Example:**

```bash
# Start a sync and get task ID
curl -X POST -H "X-API-Key: your-key" "http://localhost:8000/sync?dccs=4dn"
# Returns: {"task_id": "abc-123", ...}

# Check sync status
curl http://localhost:8000/sync/abc-123
```

## CLI Usage

### `cfdb sync`

Trigger a sync via the cfdb API.

```bash
# Sync all DCCs
cfdb sync

# Sync specific DCCs
cfdb sync 4dn hubmap

# Specify API URL
cfdb sync --api-url http://api.example.com 4dn

# Specify API key (or set SYNC_API_KEY env var)
cfdb sync --api-key your-key
```

**Options:**
- `--api-url` - cfdb API base URL (default: `http://localhost:8000`, env: `CFDB_API_URL`)
- `--api-key` - API key for sync endpoint (env: `SYNC_API_KEY`)
- `--debug` / `-d` - Enable debugpy debugging

## HuBMAP Data Portal Filter Mapping

The following table maps HuBMAP data portal search dimensions to CFDB/C2M2 fields:

| Category | HuBMAP Dimension | CFDB Field | Status | Notes |
|----------|------------------|------------|--------|-------|
| **Dataset** | Dataset/Assay Type | `assay_type.name` | ✅ | OBI CV terms (CODEX, RNA-seq, etc.) |
| **Dataset** | Data Type | `data_type.name` | ✅ | EDAM CV terms |
| **Dataset** | File Format | `file_format.name` | ✅ | EDAM CV terms |
| **Dataset** | Data Access Level | `data_access_level` | ✅ | public/consortium/protected |
| **Dataset** | Status | `status` | ✅ | Published/QA (HuBMAP-specific) |
| **Dataset** | DCC/Affiliation | `dcc.dcc_abbreviation` | ✅ | Data provider |
| **Organ** | Organ | `collections.anatomy.name` | ✅ | UBERON CV terms |
| **Sample** | Sample Prep Method | `collections.biosamples.sample_prep_method` | ✅ | OBI CV terms |
| **Sample** | Biofluid | `collections.biosamples.biofluid` | ✅ | UBERON/InterLex terms |
| **Donor** | Sex | `collections.subjects.sex` | ✅ | NCIT CV terms |
| **Donor** | Age | `collections.subjects.age_at_enrollment` | ✅ | Decimal years |
| **Donor** | Age at Sampling | `collections.biosamples.subjects.age_at_sampling` | ✅ | Decimal years |
| **Donor** | Race | `collections.subjects.race` | ✅ | CFDE CV terms (multi-valued) |
| **Donor** | Ethnicity | `collections.subjects.ethnicity` | ✅ | NCIT CV terms |
| **Donor** | Granularity | `collections.subjects.granularity` | ✅ | single organism/cell line/etc. |
| **Donor** | BMI | — | ❌ | Not in C2M2 |
| **Donor** | Height/Weight | — | ❌ | Not in C2M2 |
| **Donor** | Medical History | — | ❌ | Diabetes, hypertension, etc. |
| **Donor** | Lifestyle | — | ❌ | Smoking, alcohol, drug use |
| **Donor** | Cause of Death | — | ❌ | Not in C2M2 |
| **Donor** | Blood Type | — | ❌ | Not in C2M2 |
| **Processing** | Pipeline | `analysis_type` | ⚠️ | Partial - OBI CV terms |
| **Processing** | Processing Type | — | ❌ | HuBMAP-specific |

**Legend:** ✅ Supported | ⚠️ Partial | ❌ Not Available

## 4DN Data Portal Filter Mapping

The following table maps 4DN data portal search dimensions to CFDB/C2M2 fields:

| Category | 4DN Dimension | CFDB Field | Status | Notes |
|----------|---------------|------------|--------|-------|
| **Experiment** | Experiment Type | `assay_type.name` | ✅ | OBI CV terms (Hi-C, etc.) |
| **Experiment** | Data Category | `data_type.name` | ✅ | Sequencing vs Microscopy |
| **File** | File Format | `file_format.name` | ✅ | EDAM CV terms |
| **File** | File Size | `size_in_bytes` | ✅ | Integer bytes |
| **Sample** | Tissue/Anatomy | `collections.anatomy.name` | ✅ | UBERON CV terms |
| **Sample** | Sample Prep | `collections.biosamples.sample_prep_method` | ✅ | OBI CV terms |
| **Sample** | Biosource/Cell Line | `collections.biosamples.local_id`, `extra.biosource_name` | ✅ | Cell line in biosample ID + API-enriched name |
| **Sample** | Organism | `collections.subjects.taxonomy.name` | ✅ | NCBI taxonomy |
| **Sample** | Cell Line Tier | `extra.cell_line_tier` | ✅ | Tier 1/Tier 2 from 4DN API (~17 classified cell lines) |
| **Dataset** | Dataset/Collection | `collections.name`, `extra.dataset` | ✅ | Collection grouping + API-enriched dataset name |
| **Dataset** | Publication/DOI | `collections.persistent_id` | ⚠️ | If DOI linked |
| **Dataset** | Condition | `extra.condition` | ✅ | From 4DN API (e.g., "Formaldehyde+DSG, DpnII") |
| **File** | Genome Assembly | `extra.genome_assembly` | ✅ | From 4DN API (e.g., "GRCh38") |
| **File** | File Type | `extra.file_type` | ✅ | From 4DN API (e.g., "contact matrix") |
| **File** | File Type Detailed | `extra.file_type_detailed` | ✅ | From 4DN API (e.g., "contact matrix (mcool)") |
| **Experiment** | Experiment Type | `extra.experiment_type` | ✅ | From 4DN API (e.g., "in situ Hi-C") |
| **Experiment** | Assay Info | `extra.assay_info` | ✅ | From 4DN API (e.g., "DpnII") |
| **Replicate** | Replicate Info | `extra.replicate_info` | ✅ | From 4DN API (e.g., "Biorep 1, Techrep 1") |
| **Provider** | DCC | `dcc.dcc_abbreviation` | ✅ | Always "4DN" |
| **Provider** | Lab/Project | `project.name` | ✅ | Via project FK |
| **Experiment** | Targeted Factor | `collections.extra.targeted_factor` | ✅ | Protein target (e.g., ["CTCF protein"]) from experiment API |
| **Experiment** | Digestion Enzyme | `collections.extra.digestion_enzyme` | ✅ | Restriction enzyme from experiment API |
| **Experiment** | Lab | `collections.extra.lab` | ✅ | Lab/PI from experiment API |

**Legend:** ✅ Supported | ⚠️ Partial | ❌ Not Available

### 4DN Enriched File Format

The C2M2 `file_format` field uses EDAM ontology terms that describe container formats (e.g., HDF5) rather than the specific data format. For 4DN this is particularly limiting: `.mcool`, `.cool`, and `.hic` files are all HDF5-based, and `.hic` files have no `file_format` at all.

During materialization, the materializer derives `extra.enriched_file_format` from the filename extension when `file_format` is ambiguous (empty, HDF5, or Plain text):

| Extension | `file_format` (C2M2) | `extra.enriched_file_format` | Description |
|-----------|----------------------|------------------------------|-------------|
| `.mcool` | HDF5 | `mcool` | Multi-resolution cooler (Hi-C contact matrix) |
| `.cool` | HDF5 | `cool` | Single-resolution cooler (Hi-C contact matrix) |
| `.hic` | _(empty)_ | `hic` | Juicer Hi-C contact matrix |
| `.pairs` / `.pairs.gz` | Plain text | `pairs` | 4DN pairs format (Hi-C read pairs) |
| `.r3d` | _(empty)_ | `r3d` | 3D reconstruction data |
| `.nd2` | _(empty)_ | `nd2` | Nikon microscopy image |
| `.flex` | _(empty)_ | `flex` | Flex microscopy data |
| `.spt` | _(empty)_ | `spt` | Single-particle tracking data |
| `.matrix` | _(empty)_ | `matrix` | Matrix data |

Files with unambiguous formats (FASTQ, BAM, BED, bigWig, etc.) are not enriched since the EDAM `file_format` term is already specific.

### 4DN API Enrichment

After materialization, a post-processing step fetches additional metadata from the [4DN Search API](https://data.4dnucleome.org) and merges it into the `extra` field on materialized file documents. This supplements the C2M2 data dump (which lacks many 4DN-specific fields) without modifying the Rust materializer.

| Extra Field | Source | Description |
|-------------|--------|-------------|
| `extra.genome_assembly` | `FileProcessed.genome_assembly` | Reference genome (e.g., "GRCh38"); absent on FASTQ/image files |
| `extra.file_type` | `FileProcessed.file_type` / `FileFastq.file_type` | Semantic file type (e.g., "contact matrix") |
| `extra.file_type_detailed` | `FileProcessed.file_type_detailed` / `FileFastq.file_type_detailed` | Detailed type (e.g., "contact matrix (mcool)") |
| `extra.condition` | `track_and_facet_info.condition` | Experimental condition (e.g., "Formaldehyde+DSG, DpnII") |
| `extra.biosource_name` | `track_and_facet_info.biosource_name` | Cell line or tissue name (e.g., "GM12878") |
| `extra.dataset` | `track_and_facet_info.dataset` | Dataset description (e.g., "in situ Hi-C on GM12878") |
| `extra.experiment_type` | `track_and_facet_info.experiment_type` | Experiment type (e.g., "in situ Hi-C") |
| `extra.assay_info` | `track_and_facet_info.assay_info` | Assay details (e.g., "DpnII") |
| `extra.replicate_info` | `track_and_facet_info.replicate_info` | Replicate details (e.g., "Biorep 1, Techrep 1") |
| `extra.cell_line_tier` | Derived from `biosource_name` | 4DN cell line tier ("Tier 1" or "Tier 2"); only ~17 classified cell lines |
| `extra.extra_files` | `FileProcessed.extra_files` / `FileFastq.extra_files` | Array of associated index files (e.g., `.px2`, `.bai`) with `href`, `md5sum`, `file_size`, `file_format` |

Not all fields are present on every file. The enrichment uses `$set` with dot notation to merge into `extra` without overwriting the materializer's `enriched_file_format` field.

### 4DN Collection Enrichment

Before materialization, a pre-processing step fetches experiment metadata from the [4DN Search API](https://data.4dnucleome.org) and stores it on collection documents as `extra`. Since C2M2 collections map to 4DN experiments (`4DNEX*`) and experiment sets (`4DNES*`), this enriches collections with structured experiment-level data. The materializer then propagates `collection.extra` into `files.collections[].extra` via `coll.clone()`.

**Experiment types queried:** ExperimentHiC, ExperimentSeq, ExperimentDamid, ExperimentChiapet

| Collection Extra Field | Source | Description |
|------------------------|--------|-------------|
| `extra.display_title` | `Experiment.display_title` | Experiment display name |
| `extra.experiment_type` | `Experiment.experiment_type.display_title` | Experiment type (e.g., "in situ Hi-C", "ChIP-seq") |
| `extra.targeted_factor` | `Experiment.targeted_factor[].display_title` | Array of target proteins/marks (e.g., ["CTCF protein"], ["H3K27ac"]) |
| `extra.digestion_enzyme` | `Experiment.digestion_enzyme.display_title` | Restriction enzyme (e.g., "DpnII", "MboI") |
| `extra.lab` | `Experiment.lab.display_title` | Lab/PI name |
| `extra.crosslinking_method` | `Experiment.crosslinking_method` | Crosslinking method |
| `extra.crosslinking_temperature` | `Experiment.crosslinking_temperature` | Crosslinking temperature |
| `extra.crosslinking_time` | `Experiment.crosslinking_time` | Crosslinking time |
| `extra.ligation_temperature` | `Experiment.ligation_temperature` | Ligation temperature |
| `extra.ligation_volume` | `Experiment.ligation_volume` | Ligation volume |
| `extra.ligation_time` | `Experiment.ligation_time` | Ligation time |
| `extra.digestion_temperature` | `Experiment.digestion_temperature` | Digestion temperature |
| `extra.digestion_time` | `Experiment.digestion_time` | Digestion time |
| `extra.tagging_method` | `Experiment.tagging_method` | Tagging method (DamID) |
| `extra.fragmentation_method` | `Experiment.fragmentation_method` | Fragmentation method |
| `extra.biotin_removed` | `Experiment.biotin_removed` | Whether biotin was removed |
| `extra.library_prep_kit` | `Experiment.library_prep_kit` | Library prep kit used |
| `extra.average_fragment_size` | `Experiment.average_fragment_size` | Average fragment size |
| `extra.fragment_size_range` | `Experiment.fragment_size_range` | Fragment size range |
| `extra.status` | `Experiment.status` | Experiment status |
| `extra.date_created` | `Experiment.date_created` | Experiment creation date |

Not all fields are present on every experiment type. For example, `targeted_factor` is primarily available on ChIP-seq (ExperimentSeq), DamID, and ChIA-PET experiments.

## ENCODE Data Portal Filter Mapping

The following table maps ENCODE portal search dimensions to CFDB/C2M2 fields:

| Category | ENCODE Dimension | CFDB Field | Status | Notes |
|----------|------------------|------------|--------|-------|
| **File** | Accession | `local_id` | ✅ | Primary identifier |
| **File** | File Format | `file_format.name` | ✅ | EDAM CV terms (BAM, FASTQ, etc.) |
| **File** | File Size | `size_in_bytes` | ✅ | Integer bytes |
| **File** | MD5 Checksum | `md5` | ✅ | From md5sum field |
| **File** | Output Type | `data_type.name` | ✅ | EDAM data terms (alignments, peaks, etc.) |
| **File** | Output Type (raw) | `extra.output_type` | ✅ | Original ENCODE value |
| **File** | Download URL | `access_url` | ✅ | Direct from metadata TSV |
| **File** | Status | `status` | ✅ | e.g., "released" |
| **File** | Access Level | `data_access_level` | ✅ | Always "public" |
| **File** | Assembly | `extra.assembly` | ✅ | GRCh38, mm10, etc. |
| **File** | File Type | `extra.file_type` | ✅ | e.g., "alignments", "peaks" |
| **File** | Format Type | `extra.file_format_type` | ✅ | narrowPeak, broadPeak, etc. |
| **File** | Read Length | `extra.read_length` | ✅ | Sequencing read length |
| **File** | Mapped Read Length | `extra.mapped_read_length` | ✅ | Mapped read length |
| **File** | Run Type | `extra.run_type` | ✅ | single-ended/paired-ended |
| **File** | Paired End | `extra.paired_end` | ✅ | 1 or 2 for paired reads |
| **File** | Paired With | `extra.paired_with` | ✅ | Paired file accession |
| **File** | Index Of | `extra.index_of` | ✅ | Indexed file accession |
| **File** | Derived From | `extra.derived_from` | ✅ | Upstream file accessions |
| **File** | Controlled By | `extra.controlled_by` | ✅ | Control file accessions |
| **File** | s3 URI | `extra.s3_uri` | ✅ | S3 storage path |
| **File** | Azure URL | `extra.azure_url` | ✅ | Azure storage URL |
| **File** | Analysis Title | `extra.file_analysis_title` | ✅ | Analysis pipeline name |
| **File** | Analysis Status | `extra.file_analysis_status` | ✅ | Analysis pipeline status |
| **Experiment** | Accession | `extra.experiment_accession` | ✅ | Parent experiment |
| **Experiment** | Assay | `assay_type.name` | ✅ | OBI CV terms (ATAC-seq, ChIP-seq, etc.) |
| **Experiment** | Target | `extra.experiment_target` | ✅ | ChIP-seq target, etc. |
| **Experiment** | Date Released | `creation_time` | ✅ | ISO date |
| **Experiment** | Project | `extra.project` | ✅ | ENCODE project phase |
| **Experiment** | Lab | `extra.lab` | ✅ | Lab/PI name |
| **Experiment** | Platform | `extra.platform` | ✅ | Sequencing platform |
| **Experiment** | dbxrefs | `extra.dbxrefs` | ✅ | External cross-references |
| **Experiment** | Genome Annotation | `extra.genome_annotation` | ✅ | e.g., V29, M21 |
| **Sample** | Biosample Term ID | `collections.anatomy.id` | ✅ | EFO/CL/UBERON ontology IDs |
| **Sample** | Biosample Term Name | `collections.name`, `collections.anatomy.name` | ✅ | Cell type/tissue name |
| **Sample** | Biosample Type | `collections.biosamples.extra.biosample_type` | ✅ | primary cell, tissue, cell line, etc. |
| **Sample** | Organism | `collections.subjects.taxonomy.name` | ✅ | NCBI taxonomy (Homo sapiens, etc.) |
| **Sample** | Treatments | `collections.biosamples.extra.biosample_treatments` | ✅ | Treatment details |
| **Sample** | Genetic Modifications | `collections.biosamples.extra.biosample_genetic_modifications` | ✅ | CRISPR, RNAi, etc. |
| **Donor** | Donor ID | `collections.subjects.local_id` | ✅ | ENCODE donor accession |
| **Replicate** | Biological Replicate | `extra.biological_replicates` | ✅ | Biological replicate number(s) |
| **Replicate** | Technical Replicate | `extra.technical_replicates` | ✅ | Technical replicate number(s) |
| **Library** | Made From | `extra.library_made_from` | ✅ | RNA, DNA, etc. |
| **Library** | Depleted In | `extra.library_depleted_in` | ✅ | rRNA, etc. |
| **Library** | Extraction Method | `extra.library_extraction_method` | ✅ | |
| **Library** | Lysis Method | `extra.library_lysis_method` | ✅ | |
| **Library** | Crosslinking Method | `extra.library_crosslinking_method` | ✅ | |
| **Library** | Fragmentation Method | `extra.library_fragmentation_method` | ✅ | |
| **Library** | Strand Specific | `extra.library_strand_specific` | ✅ | |
| **Library** | Size Range | `extra.library_size_range` | ✅ | |
| **Provider** | DCC | `dcc.dcc_abbreviation` | ✅ | Always "ENCODE" |
| **Donor** | Sex | — | ❌ | Not in metadata TSV; requires per-donor API calls |
| **Donor** | Age | — | ❌ | Not in metadata TSV; requires per-donor API calls |
| **Sample** | Life Stage | — | ❌ | Not in metadata TSV |

**Legend:** ✅ Supported | ⚠️ Partial | ❌ Not Available

### ENCODE Integration Notes

Unlike 4DN and HuBMAP which use C2M2-formatted ZIP files from CFDE, ENCODE data is fetched from the ENCODE metadata TSV endpoint at `https://www.encodeproject.org/metadata/`. Key differences:

- **Data Source**: Single metadata TSV download (`/metadata/?type=Experiment&status=released`) instead of paginated JSON API or C2M2 ZIP files
- **Materialization**: Pre-materialized during sync (no Rust materializer needed)
- **File Access**: Direct HTTPS streaming (no DRS service)
- **Access Control**: All released files are publicly accessible
- **Biosample/Subject Data**: Biosample term, organism, and donor IDs are mapped to C2M2 collections, biosamples, and subjects; biosample-specific metadata (type, treatments, genetic modifications) is stored in `biosample.extra`
- **Missing Demographics**: Subject sex, age, and life stage are not available in the metadata TSV and would require per-donor API calls to `/human-donors/{id}/`
