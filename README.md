# Common Fund Database

CFDB is a Python package for querying and serving enriched C2M2 (Crosscut Metadata Model) file metadata from Common Fund Data Coordinating Centers (DCCs) and Encode.

## Quickstart

### Installation

```bash
pip install git+https://github.com/abdenlab/cfdb.git
```

Requires Python 3.10 or later.

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

### Docker Startup

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

### Production (TLS/X.509)

```bash
# 1. Generate certificates (customize hostname/IP as needed)
./certs/generate-certs.sh mongodb.example.com 10.0.1.50

# Or use environment variables
MONGODB_HOSTNAME=mongodb.example.com MONGODB_IP=10.0.1.50 ./certs/generate-certs.sh

# 2. Set a strong sync API key - this will be used to trigger metadata synchronization
export SYNC_API_KEY=<your-secret-key>

# 3. Start MongoDB with TLS
make mongodb-prod

# 4. Start API with client certificate
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

## GraphQL API

**URL:** `POST /metadata`

### Queries

The API exposes two queries: `files` (paginated list) and `file` (single lookup by MongoDB ObjectId).

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
    dcc { dccAbbreviation }
    fileFormat { name }
    collections {
      name
      biosamples { anatomy { name } }
    }
  }
}
```

```bash
# Query files from a specific DCC
curl -X POST http://localhost:8000/metadata \
  -H "Content-Type: application/json" \
  -d '{"query": "{ files(input: [{ dcc: [{ dccAbbreviation: [\"4DN\"] }] }]) { filename dcc { dccAbbreviation } } }"}'
```

Single file lookup: `{ file(id: "507f1f77bcf86cd799439011") { filename accessUrl } }`

### Query Mechanics

The GraphQL API uses an implicit OR/AND clause system for building MongoDB queries:

1. **Lists become OR clauses**: Multiple values in an array are combined with `$or`
2. **Dict keys become AND clauses**: Multiple fields in an object are combined with `$and`

Pagination is supported via `page` and `pageSize` parameters (defaults: 0 and 100).

#### OR Query - Multiple Values in a List

Find files with either filename:

```graphql
query {
  files(input: [{ filename: ["data.csv", "results.tsv"] }]) {
    filename
  }
}
```

MongoDB query: `{ "$or": [{ "filename": "data.csv" }, { "filename": "results.tsv" }] }`

#### Combined OR/AND Query

Find files from 4DN OR HuBMAP with a specific file format:

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

#### Nested Entity Query

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
    collections { biosamples { anatomy { name } } }
  }
}
```

### Data Model

The API serves file metadata following the C2M2 data model. Schema conventions:

- All C2M2 entities use composite keys: `id_namespace` + `local_id` (PK), optionally `project_id_namespace` + `project_local_id` (FK). These are omitted from individual tables below unless the entity has no other distinguishing fields.
- All entities include optional `persistent_id` and `creation_time` fields, omitted below.

#### FileMetadataModel

The central entity representing a stable digital asset.

| Field | Type | Description |
|-------|------|-------------|
| `dcc` | DCC | The Data Coordinating Center that produced this file |
| `collections` | Collection[] | Collections containing this file |
| `project` | Project? | The primary project within which this file was created |
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
| `status` | string? | Dataset status (e.g., "Published", "QA") |
| `data_access_level` | string? | Access level: public, consortium, or protected |
| `extra` | EnrichedFile? | DCC-specific file metadata (see EnrichedFile) |

#### DCC

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

#### Collection

A grouping of files, biosamples, and/or subjects.

| Field | Type | Description |
|-------|------|-------------|
| `biosamples` | Biosample[] | Biosamples in this collection |
| `subjects` | Subject[] | Subjects (donors) directly in this collection |
| `anatomy` | Anatomy[] | Anatomy terms associated with this collection |
| `abbreviation` | string? | Short display label |
| `name` | string | Human-readable label |
| `description` | string? | Human-readable description |
| `extra` | EnrichedCollection? | DCC-specific collection metadata (see EnrichedCollection) |

#### Biosample

A tissue sample or other physical specimen.

| Field | Type | Description |
|-------|------|-------------|
| `sample_prep_method` | string? | OBI CV term for preparation method |
| `anatomy` | Anatomy? | UBERON CV term for anatomical origin |
| `biofluid` | string? | UBERON/InterLex term for fluid origin |
| `subjects` | Subject[] | Subjects (donors) from which this biosample was derived |
| `extra` | EnrichedBiosample? | DCC-specific biosample metadata (see EnrichedBiosample) |

#### Ontology Types

Anatomy, FileFormat, DataType, and AssayType share an identical schema: `id` (string), `name` (string), `description` (string?). NCBITaxonomy adds an optional `clade` field.

| Entity | Ontology Source |
|--------|----------------|
| Anatomy | UBERON (Uber-anatomy ontology) |
| FileFormat | EDAM CV `format:` terms |
| DataType | EDAM CV `data:` terms |
| AssayType | OBI (Ontology for Biomedical Investigations) |
| NCBITaxonomy | NCBI Taxonomy Database |

#### Subject

A human or organism from which biosamples are derived.

| Field | Type | Description |
|-------|------|-------------|
| `granularity` | string? | CFDE CV term (single organism, cell line, microbiome, etc.) |
| `sex` | string? | NCIT CV term for biological sex |
| `ethnicity` | string? | NCIT CV term for self-reported ethnicity |
| `age_at_enrollment` | float? | Age in years when enrolled in primary project |
| `age_at_sampling` | float? | Age in years when biosample was taken |
| `race` | string[] | CFDE CV terms for self-identified race(s) |
| `taxonomy` | NCBITaxonomy? | NCBI taxonomy for the subject's organism |

#### Project

A node in the C2M2 project hierarchy.

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Human-readable label |
| `abbreviation` | string? | Short display label |
| `description` | string? | Human-readable description |

#### EnrichedFile

DCC-specific file-level metadata. Union of fields from 4DN and ENCODE enrichment pipelines.

**4DN fields:**

| Field | Type | Description |
|-------|------|-------------|
| `enriched_file_format` | string? | Derived format (mcool, hic, pairs, etc.) |
| `genome_assembly` | string? | Reference genome (e.g., "GRCh38") |
| `file_type` | string? | Semantic file type (e.g., "contact matrix") |
| `file_type_detailed` | string? | Detailed type (e.g., "contact matrix (mcool)") |
| `condition` | string? | Experimental condition |
| `biosource_name` | string? | Cell line or tissue name |
| `dataset` | string? | Dataset description |
| `experiment_type` | string? | Experiment type |
| `assay_info` | string? | Assay details |
| `replicate_info` | string? | Replicate details |
| `cell_line_tier` | string? | Cell line tier (Tier 1/Tier 2) |
| `extra_files` | ExtraFile[]? | Associated index files |

**ENCODE fields:**

| Field | Type | Description |
|-------|------|-------------|
| `assembly` | string? | Genome assembly (GRCh38, mm10, etc.) |
| `file_format_type` | string? | narrowPeak, broadPeak, etc. |
| `output_type` | string? | Original ENCODE output type |
| `experiment_accession` | string? | Parent experiment accession |
| `experiment_target` | string? | ChIP-seq target, etc. |
| `project` | string? | ENCODE project phase |
| `lab` | string? | Lab/PI name |

Additional ENCODE sequencing fields: `platform`, `read_length`, `mapped_read_length`, `run_type`, `paired_end`, `paired_with`, `biological_replicates`, `technical_replicates`.

Additional ENCODE library fields: `library_made_from`, `library_depleted_in`, `library_extraction_method`, `library_lysis_method`, `library_crosslinking_method`, `library_strand_specific`, `library_fragmentation_method`, `library_size_range`.

Additional ENCODE metadata fields: `genome_annotation`, `dbxrefs`, `controlled_by`, `index_of`, `derived_from`, `s3_uri`, `azure_url`, `file_analysis_title`, `file_analysis_status`, `rbns_protein_concentration`, `audit_warning`, `audit_not_compliant`, `audit_error`.

#### ExtraFile

An associated index or auxiliary file from 4DN.

| Field | Type | Description |
|-------|------|-------------|
| `href` | string? | Relative URL path on 4DN data portal |
| `md5sum` | string? | MD5 checksum |
| `file_size` | int? | File size in bytes |
| `file_format` | string? | Format identifier (e.g., "pairs_px2", "bai") |

#### EnrichedCollection

DCC-specific collection-level metadata from 4DN experiment API.

| Field | Type | Description |
|-------|------|-------------|
| `display_title` | string? | Experiment display name |
| `experiment_type` | string? | Experiment type (e.g., "in situ Hi-C") |
| `targeted_factor` | string[]? | Target proteins/marks (e.g., ["CTCF protein"]) |
| `digestion_enzyme` | string? | Restriction enzyme (e.g., "DpnII") |
| `lab` | string? | Lab/PI name |

Additional protocol fields: `crosslinking_method`, `crosslinking_temperature`, `crosslinking_time`, `ligation_temperature`, `ligation_volume`, `ligation_time`, `digestion_temperature`, `digestion_time`, `tagging_method`, `fragmentation_method`, `biotin_removed`, `library_prep_kit`, `average_fragment_size`, `fragment_size_range`, `status`, `date_created`.

#### EnrichedBiosample

DCC-specific biosample-level metadata from ENCODE.

| Field | Type | Description |
|-------|------|-------------|
| `biosample_type` | string? | primary cell, tissue, cell line, etc. |
| `biosample_treatments` | string? | Treatment details |
| `biosample_treatments_amount` | string? | Treatment amount |
| `biosample_treatments_duration` | string? | Treatment duration |
| `biosample_genetic_modifications` | string? | CRISPR, RNAi, etc. |

### Entity Relationships

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

### GraphiQL IDE

Visit [http://localhost:8000/metadata](http://localhost:8000/metadata) in your browser to access GraphiQL, an interactive IDE for exploring and testing GraphQL queries with schema docs, autocomplete, and query history.

## REST API

### File Streaming

**URL:** `GET /data/{dcc}/{local_id}` | `HEAD /data/{dcc}/{local_id}`

Stream file contents from DCCs via HTTPS.

**Path Parameters:**
- `dcc` - DCC abbreviation (e.g., `4dn`, `hubmap`, `encode`) - case insensitive
- `local_id` - The file's unique ID within the DCC

**Headers:**
- `Range` (optional) - `bytes=start-end` for partial content requests

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

```bash
# Check file availability
curl -I http://localhost:8000/data/4dn/abc123

# Download a file
curl -O http://localhost:8000/data/4dn/abc123

# Partial content
curl -H "Range: bytes=0-1023" http://localhost:8000/data/hubmap/xyz789
```

### Index File Streaming

**URL:** `GET /index/{dcc}/{local_id}` | `HEAD /index/{dcc}/{local_id}`

Stream index files (e.g., `.px2`, `.bai`) associated with DCC data files.

**Path Parameters:**
- `dcc` - DCC abbreviation (e.g., `4dn`) - case insensitive
- `local_id` - The file's unique ID within the DCC

**Headers:**
- `Range` (optional) - `bytes=start-end` for partial content requests

| Code | Description |
|------|-------------|
| 200 | Full index file content (GET) or file metadata (HEAD) |
| 206 | Partial content (Range request) |
| 400 | Invalid DCC or Range header |
| 404 | File not found or no index file available |
| 502 | Upstream service error |

```bash
# Download an index file
curl -O http://localhost:8000/index/4dn/4DNFIG5NX1EC
```

### Sync

**URL:** `POST /sync`

Trigger a sync of C2M2 datapackages from DCCs. Requires API key authentication.

- **Single sync at a time** - Concurrent requests return `409 Conflict`
- **Background execution** - Returns immediately with `202 Accepted` while sync runs in the background
- **Materialization** - After loading each DCC's data, the Rust materializer creates the denormalized `files` collection with all joins pre-computed
- **Database cutover** - During the clear/load phase, API requests are briefly blocked to ensure data consistency

**Headers:**
- `X-API-Key` (required) - API key matching `SYNC_API_KEY` environment variable

**Query Parameters:**
- `dccs` (optional, repeatable) - DCC names to sync. If omitted, syncs all DCCs.

| Code | Description |
|------|-------------|
| 202 | Sync started successfully |
| 401 | Invalid API key |
| 409 | A sync is already in progress |
| 500 | Server configuration error |

```bash
# Sync all DCCs
curl -X POST -H "X-API-Key: your-key" http://localhost:8000/sync

# Sync specific DCCs
curl -X POST -H "X-API-Key: your-key" "http://localhost:8000/sync?dccs=4dn&dccs=hubmap"
```

### Sync Status

**URL:** `GET /sync/{task_id}`

Check the status of a sync task. The `task_id` is returned when starting a sync.

```json
{
  "task_id": "abc-123",
  "status": "running",
  "dcc_names": ["4dn", "hubmap"],
  "started_at": "2024-01-15T10:30:00",
  "completed_at": null
}
```

| Code | Description |
|------|-------------|
| 200 | Task status returned |
| 404 | Task not found |

### CLI

```bash
# Sync all DCCs
cfdb sync

# Sync specific DCCs
cfdb sync 4dn hubmap
```

**Options:**
- `--api-url` - cfdb API base URL (default: `http://localhost:8000`, env: `CFDB_API_URL`)
- `--api-key` - API key for sync endpoint (env: `SYNC_API_KEY`)
- `--debug` / `-d` - Enable debugpy debugging
