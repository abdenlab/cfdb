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
| `persistent_id` | string? | Permanent URI |
| `creation_time` | string? | ISO 8601 timestamp |
| `abbreviation` | string? | Short display label |
| `name` | string | Human-readable label |
| `description` | string? | Human-readable description |

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
├── file_format (FileFormat) ──── via file_format ID
├── data_type (DataType) ──────── via data_type ID
├── assay_type (AssayType) ────── via assay_type ID
└── collections[] (Collection)
    └── biosamples[] (Biosample)
        └── anatomy (Anatomy) ─── via anatomy ID
```

Files are linked to collections through a `file_in_collection` cross-reference table, and biosamples are linked to collections through a `biosample_in_collection` cross-reference table.

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
| **Sample** | Biosource/Cell Line | `collections.biosamples.local_id` | ⚠️ | Cell line in biosample ID |
| **Sample** | Organism | `collections.subjects.taxonomy.name` | ✅ | NCBI taxonomy |
| **Sample** | Cell Line Tier | — | ❌ | 4DN-specific |
| **Dataset** | Dataset/Collection | `collections.name` | ✅ | Collection grouping |
| **Dataset** | Publication/DOI | `collections.persistent_id` | ⚠️ | If DOI linked |
| **Dataset** | Condition | — | ❌ | 4DN-specific |
| **Provider** | DCC | `dcc.dcc_abbreviation` | ✅ | Always "4DN" |
| **Provider** | Lab/Project | `project.name` | ✅ | Via project FK |

**Legend:** ✅ Supported | ⚠️ Partial | ❌ Not Available
