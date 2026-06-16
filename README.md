# Common Fund Database

CFDB is a Python package for querying and serving enriched C2M2 (Crosscut Metadata Model) file metadata from Common Fund Data Coordinating Centers (DCCs) and Encode.

## Quickstart

### Installation

```bash
pip install git+https://github.com/abdenlab/cfdb.git
```

Requires Python 3.11 or later.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SYNC_API_KEY` | API key for the sync endpoint. If unset, sync is unprotected (suitable for local dev). | - |
| `SYNC_DATA_DIR` | Root directory for downloaded sync data, the workflow cache (`$SYNC_DATA_DIR/cache`), and per-job workdirs (`$SYNC_DATA_DIR/jobs`). When unset the preprocessing/indexing workflow subsystem is disabled: `/data` falls through to direct upstream streaming, `/index` returns 503 for processable formats (BAM/VCF/etc.) and 404 for passthrough formats (CSV/TSV/bigWig). Both subdirectories must share a filesystem because `LocalFsCache.put` relies on `os.replace` atomicity. | - |
| `WORKFLOW_WORKER_COUNT` | Number of wool workers to lease from the external worker pool. Must be >= 1. The API does NOT spawn workers in-process. | `2` |
| `WORKFLOW_POOL_NAMESPACE` | wool LAN discovery namespace shared by the API and the worker-pool process. Both processes MUST set the same value or dispatch will hang on `NoWorkersAvailable`. | `cfdb-workers` |
| `CFDB_WORKER_TLS_CA` | Path to the shared CA certificate for the wool worker gRPC channel. When this and the cert/key below are all set, the API↔worker dispatch channel uses mutual TLS (`mutual=True`); when all three are unset the channel stays plaintext. Partial config fails fast at startup. The API and every worker MUST use certs signed by the same CA. See [Worker mTLS](#worker-mtls). | - |
| `CFDB_WORKER_TLS_CERT` | Path to this process's PEM certificate on the worker gRPC channel — the worker leaf cert on a worker (`worker_main`/`worker_lan`), the API client cert on the API. Must be signed by `CFDB_WORKER_TLS_CA`. | - |
| `CFDB_WORKER_TLS_KEY` | Path to this process's PEM private key paired with `CFDB_WORKER_TLS_CERT`. | - |
| `CFDB_API_URL` | Base URL for the cfdb API | `http://localhost:8000` |
| `DATABASE_URL` | MongoDB connection string | `mongodb://127.0.0.1:27017` |
| `DATABASE_NAME` | Name of the MongoDB database to use | `cfdb` |
| `MONGODB_TLS_ENABLED` | Enable X.509 certificate authentication (production) | `false` |
| `MONGODB_CA_PATH` | Path to CA certificate bundle used when `MONGODB_TLS_ENABLED=true` | `/etc/cfdb/certs/global-bundle.pem` |
| `MONGODB_RETRY_WRITES` | Enable retryable writes on the MongoDB client | `false` |

### Docker Startup

```bash
# 1. Start MongoDB (restores sample data and creates indexes)
make mongodb

# 2. Start the API server
make api

# 3. (Optional) Sync latest DCC metadata
curl -X POST http://localhost:8000/sync
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
| `make worker-certs` | Generate the wool worker mutual-TLS material (CA + worker + API client certs) for local dev |
| `make worker-local-tls` | Start a local LAN worker pool with worker mTLS enabled |

### Deploying the CloudFormation stacks

Production runs on four CloudFormation stacks under `cloudformation/`. Deploy them in dependency order (each imports exports from the earlier ones):

1. `network.yml` — VPC, subnets, security groups (including the worker SG), S3 gateway endpoint.
2. `database.yml` — DocumentDB cluster and connection-URL secret.
3. `workers.yml` — wool worker task definition, S3 artifact cache, worker IAM roles.
4. `backend.yml` — API service, ALB, and the IAM/env wiring that dispatches to the worker fleet.

Tear down in the reverse order (`backend` → `workers` → `database` → `network`); `backend` imports the worker exports, so it must be deleted first. The `cfdb-wool` ECR repository is a prerequisite created out of band (like the `cfdb` repository); consider giving it an `IMMUTABLE` tag policy and a lifecycle policy to prune untagged images.

**Rolling the worker fleet.** The CI pipeline builds, scans, and pushes the worker image to `cfdb-wool:<sha>` on every merge to `master`, but it does not roll workers — the worker task definition pins `Image: !Ref WorkerImageURI`. To deploy a new worker image, redeploy the workers stack with the SHA-tagged URI:

```bash
aws cloudformation deploy \
  --stack-name <workers-stack> \
  --template-file cloudformation/workers.yml \
  --parameter-overrides WorkerImageURI=<account>.dkr.ecr.<region>.amazonaws.com/cfdb-wool:<sha> \
  --capabilities CAPABILITY_IAM
```

This registers a new task-definition revision that `EcsProvisioner` launches on the next workflow dispatch. Pin to an immutable `:<sha>` (not `:latest`) so the running fleet is traceable to a commit and rollback is a redeploy with the prior SHA.

**Tearing down the cache.** CloudFormation cannot delete a non-empty S3 bucket, so empty the `CacheBucket` before deleting the workers stack or the delete will fail and roll back.

**Worker mTLS on ECS (optional, off by default).** The same `CFDB_WORKER_TLS_*` gating that secures the local channel ([Worker mTLS](#worker-mtls)) is wired into the Fargate task definitions, but disabled unless you supply cert ARNs. Fargate cannot mount a Secrets Manager secret as a file, so the mechanism is: store each PEM as a Secrets Manager secret, inject them as env vars via the task definition's `Secrets:`, and let the image entrypoint (`scripts/cfdb-tls-entrypoint.sh`) write them to files and point `CFDB_WORKER_TLS_CA/CERT/KEY` at them before the app starts. To enable:

1. Generate certs (`make worker-certs`) and upload each PEM to Secrets Manager, e.g. `aws secretsmanager create-secret --name cfdb/worker-tls/ca --secret-string file://certs/worker-ca/ca.pem` (and the worker + API leaf cert/key).
2. Pass the ARNs to the workers stack (`WorkerTlsCaSecretArn`, `WorkerTlsCertSecretArn`, `WorkerTlsKeySecretArn`) and the backend stack (`ApiTlsCaSecretArn`, `ApiTlsCertSecretArn`, `ApiTlsKeySecretArn` — reuse the same CA secret). Supplying a CA ARN flips the per-stack condition that adds the `Secrets:` env and the least-privilege `secretsmanager:GetSecretValue` IAM. Leave them empty to keep dispatch plaintext.

> **Caveat — not yet functional on Fargate.** `EcsDiscovery` dials each worker at its *dynamic* awsvpc IP and wool does not override the gRPC authority, so a static worker cert's SAN cannot match the dialed address and the mTLS handshake will fail verification. The wiring above is in place, but **leave the cert ARNs empty** until wool gains a client-side target-name override (tracked separately). Until then, the worker security group is the access control on ECS.

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
| `dcc` | Dcc | The Data Coordinating Center that produced this file |
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

#### Dcc

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
| `lab` | string? | Lab/PI name (shared across 4DN and ENCODE) |
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

Anatomy, FileFormat, DataType, and AssayType share an identical schema: `id` (string), `name` (string), `description` (string?). NcbiTaxonomy adds an optional `clade` field.

| Entity | Ontology Source |
|--------|----------------|
| Anatomy | UBERON (Uber-anatomy ontology) |
| FileFormat | EDAM CV `format:` terms |
| DataType | EDAM CV `data:` terms |
| AssayType | OBI (Ontology for Biomedical Investigations) |
| NcbiTaxonomy | NCBI Taxonomy Database |

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
| `taxonomy` | NcbiTaxonomy? | NCBI taxonomy for the subject's organism |

#### Project

A node in the C2M2 project hierarchy.

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Human-readable label |
| `abbreviation` | string? | Short display label |
| `description` | string? | Human-readable description |

#### EnrichedFile

DCC-specific file-level metadata. Each DCC's fields are namespaced under a dedicated submodel.

| Field | Type | Description |
|-------|------|-------------|
| `fourdn` | EnrichedFourdnFile? | 4DN file-level metadata |
| `encode` | EnrichedEncodeFile? | ENCODE file-level metadata |
| `hubmap` | EnrichedHubmapFile? | HuBMAP file-level metadata |

**EnrichedFourdnFile** — 4DN file fields:

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

**EnrichedEncodeFile** — ENCODE file fields:

| Field | Type | Description |
|-------|------|-------------|
| `assembly` | string? | Genome assembly (GRCh38, mm10, etc.) |
| `file_format_type` | string? | narrowPeak, broadPeak, etc. |
| `output_type` | string? | Original ENCODE output type |
| `genome_annotation` | string? | Genome annotation version |
| `controlled_by` | string? | Control file accession(s) |
| `s3_uri` | string? | S3 URI |
| `azure_url` | string? | Azure Blob URL |
| `file_analysis_title` | string? | Analysis pipeline title |
| `file_analysis_status` | string? | Analysis pipeline status |
| `biological_replicates` | string? | Biological replicate(s) |
| `technical_replicates` | string? | Technical replicate(s) |
| `read_length` | string? | Read length |
| `mapped_read_length` | string? | Mapped read length |
| `run_type` | string? | Run type (single-ended, paired-ended) |
| `paired_end` | string? | Paired end designation |
| `paired_with` | string? | Paired-with file accession |
| `index_of` | string? | File this is an index of |
| `derived_from` | string? | Parent file accession(s) |
| `audit_warning` | string? | ENCODE audit warnings |
| `audit_not_compliant` | string? | ENCODE audit non-compliance |
| `audit_error` | string? | ENCODE audit errors |

**EnrichedHubmapFile** — HuBMAP file fields:

| Field | Type | Description |
|-------|------|-------------|
| `genome_assembly` | string? | Reference genome (e.g., "GRCh38") |
| `rel_path` | string? | Relative path within the dataset |
| `is_data_product` | bool? | Whether this file is a data product |

#### ExtraFile

An associated index or auxiliary file from 4DN.

| Field | Type | Description |
|-------|------|-------------|
| `href` | string? | Relative URL path on 4DN data portal |
| `md5sum` | string? | MD5 checksum |
| `file_size` | int? | File size in bytes |
| `file_format` | string? | Format identifier (e.g., "pairs_px2", "bai") |

#### EnrichedCollection

DCC-specific collection-level metadata. Each DCC's fields are namespaced under a dedicated submodel.

| Field | Type | Description |
|-------|------|-------------|
| `fourdn` | EnrichedFourdnCollection? | 4DN experiment metadata |
| `encode` | EnrichedEncodeCollection? | ENCODE experiment metadata |
| `hubmap` | EnrichedHubmapCollection? | HuBMAP dataset metadata |

**EnrichedFourdnCollection** — 4DN experiment fields:

| Field | Type | Description |
|-------|------|-------------|
| `display_title` | string? | Experiment display name |
| `experiment_type` | string? | Experiment type (e.g., "in situ Hi-C") |
| `targeted_factor` | string[]? | Target proteins/marks (e.g., ["CTCF protein"]) |
| `digestion_enzyme` | string? | Restriction enzyme (e.g., "DpnII") |

Additional protocol fields: `crosslinking_method`, `crosslinking_temperature`, `crosslinking_time`, `ligation_temperature`, `ligation_volume`, `ligation_time`, `digestion_temperature`, `digestion_time`, `tagging_method`, `fragmentation_method`, `biotin_removed`, `library_prep_kit`, `average_fragment_size`, `fragment_size_range`, `status`, `date_created`.

**EnrichedEncodeCollection** — ENCODE experiment fields:

| Field | Type | Description |
|-------|------|-------------|
| `experiment_target` | string? | ChIP-seq target, etc. |
| `project` | string? | ENCODE project phase |
| `platform` | string? | Sequencing platform |
| `dbxrefs` | string? | Database cross-references |
| `rbns_protein_concentration` | string? | RBNS protein concentration |

**EnrichedHubmapCollection** — HuBMAP dataset fields:

| Field | Type | Description |
|-------|------|-------------|
| `dataset_type` | string? | Dataset type (e.g., "RNAseq") |
| `pipeline` | string? | Processing pipeline |
| `processing` | string? | Processing status (raw, processed) |
| `group_name` | string? | TMC group name |
| `analyte_class` | string? | Analyte class (RNA, DNA, etc.) |
| `visualization` | bool? | Whether visualization is available |
| `vitessce_hints` | string[]? | Vitessce visualization hints |
| `metadata` | dict? | Full assay-specific metadata |

#### EnrichedBiosample

DCC-specific biosample-level metadata. Each DCC's fields are namespaced under a dedicated submodel.

| Field | Type | Description |
|-------|------|-------------|
| `encode` | EnrichedEncodeBiosample? | ENCODE biosample metadata |

**EnrichedEncodeBiosample** — ENCODE biosample fields:

| Field | Type | Description |
|-------|------|-------------|
| `biosample_type` | string? | primary cell, tissue, cell line, etc. |
| `biosample_treatments` | string? | Treatment details |
| `biosample_treatments_amount` | string? | Treatment amount |
| `biosample_treatments_duration` | string? | Treatment duration |
| `biosample_genetic_modifications` | string? | CRISPR, RNAi, etc. |
| `library_made_from` | string? | Source material (RNA, DNA, etc.) |
| `library_depleted_in` | string? | Depleted material (e.g., rRNA) |
| `library_extraction_method` | string? | Extraction method |
| `library_lysis_method` | string? | Lysis method |
| `library_crosslinking_method` | string? | Crosslinking method |
| `library_strand_specific` | string? | Strand specificity |
| `library_fragmentation_method` | string? | Fragmentation method |
| `library_size_range` | string? | Library size range |

### Entity Relationships

```
file
├── dcc (Dcc) ─────────────────── via submission field
├── project (Project) ─────────── via project FK
├── file_format (FileFormat) ──── via file_format ID
├── data_type (DataType) ──────── via data_type ID
├── assay_type (AssayType) ────── via assay_type ID
└── collections[] (Collection) ── via file_in_collection
    ├── anatomy[] (Anatomy) ───── via collection_anatomy
    ├── subjects[] (Subject) ──── via subject_in_collection
    │   └── taxonomy (NcbiTaxonomy) ── via subject_role_taxonomy
    └── biosamples[] (Biosample) ─ via biosample_in_collection
        ├── anatomy (Anatomy) ──── via anatomy ID
        └── subjects[] (Subject) ─ via biosample_from_subject
            └── taxonomy (NcbiTaxonomy) ── via subject_role_taxonomy
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
| 202 | Preprocessed artifact not yet cached — workflow dispatched. `Location: /jobs/{id}` and `Retry-After` headers point to the polling endpoint. |
| 206 | Partial content (Range request) |
| 400 | Invalid DCC, path-param shape, or Range header |
| 403 | File requires authentication (consortium/protected access) or denied by upstream repository |
| 404 | File not found, or HEAD probe of a not-yet-cached processed artifact (GET would dispatch) |
| 416 | Range not satisfiable (out of bounds, or file size unknown so no range can be satisfied) |
| 501 | No supported access method (e.g., Globus-only files) |
| 502 | Upstream service error |
| 503 | Workflow subsystem shutting down (`Retry-After`) |
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
| 202 | Index not yet cached — workflow dispatched. `Location: /jobs/{id}` and `Retry-After` headers point to the polling endpoint. |
| 206 | Partial content (Range request) |
| 400 | Invalid DCC, path-param shape, or Range header |
| 403 | File requires consortium/protected access (HuBMAP) |
| 404 | File not found, format has no index (CSV/TSV/bigWig), or HEAD probe of a not-yet-cached index |
| 416 | Range not satisfiable |
| 502 | Upstream service error or malformed sidecar |
| 503 | Workflow subsystem disabled (set `SYNC_DATA_DIR`) for a processable format, or shutting down (`Retry-After`) |

```bash
# Download an index file
curl -O http://localhost:8000/index/4dn/4DNFIG5NX1EC
```

### Preprocessing & indexing workflow

Many upstream files are not directly consumable by Gosling Designer without preprocessing (e.g., sort+index for BAM, bgzip+tabix for VCF/GFF/BED). When `/data` or `/index` is called for a format that needs preprocessing and the processed artifact is not yet in cache, the API dispatches a workflow and returns `202 Accepted` with a `Location` header pointing to a job status endpoint. A subsequent call, after the workflow completes, streams the processed artifact from cache (with `Range` support). Both endpoints share a single workflow per source file via a Mongo-backed mutex.

The preprocessed artifact is the default response. Clients that want the raw upstream file instead can pass `?raw=true`; on `/index`, `raw=true` serves only an upstream sidecar (e.g., 4DN's `extra_files`) and 404s when none exists. `HEAD` requests never dispatch preprocessing — on cache miss they return 404 so monitoring probes and prefetch tools cannot trigger workflows as a side-effect. Issue a `GET` when you actually want the artifact.

| Format | Workflow | Cached artifacts |
|--------|----------|------------------|
| CSV, TSV, bigWig | passthrough — served directly | — |
| BAM | header check (must be pre-sorted upstream) + index | BAI |
| SAM | SAM→BAM convert + sort + index | sorted BAM + BAI |
| VCF, GFF, GFF3, BED, BroadPeak, NarrowPeak | decompress + sort + bgzip + tabix | bgzipped text + TBI |
| GTF | GTF→GFF3 + sort + bgzip + tabix | bgzipped GFF3 + TBI |
| bigBed | bigBedToBed + sort + bgzip + tabix | bgzipped BED + TBI |

Cache keys are content-addressed using each file's upstream `md5`, so a byte change upstream (with the sync pipeline refreshing `md5`) invalidates the cache automatically.

`/index` continues to serve upstream sidecars first when present (the 218 BED→beddb and 4 BED→tbi 4DN cases that publish under `extra.extra_files` or `extra.fourdn.extra_files`); the workflow path is dispatched only when no sidecar exists. Set `?raw=true` to bypass the workflow path entirely and return only the upstream sidecar (404 when none exists).

Required environment variables:

- `SYNC_DATA_DIR` — directory under which the workflow cache and per-job workdirs live. Both subdirectories (`$SYNC_DATA_DIR/cache` and `$SYNC_DATA_DIR/jobs`) must share a filesystem because `LocalFsCache.put` relies on `os.replace` atomicity; the API asserts this at startup and fails fast if they live on different volumes. When unset, the workflow subsystem is disabled, `/data` falls through to direct upstream streaming, `/index` returns 404 for passthrough formats (CSV/TSV/bigWig — there is no index in any state of the world), and `/index` returns 503 for processable formats that would otherwise dispatch a workflow (sidecar-served files still work).
- `WORKFLOW_WORKER_COUNT` — number of wool workers to **lease** from the surrounding worker pool (default `2`). The API does *not* spawn workers in-process; workers must be externally provisioned (e.g., as a separate ECS service or via the local-dev launch below) and discoverable via the wool LAN discovery namespace.
- `WORKFLOW_POOL_NAMESPACE` — wool discovery namespace shared by the API and the external worker pool (default `cfdb-workers`). Both processes must agree on this value or dispatch will hang waiting for workers.

Optional tunables (with defaults):

- `CFDB_WORKFLOW_DURATION_CAP_S` — per-workflow wall-clock cap (default `14400`, i.e. 4 h — sized for multi-hour preprocessing runs; lower it for fixture-bound dev).
- `CFDB_WORKFLOW_DISPATCH_WAIT_S` — how long `ensure_workflow` waits for a free worker before giving up (default `60`).
- `CFDB_WORKFLOW_HEARTBEAT_INTERVAL_S` — cadence at which the wool routine emits heartbeat events during quiet stages so the API can refresh `JobRecord.updated_at` (default `300`). The stale-reclaim threshold below is sized as `2 × heartbeat + safety_margin`; lowering this knob without also lowering the threshold widens the false-reclaim window.
- `CFDB_WORKFLOW_STALE_THRESHOLD_S` — `updated_at` age beyond which an active row is reclaimable (default `900`; sized as `2 × heartbeat_interval + safety_margin` so a single missed heartbeat does not falsely reclaim a healthy worker).
- `CFDB_SAMTOOLS_THREADS` (default `1`), `CFDB_SORT_PARALLEL` (default `2`) — CPU/thread caps for `samtools sort/index` and GNU `sort` respectively.
- `CFDB_SAMTOOLS_MEMORY_CAP_PER_THREAD` (default `256M`), `CFDB_SORT_MEMORY_CAP` (default `256M`) — memory caps passed to the same tools. **`CFDB_SAMTOOLS_MEMORY_CAP_PER_THREAD` is per-thread**: total samtools RSS is bounded by `CFDB_SAMTOOLS_THREADS × CFDB_SAMTOOLS_MEMORY_CAP_PER_THREAD`. (The previous name `CFDB_SAMTOOLS_MEMORY_CAP` is rejected at import to surface deployments that haven't migrated; rename the env var.)

Required tools on `PATH` for the **worker pool** (not the API): `samtools`, `bgzip`, `tabix`, `bcftools`, `gffread`, `bigBedToBed`. The dedicated worker image `Dockerfile.wool` installs all of these and is the image ECS runs (build it with `make wool`, tagged `cfdb-wool`; its `CMD` is the ECS entrypoint `python -m cfdb.workflows.worker_main`). For single-host local dev where the toolchain is already on your `PATH`, skip the image entirely and run the LAN worker pool directly — see "Running a local worker pool" below.

#### ECS Fargate profile

When the API runs on ECS Fargate (or LocalStack-backed dev that mirrors prod end-to-end), the lifespan switches from `LocalFsCache` + `LanDiscovery` to `S3Cache` + `EcsDiscovery` + `EcsProvisioner`. The selection is env-driven; with none of the variables below set the API runs the local PoC profile unchanged.

- `AWS_ENDPOINT_URL` — boto3 endpoint override. Unset in production (boto3 hits real AWS); set to `http://localstack:4566` (or similar) for LocalStack-backed dev. The same application code runs in both environments — only this variable differs.
- `AWS_REGION` — AWS region for the boto3 client (default `us-east-1`).
- `WORKFLOW_S3_BUCKET` — when set, the lifespan instantiates `S3Cache` instead of `LocalFsCache`. The bucket must already exist (creation is out of band). When unset, the API stays on the local filesystem cache.
- `WORKFLOW_S3_PREFIX` — optional key prefix the S3 backend prepends to every cache key (default empty). Lets a single bucket host multiple environments (`dev/`, `staging/`, `prod/`) without collisions.
- `ECS_CLUSTER` — ECS cluster name or ARN. Gates the ECS-backed provisioner and discovery profile; unset means the PoC profile stays on `LanDiscovery` with no provisioner.
- `ECS_WORKER_TASK_DEFINITION` — task definition for the worker container, as a family name (`cfdb-worker`) or `family:revision`. The provisioner passes it through to `RunTask` verbatim.
- `ECS_WORKER_TASK_FAMILY` — family used by `EcsDiscovery` to filter `ListTasks`. Defaults to `ECS_WORKER_TASK_DEFINITION` with any `:revision` suffix stripped; set explicitly only when the discovery family differs from the provisioner task-def family (rare).
- `ECS_WORKER_SUBNETS` — comma-separated awsvpc subnet IDs the worker ENIs land in. Required for the ECS profile; an empty list with `ECS_CLUSTER` set is a misconfiguration.
- `ECS_WORKER_SECURITY_GROUPS` — comma-separated awsvpc security group IDs. Optional — when empty, ECS applies the VPC default SG.
- `ECS_WORKER_ASSIGN_PUBLIC_IP` — `ENABLED` or `DISABLED` (default `DISABLED`). Production should leave this disabled and reach AWS via VPC endpoints; LocalStack accepts either value.

The worker container's `CMD` is `python -m cfdb.workflows.worker_main`. Worker-side knobs (gRPC port, health port, max lifetime, drain grace) are documented under `--help` on that command; their env vars are `CFDB_WORKER_GRPC_PORT`, `CFDB_WORKER_HEALTH_PORT`, `CFDB_WORKER_MAX_LIFETIME_SECONDS`, and `CFDB_WORKER_DRAIN_GRACE_SECONDS`. The worker task definition MUST declare a `healthCheck` against the gRPC port; without one ECS reports `healthStatus: UNKNOWN` indefinitely and the worker is never advertised to discovery.

#### Running a local worker pool

For single-host development, start a wool worker pool in a separate process *before* launching the API, with `WORKFLOW_POOL_NAMESPACE` matching what the API uses:

```bash
# Publishes WORKFLOW_WORKER_COUNT workers (default 2) under
# WORKFLOW_POOL_NAMESPACE (default cfdb-workers) over LAN discovery.
python -m cfdb.workflows.worker_lan --namespace cfdb-workers --workers 2
# or, with defaults: make worker-local
```

This is the local-dev counterpart to the ECS entrypoint (`python -m cfdb.workflows.worker_main`): `worker_lan` spawns a `wool.WorkerPool` wired to `LanDiscovery` so the pool advertises its workers over zeroconf/mDNS, whereas `worker_main` boots a bare worker that `EcsDiscovery` finds by polling the ECS control plane. The API connects via LAN discovery and dispatches workflows to whatever workers are publishing under that namespace. With no worker pool running, `/data` and `/index` requests for processable formats will hang on the dispatch retry budget (60s by default) before failing with `NoWorkersAvailable`.

#### Worker mTLS

By default the API↔worker gRPC dispatch channel is plaintext, gated only by network reachability (in production, the worker security group). Setting the three `CFDB_WORKER_TLS_*` cert paths on **both** sides turns on wool's native mutual TLS (`mutual=True`): the channel is encrypted and each side presents a CA-signed certificate that the other verifies. wool's mTLS is peer-to-peer, so each process holds its own leaf cert/key while the CA is shared — the API and every worker MUST be signed by the same CA, or dispatch is rejected.

The configuration is gating-by-presence: when all three of `CFDB_WORKER_TLS_CA`, `CFDB_WORKER_TLS_CERT`, and `CFDB_WORKER_TLS_KEY` are unset the plaintext path is used unchanged (local PoC dev needs no certs); when all three are set mTLS is enforced. A *partial* configuration (some set, some not) fails fast at startup rather than silently degrading to plaintext.

Generate a local CA and the worker + API leaf certs with:

```bash
make worker-certs
# wraps ./certs/generate-worker-certs.sh — see --help for SAN options.
# Writes (all git-ignored):
#   certs/worker-ca/ca.pem            shared CA
#   certs/worker/worker-cert.pem,-key.pem   worker leaf
#   certs/api/api-cert.pem,-key.pem         API client leaf
```

Then point each process at the shared CA plus its own leaf. The worker pool:

```bash
CFDB_WORKER_TLS_CA=certs/worker-ca/ca.pem \
CFDB_WORKER_TLS_CERT=certs/worker/worker-cert.pem \
CFDB_WORKER_TLS_KEY=certs/worker/worker-key.pem \
python -m cfdb.workflows.worker_lan
# or, with these env vars baked in: make worker-local-tls
```

…and the API process, with the **same** `CFDB_WORKER_TLS_CA` but the API leaf:

```bash
export CFDB_WORKER_TLS_CA=certs/worker-ca/ca.pem
export CFDB_WORKER_TLS_CERT=certs/api/api-cert.pem
export CFDB_WORKER_TLS_KEY=certs/api/api-key.pem
```

The same three env vars configure the ECS worker entrypoint (`worker_main`) and the API on Fargate; distributing the certs to the ECS task definitions is tracked as follow-up work and is not yet wired into the CloudFormation deploy.

**URL:** `GET /jobs/{job_id}`

Poll the status of a dispatched workflow:

```json
{
  "job_id": "abc-123",
  "status": "running",
  "stages_done": ["data"],
  "artifacts": {"data": "encode/ENCFF123/data/abc-v1"},
  "progress": null,
  "error": null,
  "superseded_by": null
}
```

| Code | Description |
|------|-------------|
| 200 | Job status returned |
| 404 | Job not found |

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
# Sync all DCCs (no API key needed when SYNC_API_KEY is unset)
curl -X POST http://localhost:8000/sync

# Sync specific DCCs
curl -X POST "http://localhost:8000/sync?dccs=4dn&dccs=hubmap"

# With API key (required in production when SYNC_API_KEY is set)
curl -X POST -H "X-API-Key: your-key" http://localhost:8000/sync
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
