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
| `WORKFLOW_WORKER_COUNT` | Local-dev only: number of workers the LAN pool (`worker_lan`) spawns. The API no longer leases a fixed count — its pool admits every discovered worker. In the ECS profile the concurrent worker fleet is bounded by `ECS_MAX_WORKERS`, with the AWS Fargate vCPU service quota as the hard ceiling. | `2` |
| `ECS_MAX_WORKERS` | ECS profile only: cap on concurrently-running ephemeral Fargate worker tasks. The provisioner counts the fleet before each `RunTask` and queues rather than spawning past this, so it bounds workers without limiting the queue (that is `CFDB_WORKFLOW_MAX_ACTIVE`). `0` disables the cap. | `16` |
| `WORKFLOW_POOL_NAMESPACE` | wool LAN discovery namespace shared by the API and the worker-pool process. Both processes MUST set the same value or dispatch will hang on `NoWorkersAvailable`. | `cfdb-workers` |
| `CFDB_WORKER_TLS_CA` | Path to the shared CA certificate for the wool worker gRPC channel. When this and the cert/key below are all set, the API↔worker dispatch channel uses mutual TLS (`mutual=True`); when all three are unset the channel stays plaintext. Partial config fails fast at startup. The API and every worker MUST use certs signed by the same CA. See [Worker mTLS](#worker-mtls). | - |
| `CFDB_WORKER_TLS_CERT` | Path to this process's PEM certificate on the worker gRPC channel — the worker leaf cert on a worker (`worker_main`/`worker_lan`), the API client cert on the API. Must be signed by `CFDB_WORKER_TLS_CA`. | - |
| `CFDB_WORKER_TLS_KEY` | Path to this process's PEM private key paired with `CFDB_WORKER_TLS_CERT`. | - |
| `CFDB_WORKER_TLS_IDENTITY` | Logical name the **API** verifies worker certificates against, in place of the address it dialed. Workers answer on addresses assigned at launch — an awsvpc IP on Fargate, a bridge IP in local containers — that no certificate minted ahead of time can name, so without this the handshake cannot succeed. The worker leaf must carry this value as a SAN; `certs/generate-worker-certs.sh` mints the default. Set to the empty string to verify against the dialed address instead. Read by every process that dials: the API on the dispatch channel, and each worker on the graceful-stop channel wool opens back to its own subprocess — so the API and the workers MUST agree on this value, or drain fails its name check and in-flight work is lost with no TLS error anywhere. Ignored while mTLS is off. See [Worker mTLS](#worker-mtls). | `cfdb-worker` |
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
| `make schema` | Regenerate the checked-in `schema.graphql` from the Strawberry schema |
| `make certs` | Generate TLS certificates for production |
| `make mongodb-prod` | Start MongoDB with TLS/X.509 authentication |
| `make api-prod` | Start API with X.509 client certificate |
| `make worker-certs` | Generate the wool worker mutual-TLS material (CA + worker + API client certs) for local dev |
| `make worker-local-tls` | Start a local LAN worker pool with worker mTLS enabled |

### Deploying the CloudFormation stacks

Each deployed environment runs on the same four CloudFormation stacks under `cloudformation/`, deployed under environment-specific stack names. Deploy them in dependency order (each imports exports from the earlier ones):

1. `network.yml` — VPC, subnets, security groups (including the worker SG), S3 gateway endpoint.
2. `database.yml` — DocumentDB cluster and connection-URL secret.
3. `workers.yml` — wool worker task definition, S3 artifact cache, worker IAM roles.
4. `backend.yml` — API service, ALB, and the IAM/env wiring that dispatches to the worker fleet.

Tear down in the reverse order (`backend` → `workers` → `database` → `network`); `backend` imports the worker exports, so it must be deleted first. Both the `cfdb` and `cfdb-wool` ECR repositories are prerequisites created out of band. Each MUST allow tag mutability (`MUTABLE`), because the moving-tag deploys re-push the `:dev` and `:prod` tags and an `IMMUTABLE` repository would reject the second push of either. If you want `:<sha>` tags to stay immutable while still allowing the moving tags to be re-pushed, configure ECR `IMMUTABLE_WITH_EXCLUSION` with wildcard filters exempting both `dev` and `prod` (so `:<sha>` is immutable and the moving tags are mutable). Give both repositories a lifecycle policy to prune untagged images.

#### Environments

Two environments share the account `605134458779` and region `us-east-2`. Every cross-stack export is `${AWS::StackName}`-scoped, so the two sets of stacks coexist without collisions — but the physical names that AWS requires to be account- and region-unique (the ALB target group in particular) MUST be passed per environment, which is why `TargetGroupName` is a parameter.

| | dev | prod |
|---|---|---|
| Stack names | `cfdb-network-dev`, `cfdb-db-dev`, `cfdb-workers-dev`, `cfdb-backend-dev` | `cfdb-network-prod`, `cfdb-db-prod`, `cfdb-workers-prod`, `cfdb-backend-prod` |
| Domain | `dev.cfdb.vis-api.link` | `cfdb.visualizationhub.org` |
| Hosted zone | `vis-api.link` (`Z09477406JQAR0KB7G87`) | `visualizationhub.org` (`Z02332581YW11QLMXBXY4`) |
| Moving tag | `:dev` | `:prod` |
| VPC CIDR | `10.2.0.0/21` | `10.3.0.0/21` |
| Ships when | every merge to `master`, automatically | a human dispatches **Promote CFDB to prod** and a reviewer approves |

Note the templates' parameter defaults (`cfdb.vis-api.link`, `10.2.0.0/21`, `cfdb-tg`) match *neither* live environment — they exist only so a bare `cloudformation deploy` is not rejected for missing parameters. Always pass explicit overrides.

One parameter deliberately breaks that convention: `workers.yml`'s `BackendClusterName` has **no default**, so a deploy that omits it is rejected outright. The convention rests on a wrong value failing visibly — a wrong `DomainName` is the wrong DNS, a wrong `TargetGroupName` collides — and that does not hold for a value which scopes an IAM grant. A placeholder there deploys cleanly, attaches `ecs:TagResource` to a cluster that does not exist, and leaves every worker failing the same `AccessDenied` the grant exists to prevent, with `UPDATE_COMPLETE` on screen. CloudFormation reuses stored parameter values on update, so it only has to be supplied on a stack's first deploy.

#### Deploying the prod stacks

One-time, run by a principal holding `cloudformation:*`, `iam:PassRole`, and `iam:PutRolePolicy` — see [Which principal can run a privileged deploy](#which-principal-can-run-a-privileged-deploy), because the obvious candidate is not sufficient. The CIDRs deliberately do not overlap dev's so the two VPCs can be peered later if needed.

```bash
aws cloudformation deploy --region us-east-2 \
  --stack-name cfdb-network-prod \
  --template-file cloudformation/network.yml \
  --parameter-overrides \
    CidrBlock=10.3.0.0/21 \
    CidrPublicSubnetA=10.3.0.0/24 CidrPublicSubnetB=10.3.1.0/24 \
    CidrPrivateSubnetA=10.3.2.0/24 CidrPrivateSubnetB=10.3.3.0/24 \
  --capabilities CAPABILITY_IAM

aws cloudformation deploy --region us-east-2 \
  --stack-name cfdb-db-prod \
  --template-file cloudformation/database.yml \
  --parameter-overrides \
    NetworkStackName=cfdb-network-prod \
    DBMasterUsername=<username> \
  --capabilities CAPABILITY_IAM

aws cloudformation deploy --region us-east-2 \
  --stack-name cfdb-workers-prod \
  --template-file cloudformation/workers.yml \
  --parameter-overrides \
    WorkerImageURI=605134458779.dkr.ecr.us-east-2.amazonaws.com/cfdb-wool:prod \
    BackendClusterName=cfdb-backend-prod-cluster \
  --capabilities CAPABILITY_IAM

aws cloudformation deploy --region us-east-2 \
  --stack-name cfdb-backend-prod \
  --template-file cloudformation/backend.yml \
  --parameter-overrides \
    NetworkStackName=cfdb-network-prod \
    DatabaseStackName=cfdb-db-prod \
    WorkersStackName=cfdb-workers-prod \
    ImageURI=605134458779.dkr.ecr.us-east-2.amazonaws.com/cfdb:prod \
    DomainName=cfdb.visualizationhub.org \
    HostedZoneName=visualizationhub.org \
    HostedZoneId=Z02332581YW11QLMXBXY4 \
    TargetGroupName=cfdb-prod-tg \
  --capabilities CAPABILITY_IAM
```

Because both prod task definitions already reference `:prod`, this doubles as the prod bootstrap — but the `:prod` tags must exist in ECR *before* the stacks can pull them, so run one promotion (below) first, or seed `:prod` by re-tagging a known-good `:<sha>`.

#### Promoting to prod

Production never advances automatically. `.github/workflows/promote-to-prod.yml` is `workflow_dispatch`-only and gated on the `prod` GitHub Environment's required reviewers. Dispatch it with the 8-character `:<sha>` tag that the dev pipeline already built, scanned, and shipped; it verifies that tag exists in **both** `cfdb` and `cfdb-wool`, re-tags each onto `:prod` by copying the ECR manifest, rolls the prod API service, and waits for `services-stable`.

Promotion re-tags rather than rebuilds, so prod runs bytes byte-identical to what dev validated. **Rollback is the same operation with an older SHA:** dispatch the workflow again naming the previous `:<sha>`. As on dev, the worker fleet needs no ECS step — the prod worker task def also references `:prod` and workers are ephemeral, so the next `EcsProvisioner` `RunTask` pulls the new image. That does mean the API rolls immediately while in-flight workers may still run the prior image until they drain, so keep the API↔worker dispatch contract compatible across adjacent promotions.

Prod requires this one-time setup before the first promotion:

- A `prod` **GitHub Environment** with required reviewers, carrying two environment variables: `PROD_BACKEND_CLUSTER` (`cfdb-backend-prod-cluster`) and `PROD_BACKEND_SERVICE` (`cfdb-backend-prod-service`). The workflow fails its pre-flight before touching ECR if either is unset.
- Both ECR repositories accepting the `:prod` moving tag (see the mutability note above).

As with dev, confirm after the first promotion that the running prod tasks reference `:prod`. If the task definitions still pin a `:<sha>`, promotions will report success while the live code never advances.

**CI auto-deploy to dev (moving-tag steady state).** This pipeline ships **dev only**; prod is promoted manually as described above. Every merge to `master` runs `.github/workflows/deploy-to-ecr.yml`, which builds the API (`cfdb`) and worker (`cfdb-wool`) images, trivy-scans the worker image, pushes both images, then rolls the API service — using only the permissions the `cfdb-deploy` CI role already holds (ECR push/pull on both repos, and `ecs:UpdateService`/`DescribeServices`/`DescribeTasks`/`ListTasks` on `cfdb-backend-dev-cluster`). Only the worker (`cfdb-wool`) image is trivy-scanned for HIGH/CRITICAL vulnerabilities — the worker shells out to `samtools`/`tabix`/`bigBedToBed` over untrusted upstream bytes — and that scan gates both pushes, so a scan failure leaves ECR fully on the prior images; the API image is not scanned. The role has **no** `cloudformation:*`, `ecs:RegisterTaskDefinition`, or `iam:PassRole`, so deploys no longer run `aws cloudformation deploy`; they use the **moving-tag** pattern instead. Each image is pushed to two tags: the immutable `:<sha>` (traceable to a commit, used for audit and rollback) and a moving, environment-scoped `:dev`. The ECS task definitions reference `:dev`. The workflow then runs `aws ecs update-service --force-new-deployment` on the backend service, which launches fresh Fargate tasks that re-pull `:dev` — shipping the new API code without registering a task definition — and waits for the rollout to stabilize with `aws ecs wait services-stable`. The worker fleet needs no ECS step: the worker task def also references `:dev`, and the workers are ephemeral, so the next `EcsProvisioner` `RunTask` (issued by the API role at workflow dispatch) pulls the freshly-pushed image. **Rollback is an ECR re-tag, not a redeploy:** re-tag the desired `:<sha>` onto `:dev` (`docker pull` the old `:<sha>`, re-tag it `:dev`, `docker push`; or `aws ecr batch-get-image` + `put-image`, both granted to the role), then `aws ecs update-service --force-new-deployment` to roll the API onto it (the workers re-pull on their next dispatch).

A few operational caveats of the moving-tag pattern:

- **Version skew across a deploy.** The API service rolls immediately, but the worker fleet only advances when the next `EcsProvisioner` `RunTask` pulls the freshly-pushed `:dev`. Between the API roll and the next worker launch, a newly-rolled API can dispatch to in-flight workers still running the prior `:dev` image — so a deploy has a transient window where the API and worker code can be one commit apart. Keep the API↔worker dispatch contract backward-compatible across adjacent commits.
- **A wool version bump is a deliberate exception to that.** wool admits a worker only when the proxy's version is `<=` the worker's within the same major (`is_version_compatible`), applied as a discovery filter, and `wool.protocol.__version__` is just the installed package version. Since the pipeline rolls the API first, a bump means the new API rejects **every** in-flight worker until the fleet turns over. It self-heals — jobs stay `pending` and the durable scheduler drains them once fresh workers spawn — but two second-order effects are worth knowing. Rejected-but-running workers still count toward `ECS_MAX_WORKERS` in the provisioner's pre-`RunTask` census, and nothing reaps them before `CFDB_WORKER_MAX_LIFETIME_SECONDS` (5 h, longer than the 4 h dispatch deadline), so a bump landing on a near-capped fleet can wedge spawning long enough to fail jobs `capacity:`. And the symptom is `NoWorkersAvailable`, indistinguishable from the TLS failures above. To make it a non-event, drain the fleet as part of the deploy that carries the bump — `aws ecs list-tasks --cluster <cluster> --family <worker-family>` then `stop-task` on each, or simply confirm it is empty before promoting. The rollback direction is safe: an older API against newer workers passes the gate.
- **Single-environment moving tag.** `MOVING_TAG` is hard-coded to `dev` in the workflow, so this pipeline targets exactly one environment. A second environment (e.g. `prod`) would need its own moving tag, repo variables, and task-def wiring — not yet parameterized.
- **Rollout wait ceiling.** `aws ecs wait services-stable` polls for up to ~10 minutes (40 attempts × 15 s) before timing out. A genuinely slow or wedged rollout will fail the workflow at that ceiling even though the `update-service` call itself succeeded; the deploy may still converge afterward, or the circuit breaker (below) may roll it back.
- **Stale GitHub secrets.** The old `BACKEND_STACK_NAME` and `WORKERS_STACK_NAME` GitHub secrets are no longer used by this workflow (it no longer runs `cloudformation deploy`) and can be deleted.

This needs two pieces of one-time configuration:

- **GitHub repo variables** (in addition to the existing `AWS_IAM_ROLE` secret): `BACKEND_CLUSTER` (= `cfdb-backend-dev-cluster`) and `BACKEND_SERVICE` (= `cfdb-backend-dev-service`) — the ECS cluster and service the "Roll API service" / "Wait for API rollout" steps target via `aws ecs update-service`. These are GitHub **variables** (`vars.*`), not secrets. `backend.yml` now sets an explicit `ServiceName: ${AWS::StackName}-service` so `BACKEND_SERVICE` is the deterministic `<stack>-service` (e.g. `cfdb-backend-dev-service`) rather than a CloudFormation-generated name. **Caveat:** introducing that explicit name forces a ONE-TIME service replacement on the next backend `cloudformation deploy` (the service is recreated under the new name), after which `BACKEND_SERVICE` must be set to `<stack>-service`.
- **One-time privileged bootstrap** (run once, by a principal holding `cloudformation:*`, `iam:PassRole`, and `iam:PutRolePolicy` — see [Which principal can run a privileged deploy](#which-principal-can-run-a-privileged-deploy); the `cfdb-deploy` CI role has none of them). Flip both task definitions from a `:<sha>` image to the moving `:dev` tag with a single `cloudformation deploy` per stack, after which all steady-state deploys run on the CI role's existing permissions:

> **Failure mode if the bootstrap is skipped — the deploy reports green but ships nothing.** The CI workflow only pushes images and force-rolls the service; it never touches the task definition. Until both task defs reference `:dev`, the service keeps re-pulling whatever SHA the task def still pins, so every CI run will pass (`update-service` and `services-stable` both succeed) while the live code never advances. Always run the bootstrap once before relying on CI deploys, and confirm the running tasks reference `:dev` afterward. The "Verify live image is :dev" guard step below performs a best-effort check of this on every deploy and warns (without failing the build) when the running task's image is not `:dev`; it inspects `aws ecs describe-services` → `aws ecs describe-tasks` because the `cfdb-deploy` role intentionally lacks `ecs:DescribeTaskDefinition`.

```bash
aws cloudformation deploy \
  --region us-east-2 \
  --stack-name <workers-stack> \
  --template-file cloudformation/workers.yml \
  --parameter-overrides \
    WorkerImageURI=605134458779.dkr.ecr.us-east-2.amazonaws.com/cfdb-wool:dev \
    BackendClusterName=<backend-stack>-cluster \
  --capabilities CAPABILITY_IAM
aws cloudformation deploy \
  --region us-east-2 \
  --stack-name <backend-stack> \
  --template-file cloudformation/backend.yml \
  --parameter-overrides ImageURI=605134458779.dkr.ecr.us-east-2.amazonaws.com/cfdb:dev \
  --capabilities CAPABILITY_IAM
```

The `cloudformation/backend.yml` `ImageURI` and `cloudformation/workers.yml` `WorkerImageURI` parameters now **default** to these `:dev` URIs. Note what that default does and does not do: on a stack **UPDATE**, CloudFormation reuses each parameter's **previous** value, not its default — so the `:dev` default only governs a fresh stack **CREATE**. The point is that a later infra `cloudformation deploy` (an update) keeps whatever value the bootstrap set — `:dev` — rather than reverting to a stale SHA, so the moving-tag CI deploy stays the source of truth for what code runs. Because the task defs pin `:dev` rather than a SHA, task-definition-level traceability to a commit is intentionally given up; it is recovered by the immutable `:<sha>` tag pushed alongside `:dev` and by SHA-based rollback via ECR re-tag.

> **One-time privileged bootstrap #2 — the `ecs:TagResource` grant, ordered BEFORE the image.** Worker images that publish their own metadata (the `wool.version`/`wool.secure` task tags `EcsDiscovery` requires) **exit at startup** when their task role lacks `ecs:TagResource` — deliberately, because an unpublishable worker can never be discovered and would otherwise hold a Fargate slot while unable to receive work. That grant lives in `cloudformation/workers.yml`, and the CI pipeline **cannot deploy it**: the `cfdb-deploy` role holds no `cloudformation:*`, so merging ships the tag-or-die image while the deployed task role still lacks the permission. The failure mode is a fleet that empties itself — every `RunTask` launches a worker that logs `ecs:TagResource denied … exiting` and dies, jobs sit `pending` to the 4 h deadline, and the API-side symptom is the same `NoWorkersAvailable` as every other failure in this section — while CI reports green. **Before the first image carrying metadata publishing reaches an environment**, a principal holding `cloudformation:*`, `iam:PassRole`, and `iam:PutRolePolicy` (see [below](#which-principal-can-run-a-privileged-deploy)) must run, per environment:
>
> ```bash
> aws cloudformation deploy --region us-east-2 \
>   --stack-name cfdb-workers-<env> \
>   --template-file cloudformation/workers.yml \
>   --parameter-overrides BackendClusterName=cfdb-backend-<env>-cluster \
>   --capabilities CAPABILITY_IAM
> ```
>
> The forward order is harmless — an old image simply ignores the extra permission — so deploy the stack first, then merge. The same ordering applies to prod before the first promotion carrying this change (`promote-to-prod.yml` runs no CloudFormation either). Confirm with a worker log line `Published worker metadata to <arn>`; the denial, if you got the order wrong, is an `AccessDeniedException` in the worker's CloudWatch group and a rate-limited `none advertisable` warning in the API's.

#### Which principal can run a privileged deploy

Every bootstrap above needs `cloudformation:*`, **`iam:PassRole`**, and **`iam:PutRolePolicy`**. `PassRole` is required because any template change touching a container definition registers a new ECS task definition, which passes the task and execution roles; `PutRolePolicy` because these templates own inline policies on those roles.

**`AWSReservedSSO_PowerUserAccess` is not sufficient**, and it is the trap: it is the role a cfdb operator actually holds, and it is *not* the `cfdb-deploy` CI role, so it reads as qualifying. But PowerUserAccess excludes `iam:*`. The deploy gets as far as building a changeset and then fails:

```
Resource handler returned message: "Access denied for operation 'Create TaskDefinition
Access denied: User: arn:aws:sts::605134458779:assumed-role/AWSReservedSSO_PowerUserAccess_.../...
is not authorized to perform: iam:PassRole on resource:
arn:aws:iam::605134458779:role/cfdb-workers-dev-WorkerTaskRole-...
```

A refused attempt is safe: the stack rolls back to `UPDATE_ROLLBACK_COMPLETE` with nothing partially applied, so the cost is a round trip rather than a broken stack. Log in with an `AdministratorAccess`-equivalent permission set and re-run the identical command — these deploys are idempotent.

**Tearing down the cache.** CloudFormation cannot delete a non-empty S3 bucket, so empty the `CacheBucket` before deleting the workers stack or the delete will fail and roll back.

#### Worker mTLS on ECS

**Optional, off by default — and deliberately left off.** The same `CFDB_WORKER_TLS_*` gating that secures the local channel ([Worker mTLS](#worker-mtls)) is wired into the Fargate task definitions, but disabled unless you supply cert ARNs. Fargate cannot mount a Secrets Manager secret as a file, so the mechanism is: store each PEM as a Secrets Manager secret, inject them as env vars via the task definition's `Secrets:`, and let the image entrypoint (`scripts/cfdb-tls-entrypoint.sh`) write them to files and point `CFDB_WORKER_TLS_CA/CERT/KEY` at them before the app starts.

**Why it stays off.** On AWS the marginal value is small and the operational cost is not. The worker security group already restricts inbound `50051` to the API's security group alone — there is no worker-to-worker rule, so a compromised worker cannot reach its peers — and only public data enters the pipeline, since `enforce_hubmap_access` rejects anything whose `data_access_level` is not `public` before dispatch. Fargate runs on Nitro, where intra-VPC traffic between tasks is already encrypted at the link layer, so this is not the difference between plaintext and encrypted. What mTLS would add is a backstop if the security group is ever widened, and authentication that does not depend on network position. What it costs is a CA to guard, a non-atomic rotation procedure, certificates that expire with nothing watching them, and a failure mode whose only symptom is a job that never moves. The capability is here so the decision can be revisited; the recommendation is to leave the cert ARNs empty.

Locally the calculation is different — there is no security group, developer machines share networks, and it is the configuration the tests exercise. See [Worker mTLS](#worker-mtls).

**If you do enable it**, three things have to be true together, and the middle one is the part that is easy to miss:

1. Every PEM is in Secrets Manager and the ARNs are passed to both stacks — the workers stack (`WorkerTlsCaSecretArn`, `WorkerTlsCertSecretArn`, `WorkerTlsKeySecretArn`) and the backend stack (`ApiTlsCaSecretArn`, `ApiTlsCertSecretArn`, `ApiTlsKeySecretArn`, reusing the same CA secret). Supplying a CA ARN flips the per-stack condition that adds the `Secrets:` env and the least-privilege `secretsmanager:GetSecretValue` IAM.
2. The worker leaf carries the same `WorkerTlsIdentity` (default `cfdb-worker`) as a SAN, and **both stacks are given the same value** — the backend stack's parameter is what the API verifies dispatch against, and the workers stack's parameter is what each worker verifies on the graceful-stop channel wool dials back to its own subprocess. `EcsDiscovery` reaches each worker at the awsvpc IP assigned at launch, so verification uses that logical name rather than the dialed address; a leaf without the SAN cannot be verified at all. Letting the two stacks drift is the quiet failure: dispatch works, and every graceful drain fails its name check, force-reaping the subprocess and losing in-flight work.
3. Both stacks are enabled together. wool's admission gate is symmetric: a proxy holding credentials admits only workers advertising `secure=true`, and a proxy without credentials admits only workers without. Turning mTLS on in one stack and not the other empties the pool.

**Diagnosing a failed handshake.** wool logs it. `WorkerProxy` emits a rate-limited warning per worker under the `wool.runtime.worker.proxy` logger, carrying the gRPC status and detail, and the API's root logger is at `INFO` so it reaches CloudWatch:

```
WARNING wool.runtime.worker.proxy Skipping worker 0b49fcd9-… at 10.3.2.7:50051 after handshake failure:
UNAVAILABLE: … UNAUTHENTICATED: Hostname Verification Check failed.
```

Grep for `after handshake failure`. The detail distinguishes the cases — `Hostname Verification Check failed` is a SAN mismatch, a CA mismatch and an expired leaf read differently. The API also logs `worker_tls_identity=` at startup, which tells you what it expected to match; that is corroboration, not the diagnosis. One genuine blind spot remains: a *worker* rejecting the API's client certificate surfaces as a plain `UNAVAILABLE` with no TLS evidence, observable only on the worker.

Note what a handshake failure looks like from outside: `HandshakeError` is transient in wool, so the worker is skipped rather than evicted, and dispatch reports `NoWorkersAvailable`. Nothing in the client-visible error names TLS.

#### Operating the worker certificates

The mechanics below are not recoverable from the code, and they are needed exactly when someone first enables mTLS. There is no tooling for any of it beyond `generate-worker-certs.sh`, which is local-dev tooling — it mints usable material, but its CA key is unencrypted and lands wherever you ran it, so treat production material as something you mint deliberately (CloudShell, or an offline host) rather than as a by-product of `make`.

**Give each environment its own CA.** dev and prod share account `605134458779`. A CA is a trust root, so one CA across both environments means a leaked *dev* worker certificate authenticates to *prod* workers — the blast radius of the less-guarded environment becomes the blast radius of the guarded one. Mint and upload two independent sets, and never point a prod stack at a dev ARN.

**Archive each CA key before minting the next.** The script writes the CA to the fixed path `certs/worker-ca/ca-key.pem` and regenerates it in place under `--force`, and the CA key is deliberately *not* uploaded to Secrets Manager. So minting a second environment overwrites the first environment's CA key, after which no replacement leaf can ever be issued for it and the only recovery is the full CA rotation below. Copy `ca-key.pem` somewhere durable first — a password manager, or its own Secrets Manager secret readable by neither task role nor the `cfdb-deploy` CI role.

**Regenerating requires `--force`.** Both the CA block and `mint_leaf` skip when the files already exist, and `make worker-certs` passes no arguments, so `make worker-certs` on a machine that already has certs prints "already exists" and changes nothing. Call the script directly when you mean to replace material:

```bash
./certs/generate-worker-certs.sh --force                      # replace everything
./certs/generate-worker-certs.sh --force --identity NAME      # …with a different SAN
```

**What a compromised task can reach.** The templates scope `secretsmanager:GetSecretValue` to the *execution* role — the one the ECS agent uses to inject the container's `Secrets:` — and to exactly the cert ARNs, so the **task** role holds no `GetSecretValue` at all. That bounds a task compromise to the material that task was already given: the entrypoint writes its own leaf and key to `/tmp/cfdb-tls/`, so a compromised task trivially holds those. What it cannot reach is the other side's leaf, the other environment's material, or the CA key.

Related, and worth stating because an operator will otherwise assume otherwise: under a shared CA these certificates prove **fleet membership, not peer role**. The API verifies a worker by name, but a worker verifies its client by chain alone, so any CA-signed leaf can act as a client. A compromised worker — the component that shells out to `samtools`/`tabix`/`bigBedToBed` over untrusted upstream bytes — therefore holds a credential the API accepts. mTLS does not contain a worker compromise. The generator mitigates the reverse direction by giving the API leaf `clientAuth` only, so the API's certificate cannot terminate a server side.

**Rotating a leaf** is two commands, because the entrypoint materializes the PEMs once at container start:

```bash
aws secretsmanager put-secret-value --secret-id <api-cert-secret> \
  --secret-string file://certs/api/api-cert.pem
aws ecs update-service --cluster cfdb-backend-prod-cluster \
  --service cfdb-backend-prod-service --force-new-deployment
```

Workers need no step at all — they are ephemeral, so the next `EcsProvisioner` `RunTask` pulls the new secret. (wool 0.13 can adopt rotated material without a restart via a reloadable credential provider; cfdb has not adopted it, because on ECS the redeploy above is cheap.)

**Rotating the CA is not atomic**, and the obvious approach breaks dispatch: a rolled API trusting only the new CA rejects every in-flight worker still holding a leaf signed by the old one. gRPC accepts a *bundle* of roots, so stage it instead — publish both CAs concatenated, roll everything onto it, then drop the old:

```bash
cat certs/worker-ca/ca.pem /path/to/new-ca.pem > /tmp/ca-bundle.pem
aws secretsmanager put-secret-value --secret-id <ca-secret> \
  --secret-string file:///tmp/ca-bundle.pem
# Roll the API onto the bundle and let in-flight workers cycle out, then
# reissue both leaves under the new CA, roll again, and only then publish
# the new CA alone.
```

**Nothing watches expiry.** `generate-worker-certs.sh` mints 825-day leaves, and an expired certificate is skipped as a transient handshake failure like any other — wool's warning does name it, but only if someone is reading. Either put rotation on a schedule short enough that this runbook stays exercised, or record the expiry somewhere that will page you: `openssl x509 -enddate -noout -in certs/worker/worker-cert.pem`.

## GraphQL API

**URL:** `POST /metadata`

### Queries

The API exposes four queries: `files` (paginated list), `file` (single lookup by MongoDB ObjectId), `fileCount` (match count for a filter, without fetching any documents), and `distinctValues` (unique values for a set of queryable fields).

`files` returns a `FileList` envelope rather than a bare list: `items` carries the requested page, and `totalCount` reports how many documents match `input` in total — before `page`/`pageSize` are applied — so clients can size pagination controls. Use `fileCount` when only the count is needed: it runs the same filter without materializing a page of documents.

```graphql
query {
  files(
    input: [FileMetadataInput]
    page: Int = 0
    pageSize: Int = 25
  ) {
    totalCount
    items {
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
}
```

```bash
# Query files from a specific DCC
curl -X POST http://localhost:8000/metadata \
  -H "Content-Type: application/json" \
  -d '{"query": "{ files(input: [{ dcc: [{ dccAbbreviation: [\"4DN\"] }] }]) { totalCount items { filename dcc { dccAbbreviation } } } }"}'
```

Single file lookup: `{ file(id: "507f1f77bcf86cd799439011") { filename accessUrl } }`

File count for a filter: `{ fileCount(input: [{ dcc: [{ dccAbbreviation: ["4DN"] }] }]) }` — returns the number of matching files without fetching any documents. It accepts the same `FileMetadataInput` filter shape as `files`; with no `input` it counts every file.

### Custom Scalars

The published contract is `schema.graphql` at the repo root — a generated artifact, regenerated by `make schema` and never edited by hand. Three of its scalars are outside the GraphQL specification, so a typed client must map all three explicitly:

| Scalar | Wire form | Used by |
|--------|-----------|---------|
| `ObjectIdScalar` | JSON string | `file(id:)` |
| `BigInt` | JSON number | `sizeInBytes`, on both `FileMetadataType` and `FileMetadataInput` |
| `JSON` | Any JSON value | `distinctValues.values`, `collections[].extra.hubmap.metadata` |

`BigInt` is a signed 64-bit integer. The GraphQL specification fixes `Int` at 32 bits, so a file larger than 2,147,483,647 bytes (~2.1 GB) could not be represented at all: the field resolved to `null` and contributed a `Int cannot represent non 32-bit signed integer value` entry to the response's `errors` array, degrading a whole page of results to a partial one. That affects every ENCODE `.hic` file (6–51 GB) and the larger 4DN mcools, so it is the common case for contact maps rather than an edge case. The input filter carries the same scalar — a 32-bit filter would leave exactly the files the widened output field exposes unfilterable. Note that size filtering is exact-match, not a range, and that `size_in_bytes` is stored as a BSON int64 only for ENCODE: 4DN and HuBMAP load it as a string through the C2M2 TSV path, so a numeric equality predicate does not match their documents. That is an ingest-layer gap independent of the scalar's width — the filter was equally inert at `Int` — and normalising it is tracked separately.

`BigInt` stays a JSON **number** on the wire rather than a string, so `sizeInBytes` remains directly usable in client-side arithmetic and comparisons with no parsing step. The usual objection to that choice — values above `Number.MAX_SAFE_INTEGER` (2^53-1) lose precision in JavaScript — does not bind here: 2^53 bytes is ~9 PB, far above any file this API serves. That is a property of *which* fields are routed through the scalar rather than something the scalar enforces, so staying under 2^53 is an admission criterion for any future `BigInt` field. What the scalar does enforce is the signed 64-bit range — exactly where BSON itself stops — plus a rejection of non-integers, including `true`/`false`, on both input and output.

**This is a breaking schema change.** A client that hard-codes `Int` breaks in four ways:

- A query declaring `query Q($s: [Int!])` and passing it to `sizeInBytes` now fails variable-type validation and must declare `[BigInt!]`.
- Generated clients must re-run codegen against the new SDL **and add a scalar mapping** — `graphql-codegen` and most typed clients silently widen an unrecognised custom scalar to `any`, so `scalars: { BigInt: 'number' }` (or the equivalent) is required or `sizeInBytes` loses its type with no build failure.
- Any client validating responses against a stored copy of the schema must refresh it.
- A client sending an integral float — `1234.0`, which any language that round-trips numbers through a float will emit — is now rejected. `Int` coerced it; `BigInt` does not, so the wire form stays one unambiguous representation.

A client that merely *reads* `sizeInBytes` out of the JSON response needs no change — it was already receiving a JSON number, and now receives a correct one instead of `null`.

**Migrating without a window of failures.** The server side of this break is not atomic. A rolling ECS deploy keeps old and new tasks in the target group simultaneously, dev ships on every merge while prod is promoted by hand, and the documented rollback (re-tag an older `:<sha>`) reverts the schema under clients that have already migrated. In each case a client that *names* `BigInt` in a variable declaration fails against whichever tasks are still on the old schema. The way out is that the leaf type only has to be named when the client declares a variable for it: passing the filter as an inline literal (`sizeInBytes: [6262125716]`), or hoisting the variable up to the whole input object (`query Q($input: [FileMetadataInput!])`), validates against the old schema and the new one alike. Move consumers to one of those forms first, and the deploy — in either direction — is a non-event.

### Query Mechanics

The GraphQL API uses an implicit OR/AND clause system for building MongoDB queries:

1. **Lists become OR clauses**: Multiple values in an array are combined with `$or`
2. **Dict keys become AND clauses**: Multiple fields in an object are combined with `$and`

Pagination is supported via `page` and `pageSize` parameters (defaults: 0 and 25). `totalCount` is unaffected by both — it always reports the full number of matches for the filter.

Both parameters are bounds-checked and an out-of-range value is rejected with a GraphQL error rather than being passed to the cursor: `page` must be `>= 0`, and `pageSize` must be between 1 and 500 (`MAX_PAGE_SIZE`). The floor matters because MongoDB reads `limit(0)` as *no limit*, so an unvalidated `pageSize: 0` would fetch and convert every matching document — on an unauthenticated endpoint over a collection of millions. Use `fileCount` when you want a count without documents. This is a behavior change: `pageSize: 0` previously returned every match, `pageSize: -1` quietly behaved as `pageSize: 1`, and any `pageSize` above 500 was honored.

Note what those bounds do *not* cover. `page` has a floor but no ceiling, so a deep page still costs the database an O(skip) walk — MongoDB satisfies a skip by discarding documents one at a time — and the ceiling on `pageSize` bounds a single `files` selection rather than a whole request, which may alias the field more than once. Both are properties of the endpoint as a whole rather than of a single argument, and neither is addressed here.

#### OR Query - Multiple Values in a List

Find files with either filename:

```graphql
query {
  files(input: [{ filename: ["data.csv", "results.tsv"] }]) {
    totalCount
    items {
      filename
    }
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
    totalCount
    items {
      filename
      dcc { dccAbbreviation }
      fileFormat { name }
    }
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
    totalCount
    items {
      filename
      collections { biosamples { anatomy { name } } }
    }
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
| `size_in_bytes` | int? | File size, exposed as the 64-bit `BigInt` scalar (see [Custom Scalars](#custom-scalars)) |
| `sha256` | string? | SHA-256 checksum (preferred) |
| `md5` | string? | MD5 checksum (if SHA-256 unavailable) |
| `filename` | string | Filename without path |
| `accession_id` | string? | The DCC's own accession for this file, stored upper-cased so an `accessionId` filter matches in any casing. Populated for 4DN and ENCODE; always null for HuBMAP (see the note below). |
| `file_format` | FileFormat? | EDAM CV term for digital format |
| `compression_format` | string? | EDAM CV term ID for compression (e.g., `format:3989` for gzip); `""` when no compression is recorded or recognized; null/absent when undetermined. Read the note below before relying on it. |
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

**A note on `compression_format`, because it is easy to over-read.** The field reports compression that is *extrinsic* to `file_format` — a wrapper the source reveals and `file_format` does not. It is not a statement about whether the bytes are compressed. A BAM, bigWig or bigBed carries `""` despite being internally compressed, because its `file_format` already names that container. Conversely 4DN and HuBMAP take the value straight from their upstream C2M2 datapackage, which in practice leaves it blank on every file — including gzipped ones — so `""` from those DCCs means "upstream recorded nothing", not "uncompressed". Never treat `""` as a licence to skip inspecting the bytes.

ENCODE derives the value from the download URL's filename suffix, because the ENCODE metadata TSV has no compression column. Two consequences are worth knowing. The field is **absent** (rendered as null) rather than `""` when nothing could be determined — no filename in the URL, or a compression suffix no EDAM term expresses (`.bz2`, `.xz`, `.zst`, `.zip`, `.starch`) — so treat null as "sniff the bytes", never as "uncompressed". And `format:3989` means "gzip-family stream": ENCODE names both plain gzip and BGZF `.gz` (it publishes no `.bgz` at all, and roughly a quarter of its `.gz` files are BGZF), so the value cannot distinguish them. Anything deciding on `gunzip | bgzip` must read the BGZF header — which is what `cfdb.workflows.processors.tabix` does, deliberately, and that byte-level check remains the decision of record.

**A note on `accession_id`, because a null does not mean what it looks like.** The field exists so one input works across DCCs: 4DN puts an opaque UUID in `local_id` and carries its accession only inside the `persistent_id` URL, while ENCODE stores the accession *as* `local_id`. It is stored case-folded and filter values are folded identically, so `accessionId: ["4dnfimcjxzkh"]` and `["4DNFIMCJXZKH"]` match the same file. Three different situations all render as a null field and a `totalCount` of 0, and the API cannot distinguish them for you: the accession genuinely does not exist; the DCC issues none (all of HuBMAP, which matches files by filename within a dataset — tracked in [#102](https://github.com/abdenlab/cfdb/issues/102)); or that DCC has not been synced since the field was added. **A deployment must re-sync each DCC before `accessionId` returns anything.** Each sync logs its coverage (`4DN accession coverage: 53697/53697 files carry accession_id`), which is the only place that distinction is visible.

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
| `accession_id` | string? | The DCC's accession for the experiment this collection represents, stored upper-cased (shared across 4DN and ENCODE). Null on ENCODE's biosample-keyed fallback collections, which name no experiment, and on all of HuBMAP. |
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
| FileFormat | EDAM CV `format:` terms, or a `cfdb:`-prefixed token minted where EDAM has none |
| DataType | EDAM CV `data:` terms |
| AssayType | OBI (Ontology for Biomedical Investigations) |
| NcbiTaxonomy | NCBI Taxonomy Database |

Every `id` above resolves in its source ontology except the minted `FileFormat` tokens. EDAM has no term for a few formats cfdb ingests — `bedpe` and `bigInteract` at present — and aliasing them onto the nearest EDAM term would make each indistinguishable from the format it was aliased to, so a `cfdb:` token is minted instead. A client resolving `file_format.id` against EDAM should skip ids carrying that prefix rather than treat them as resolvable.

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
| 202 | Preprocessed artifact not yet cached — workflow accepted. `Location: /jobs/{id}` and `Retry-After` headers point to the polling endpoint. Under load the job may sit `pending` in the durable queue before a worker picks it up; poll `/jobs/{id}` for progress. |
| 206 | Partial content (Range request) |
| 400 | Invalid DCC, path-param shape, or Range header |
| 403 | File requires authentication (consortium/protected access) or denied by upstream repository |
| 404 | File not found, or HEAD probe of a not-yet-cached processed artifact (GET would dispatch) |
| 416 | Range not satisfiable (out of bounds, or file size unknown so no range can be satisfied) |
| 429 | Too many active preprocessing jobs — the active-workflow ceiling (`CFDB_WORKFLOW_MAX_ACTIVE`) is reached; `Retry-After` header set. Retry shortly. |
| 501 | No supported access method — the record carries no access URL, or the file is reachable only by an unsupported transfer (e.g. Globus-only files) |
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
| 202 | Index not yet cached — workflow accepted. `Location: /jobs/{id}` and `Retry-After` headers point to the polling endpoint. Under load the job may sit `pending` in the durable queue before a worker picks it up; poll `/jobs/{id}` for progress. |
| 206 | Partial content (Range request) |
| 400 | Invalid DCC, path-param shape, or Range header |
| 403 | File requires consortium/protected access (HuBMAP) |
| 404 | File not found, format has no index (CSV/TSV/bigWig), or HEAD probe of a not-yet-cached index |
| 416 | Range not satisfiable |
| 429 | Too many active preprocessing jobs — the active-workflow ceiling (`CFDB_WORKFLOW_MAX_ACTIVE`) is reached; `Retry-After` header set. Retry shortly. |
| 502 | Upstream service error or malformed sidecar |
| 503 | Workflow subsystem disabled (set `SYNC_DATA_DIR`) for a processable format, or shutting down (`Retry-After`) |

```bash
# Download an index file
curl -O http://localhost:8000/index/4dn/4DNFIG5NX1EC
```

### Readiness Probes

**URL:** `GET /data/{dcc}/{local_id}/status` | `GET /index/{dcc}/{local_id}/status`

Side-effect-free probes that report whether a streaming `GET` to the corresponding endpoint would return bytes immediately or first require preprocessing. They reuse the same lookup, DCC normalization, and access-control logic as `/data` and `/index` and mirror their error codes, but on success return a small JSON body `{ "ready": bool }` instead of streaming. They **never dispatch a workflow** and make no upstream network calls — they read only database and cache metadata, so they are cheap control-plane queries safe to call before committing to a fetch.

**Path Parameters:**
- `dcc` - DCC abbreviation (e.g., `4dn`, `hubmap`, `encode`) - case insensitive
- `local_id` - The file's unique ID within the DCC

- `ready: true` — a `GET` would not require preprocessing: the processed artifact is already cached, an upstream sidecar exists (`/index`), or the format is served directly (passthrough such as CSV/TSV/bigWig, or a format with no processed `/data` artifact).
- `ready: false` — the file is accessible, but a `GET` would trigger preprocessing (a processable format whose artifact is not yet cached, i.e. `GET` would return `202`).

`ready` reflects only the default (preprocessed) path. It indicates that **no preprocessing is required**, not that a `GET` is guaranteed to return `200`: the subsequent `GET` may still resolve an access URL upstream and return `403`/`404`/`501`/`502`/`504`, and when the workflow subsystem is disabled a `ready: true` processable file is served as raw upstream bytes rather than a preprocessed artifact. The probes take no `raw`, `Range`, or `HEAD` semantics — they always describe the default path.

| Code | Description |
|------|-------------|
| 200 | Readiness reported as `{ "ready": bool }` |
| 400 | Invalid DCC or path-param shape |
| 403 | File requires consortium/protected access (HuBMAP) |
| 404 | File not found, or (on `/index`) the format has no index (CSV/TSV/bigWig) |
| 501 | (`/data` only) File has no access URL |
| 502 | (`/index` only) Malformed upstream sidecar |
| 503 | (`/index` only) Workflow subsystem disabled (set `SYNC_DATA_DIR`) for a processable format |

```bash
# Will a GET stream immediately, or trigger preprocessing?
curl http://localhost:8000/data/4dn/4DNFIG5NX1EC/status
# {"ready": true}

curl http://localhost:8000/index/encode/ENCFF123ABC/status
# {"ready": false}
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

Cache keys have the shape `{dcc}/{local_id}/{artifact_kind}/{processor_id}/{md5}-v{processor_version}` — for example `encode/ENCFF732YBO/index/tabix-interval/6fccbb438a046075cb438f84d0defe8d-v2`. They are content-addressed using each file's upstream `md5`, so a byte change upstream (with the sync pipeline refreshing `md5`) invalidates the cache automatically, and they carry the producing processor's identity (`Processor.processor_id`) so two processors claiming the same file and artifact kind can never read back each other's output. Without that segment a version number was the only thing separating processors — and `TabixIntervalProcessor` and `BamIndexProcessor` both sit at version 2, staying apart only because their `supported_formats` happen to be disjoint. A processor that declares no `processor_id` inherits its own class name; declare one explicitly when the identity should survive a class rename, since changing the string invalidates every artifact keyed under it. The declaration must sit in the processor's own class body — a value supplied by a base class or mixin is discarded in favour of the class name, so factoring a pinned identity into a mixin would silently cold-cache everything keyed under it. An identity may not be blank, contain a path separator, be `.` or `..`, or equal an artifact kind (`data`, `index`); each is rejected when the class is declared, and the last of those keeps a mis-specified purge prefix from reducing a live key to something shaped like a retired one.

**Bounded concurrency, durable queuing, and admission control.** Dispatch is bounded on three cooperating layers so an unauthenticated burst on `/data` and `/index` can't oversubscribe the worker fleet or queue unbounded work:

- **Per-worker backpressure** — each worker accepts at most `CFDB_WORKER_MAX_CONCURRENT_TASKS` tasks at once (default `1`), serializing the subprocess pipelines on a 1-vCPU worker. A worker at capacity rejects the dispatch and the API's priority load balancer rotates to the next worker.
- **Priority (leaky-bucket) load balancing** — the API offers each task to discovered workers in a stable order, so load concentrates on the lowest-ordered workers and over-provisioned workers drain to idle and self-reap (via `CFDB_WORKER_MAX_LIFETIME_SECONDS`) instead of every worker carrying a thin perpetual slice.
- **Durable queue + retry-to-deadline** — when no worker has capacity, the job is **not** failed and does **not** block the request: it stays `pending` and a durable, Mongo-backed scheduler re-attempts dispatch every `CFDB_WORKFLOW_RETRY_INTERVAL_S` (plus jitter) until a worker frees up or the `CFDB_WORKFLOW_DISPATCH_DEADLINE_S` deadline elapses (then it is failed with a `capacity:`-prefixed error). Because the queue lives in Mongo, an API restart resumes it. On every scheduler tick (including the first, on boot) an orphan-recovery sweep re-queues jobs a crash left mid-flight — a `running` job whose API consumer died, or a fresh `pending` claim that never rescheduled — once they pass the stale threshold (`CFDB_WORKFLOW_STALE_THRESHOLD_S`), so recovery is autonomous and does not wait for a client to re-request the file. Recovery shares the same deadline clock as a fresh job: the re-queue preserves the original submission time, so an orphan older than `CFDB_WORKFLOW_DISPATCH_DEADLINE_S` is failed `capacity:` on its first recovery attempt rather than resumed (its committed cache artifacts survive for a later fresh `GET` to reuse) — recovery is best-effort, not unbounded. On the ECS profile, an overflow also requests one bounded worker spawn (the leaky bucket overflowing), inverting the old unconditional per-request spawn.
- **Admission ceiling** — once `CFDB_WORKFLOW_MAX_ACTIVE` workflows are active (`pending` + `running`), further preprocessing requests are shed with `429 Retry-After` rather than queued, so the backlog itself is bounded. The check runs before the per-file mutex, so at the ceiling even a re-`GET` for a file whose workflow is already in flight is shed with `429` (rather than attaching to the in-flight job) and the client retries — the deliberate trade for shedding before an unbounded admission race. The readiness `/status` probes never dispatch and so never `429`.

`/index` continues to serve upstream sidecars first when present (the 218 BED→beddb and 4 BED→tbi 4DN cases that publish under `extra.extra_files` or `extra.fourdn.extra_files`); the workflow path is dispatched only when no sidecar exists. Set `?raw=true` to bypass the workflow path entirely and return only the upstream sidecar (404 when none exists).

Required environment variables:

- `SYNC_DATA_DIR` — directory under which the workflow cache and per-job workdirs live. Both subdirectories (`$SYNC_DATA_DIR/cache` and `$SYNC_DATA_DIR/jobs`) must share a filesystem because `LocalFsCache.put` relies on `os.replace` atomicity; the API asserts this at startup and fails fast if they live on different volumes. When unset, the workflow subsystem is disabled, `/data` falls through to direct upstream streaming, `/index` returns 404 for passthrough formats (CSV/TSV/bigWig — there is no index in any state of the world), and `/index` returns 503 for processable formats that would otherwise dispatch a workflow (sidecar-served files still work).
- `WORKFLOW_WORKER_COUNT` — local-dev only: how many workers the LAN pool (`python -m cfdb.workflows.worker_lan`) spawns and publishes (default `2`). The API itself no longer leases a fixed count — its `WorkerPool` admits every worker discovery surfaces. In the ECS profile one ephemeral worker is launched per workflow (via `EcsProvisioner` `RunTask`), and the concurrent worker fleet is bounded by `ECS_MAX_WORKERS` (default `16`; see the bounded-concurrency knobs below) with the **AWS Fargate vCPU service quota** as the hard ceiling above it. A burst of N distinct uncached files launches up to `min(N, ECS_MAX_WORKERS)` workers; the rest queue.
- `WORKFLOW_POOL_NAMESPACE` — wool discovery namespace shared by the API and the external worker pool (default `cfdb-workers`). Both processes must agree on this value or dispatch will hang waiting for workers.

Bounded-concurrency control (issue #45):

- `CFDB_WORKER_MAX_CONCURRENT_TASKS` — per-worker backpressure threshold: a worker rejects a dispatch (gRPC `RESOURCE_EXHAUSTED`, which wool classifies as transient — leaving the worker in the pool while the priority load balancer rotates past it) once it already has this many tasks in flight (default `1`, to serialize the subprocess pipelines on a 1-vCPU worker; `0` disables backpressure). Set on the **worker** process.
- `CFDB_WORKFLOW_MAX_ACTIVE` — admission ceiling on concurrently active workflows (`pending` + `running` jobs combined — i.e. the **queue depth plus running**, not the worker count). Once this many are active, `/data` and `/index` shed new preprocessing requests with `429 Retry-After` before claiming the per-file mutex (default `1024`). To cap the worker fleet without limiting the queue, use `ECS_MAX_WORKERS` instead. Soft cap — a count-then-claim race may briefly overshoot. Set on the **API**.
- `ECS_MAX_WORKERS` — ECS profile only: cap on concurrently-running ephemeral Fargate worker tasks (default `16`; `0` disables the cap and relies on the Fargate vCPU quota). Before each `RunTask` the `EcsProvisioner` counts the ECS-visible fleet (`list_tasks`) **plus its own recently-issued launches that `list_tasks` may not reflect yet**, under a lock, and skips the spawn when at the cap — so the **worker fleet** is bounded while excess jobs stay `pending` and the durable scheduler dispatches them as workers free up. It does **not** shed (that is `CFDB_WORKFLOW_MAX_ACTIVE`'s job) and does **not** limit the queue. Counting in-flight launches is what bounds a simultaneous cold-start burst; counting only `list_tasks` (which is eventually consistent) lets a concurrent burst see a stale count and every spawn slip under the cap. Bounded per API task: a single API task (the default) is held to the cap; a multi-task API would each track only its own launches and need a shared lease. Set on the **API**.
- `CFDB_WORKFLOW_RETRY_INTERVAL_S` — base cadence (plus a small random jitter) at which the durable retry scheduler re-attempts dispatch for a job awaiting worker capacity (default `120`, i.e. 2 min). A dispatch attempt that finds no free worker leaves the job `pending` and rescheduled rather than blocking the request. Set on the **API**.
- `CFDB_WORKFLOW_DISPATCH_DEADLINE_S` — how long a job may wait for worker capacity (re-attempted on the retry cadence above) before the scheduler fails it with a `capacity:`-prefixed error (default `14400`, i.e. 4 h). Replaces the former single in-request `CFDB_WORKFLOW_DISPATCH_WAIT_S` wait — instead of blocking one request on a cold start, the job queues durably and is retried to this deadline. Set on the **API**.

Optional tunables (with defaults):

- `CFDB_WORKFLOW_DURATION_CAP_S` — per-workflow wall-clock cap (default `14400`, i.e. 4 h — sized for multi-hour preprocessing runs; lower it for fixture-bound dev).
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

**Workers publish their own metadata.** ECS reports a task's address and health, but not what is running inside the container — and two fields of wool's `WorkerMetadata` are knowable only to the worker: the wool protocol version it runs, and whether it configured TLS. wool gates worker admission on both, so a value the API invented for them would be a value that silently rejects the entire fleet. After starting, each worker therefore tags its own ECS task (`wool.version`, `wool.secure`) with what wool authored for it, and `EcsDiscovery` reads those tags back via `DescribeTasks … --include TAGS`. ECS supplies liveness; the worker supplies identity.

Two consequences worth knowing. The worker task role MUST hold `ecs:TagResource` — `workers.yml` grants it, but only a privileged `cloudformation deploy` can land that grant, and it MUST land **before** the image that publishes does (see the one-time bootstrap in [Deploying the CloudFormation stacks](#deploying-the-cloudformation-stacks) — the CI role cannot deploy templates, so merging without the bootstrap ships a fleet that exits at startup). A worker that cannot publish **exits** rather than serving, because it could never be discovered and would otherwise hold a Fargate slot and count against `ECS_MAX_WORKERS` while being unable to receive work. And a task that is `RUNNING`/`HEALTHY` but whose tags have not yet landed is deliberately not advertised: "has not published yet" is not "has default metadata", and treating them the same is what previously made every ECS worker unadmittable.

#### Running a local worker pool

For single-host development, start a wool worker pool in a separate process *before* launching the API, with `WORKFLOW_POOL_NAMESPACE` matching what the API uses:

```bash
# Publishes WORKFLOW_WORKER_COUNT workers (default 2) under
# WORKFLOW_POOL_NAMESPACE (default cfdb-workers) over LAN discovery.
python -m cfdb.workflows.worker_lan --namespace cfdb-workers --workers 2
# or, with defaults: make worker-local
```

This is the local-dev counterpart to the ECS entrypoint (`python -m cfdb.workflows.worker_main`): `worker_lan` spawns a `wool.WorkerPool` wired to `LanDiscovery` so the pool advertises its workers over zeroconf/mDNS, whereas `worker_main` boots a bare worker that `EcsDiscovery` finds by polling the ECS control plane and reading the metadata tags the worker published onto its own task. The API connects via LAN discovery and dispatches workflows to whatever workers are publishing under that namespace. With no worker pool running, `/data` and `/index` requests for processable formats will hang on the dispatch retry budget (60s by default) before failing with `NoWorkersAvailable`.

#### Worker mTLS

By default the API↔worker gRPC dispatch channel is plaintext, gated only by network reachability (in production, the worker security group). Setting the three `CFDB_WORKER_TLS_*` cert paths on **both** sides turns on wool's native mutual TLS (`mutual=True`): the channel is encrypted and each side presents a CA-signed certificate that the other verifies. wool's mTLS is peer-to-peer, so each process holds its own leaf cert/key while the CA is shared — the API and every worker MUST be signed by the same CA, or dispatch is rejected.

The configuration is gating-by-presence: when all three of `CFDB_WORKER_TLS_CA`, `CFDB_WORKER_TLS_CERT`, and `CFDB_WORKER_TLS_KEY` are unset the plaintext path is used unchanged (local PoC dev needs no certs); when all three are set mTLS is enforced. A *partial* configuration (some set, some not) fails fast at startup rather than silently degrading to plaintext.

**Identity.** TLS normally verifies a server's certificate against the address the client dialed, which is a problem here: workers answer wherever they happen to come up. `EcsDiscovery` reaches each Fargate worker at the awsvpc IP assigned at launch, and a containerized local worker answers on a bridge IP — neither is knowable when the certificate is minted. `CFDB_WORKER_TLS_IDENTITY` (default `cfdb-worker`) points the API at a fixed logical name instead, so the worker leaf carries one stable SAN rather than an enumeration of every address it might be reached at. Chain and SAN verification both still happen; only the name being matched changes. It is a client-side setting — but "client" is a property of a connection, not of a process: the API is the client on the dispatch channel, and each worker is the client on the one channel wool opens back to its own subprocess to drain it, verifying the same SAN. Both sides therefore read the variable and MUST agree on it; a worker left at a different value drains by force-reap instead of gracefully, losing in-flight work with no TLS error anywhere. Setting it to the empty string restores address verification, which is only workable when workers are reached at a fixed, certified address.

Generate a local CA and the worker + API leaf certs with:

```bash
make worker-certs
# wraps ./certs/generate-worker-certs.sh — see --help for --identity and SAN options.
# Writes (all git-ignored):
#   certs/worker-ca/ca.pem            shared CA
#   certs/worker/worker-cert.pem,-key.pem   worker leaf (SAN: cfdb-worker)
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

…and the API process, with the **same** `CFDB_WORKER_TLS_CA` but the API leaf. `CFDB_WORKER_TLS_IDENTITY` already defaults to the SAN `make worker-certs` mints, so it only needs setting when you passed a different `--identity`:

```bash
export CFDB_WORKER_TLS_CA=certs/worker-ca/ca.pem
export CFDB_WORKER_TLS_CERT=certs/api/api-cert.pem
export CFDB_WORKER_TLS_KEY=certs/api/api-key.pem
```

The same env vars configure the ECS worker entrypoint (`worker_main`) and the API on Fargate — see [Worker mTLS on ECS](#worker-mtls-on-ecs) for distributing the material through Secrets Manager.

> **Upgrading an existing mTLS deployment.** Worker certificates minted before identity support have no `cfdb-worker` SAN, so the API will now fail to verify them. Regenerate with `./certs/generate-worker-certs.sh --force` — plain `make worker-certs` passes no arguments and the script skips material that already exists, so it will report success and change nothing. Use `--force --identity NAME` to match a SAN your certificates already carry instead. Alternatively set `CFDB_WORKER_TLS_IDENTITY=""` to keep verifying against the dialed address. Deployments running the plaintext channel are unaffected.

> **Rolling back past this change.** The reverse is also breaking, and less obvious. Once workers hold identity-carrying certificates, an API rolled back to a pre-identity image drops the target-name override and verifies against the dialed address again — which for a leaf whose only SAN is the identity fails every handshake. wool's version gate does not catch this, because an older API against newer workers passes `client <= server`: the workers are admitted and then fail at the handshake. Roll back with `CFDB_WORKER_TLS_IDENTITY=""` set, or revert to address-verifiable certificates first.

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
