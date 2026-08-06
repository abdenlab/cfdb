#!/usr/bin/env bash
#
# Deploy the workers stack so worker tasks can publish their own wool
# metadata (issue #90, PR #89).
#
# WHY THIS EXISTS
#
# Since PR #89, a worker publishes its wool protocol version and TLS
# flag by tagging its own ECS task, and EcsDiscovery reads those tags
# back to build the WorkerMetadata wool gates admission on. A worker
# that cannot tag its task exits deliberately — it could never be
# discovered, so serving would hold a Fargate slot while unable to
# receive work.
#
# The ecs:TagResource grant that makes tagging possible lives in
# cloudformation/workers.yml. The CI pipeline CANNOT deploy it: the
# cfdb-deploy role holds no cloudformation:* (by design). So shipping
# the image without first deploying this stack empties the fleet —
# every RunTask launches a worker that exits in ~25s — while CI reports
# success, because update-service and services-stable both pass.
#
# That is exactly what happened to dev on 2026-08-05.
#
# ORDER MATTERS, IN ONE DIRECTION ONLY
#
#   stack THEN image  -> harmless; an older image ignores a permission
#                        it never uses.
#   image THEN stack  -> outage for the whole window between them.
#
# So this is always safe to run early, and never safe to run late.
#
# USAGE
#
#   ./scripts/deploy-worker-metadata-grant.sh preflight   # check creds only
#   ./scripts/deploy-worker-metadata-grant.sh dev
#   ./scripts/deploy-worker-metadata-grant.sh prod
#   ./scripts/deploy-worker-metadata-grant.sh verify dev|prod
#
# CREDENTIALS
#
# Requires cloudformation:*, iam:PassRole, and iam:PutRolePolicy.
# AWSReservedSSO_PowerUserAccess is NOT sufficient — it excludes iam:*,
# and the deploy fails at RegisterTaskDefinition with:
#
#   is not authorized to perform: iam:PassRole on resource:
#   .../cfdb-workers-<env>-WorkerTaskRole-...
#
# The failure rolls back cleanly (UPDATE_ROLLBACK_COMPLETE), so a
# refused attempt costs nothing but time. See the README section
# "Which principal can run a privileged deploy" — this script's
# preflight enforces what that section documents.

set -euo pipefail

readonly REGION="us-east-2"
readonly ACCOUNT="605134458779"
readonly ECR="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
readonly TEMPLATE="cloudformation/workers.yml"

# Per-environment coordinates. BackendClusterName scopes the tagging
# grant to this environment's own cluster — dev and prod share an
# account and region, and these tags drive admission, so an unscoped
# grant would let a compromised worker in one environment empty the
# other's pool. Its template default (cfdb-backend-cluster) matches
# NEITHER environment on purpose, so a forgotten override fails loudly
# rather than silently granting nothing usable.
env_config() {
    case "$1" in
        dev)
            STACK="cfdb-workers-dev"
            CLUSTER="cfdb-backend-dev-cluster"
            IMAGE="${ECR}/cfdb-wool:dev"
            ;;
        prod)
            STACK="cfdb-workers-prod"
            CLUSTER="cfdb-backend-prod-cluster"
            IMAGE="${ECR}/cfdb-wool:prod"
            ;;
        *)
            echo "unknown environment: $1 (expected dev or prod)" >&2
            exit 2
            ;;
    esac
    WORKER_FAMILY="${STACK}-worker"
    LOG_GROUP="/ecs/${WORKER_FAMILY}"
}

# ---------------------------------------------------------------------
# Preflight — fail before touching a stack, not halfway through one.
# ---------------------------------------------------------------------

preflight() {
    echo "== caller identity =="
    aws sts get-caller-identity --output json

    # A cheap, non-mutating probe of the permission that actually
    # blocks this deploy. PowerUserAccess can read IAM here but cannot
    # PassRole, and the distinction is invisible until RegisterTaskDefinition.
    local arn
    arn="$(aws sts get-caller-identity --query Arn --output text)"
    case "$arn" in
        *PowerUserAccess*)
            cat >&2 <<'WARN'

WARNING: this looks like AWSReservedSSO_PowerUserAccess, which excludes
iam:* and therefore cannot PassRole. The deploy will fail at
WorkerTaskDefinition and roll back. Log in with an IAM-capable
permission set (AdministratorAccess or equivalent) first.

WARN
            return 1
            ;;
    esac
    echo "principal is not PowerUser — proceeding"
}

# ---------------------------------------------------------------------
# Deploy.
# ---------------------------------------------------------------------

deploy() {
    env_config "$1"

    echo "== ${STACK}: state before =="
    aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
        --query 'Stacks[0].{Status:StackStatus,Updated:LastUpdatedTime}' --output json

    # WorkerImageURI is passed explicitly even though CloudFormation
    # would reuse the previous value on an update. Being explicit means
    # this script says what it deploys rather than depending on stack
    # history — and both stacks already point at their moving tag, so
    # this is a no-op that documents itself.
    #
    # Every other parameter (WorkerCpu, WorkerMemory, the TLS secret
    # ARNs, ...) is deliberately omitted so CloudFormation reuses the
    # deployed values. In particular the WorkerTls*SecretArn parameters
    # stay empty, which keeps mTLS off — this change does not enable it.
    echo "== deploying ${STACK} =="
    aws cloudformation deploy \
        --region "$REGION" \
        --stack-name "$STACK" \
        --template-file "$TEMPLATE" \
        --parameter-overrides \
            "WorkerImageURI=${IMAGE}" \
            "BackendClusterName=${CLUSTER}" \
        --capabilities CAPABILITY_IAM

    echo "== ${STACK}: state after =="
    aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
        --query 'Stacks[0].StackStatus' --output text
}

# ---------------------------------------------------------------------
# Verify — assert the grant landed, then (dev) prove a job moves.
# ---------------------------------------------------------------------

verify() {
    env_config "$1"

    # 1. The inline policy exists on the worker task role. Before the
    #    deploy this lists only "cache-access"; after, it must also
    #    carry "worker-metadata".
    local role
    role="$(aws cloudformation describe-stack-resources \
        --stack-name "$STACK" --region "$REGION" \
        --logical-resource-id WorkerTaskRole \
        --query 'StackResources[0].PhysicalResourceId' --output text)"
    echo "== worker task role: ${role} =="
    aws iam list-role-policies --role-name "$role" --query 'PolicyNames' --output json

    # 2. The grant is scoped to THIS environment's cluster and to the
    #    two wool.* keys. A bare task/* here would mean the
    #    BackendClusterName override was missed.
    #    Tolerates absence so this whole function is runnable BEFORE the
    #    deploy, to capture a baseline — a missing policy is the
    #    pre-fix state, not a reason to stop reporting.
    echo "== ecs:TagResource statement =="
    if ! aws iam get-role-policy --role-name "$role" --policy-name worker-metadata \
            --query 'PolicyDocument.Statement[0].{Action:Action,Resource:Resource,Condition:Condition}' \
            --output json 2>/dev/null; then
        echo "MISSING — the worker-metadata policy is not attached."
        echo "         This is the pre-fix state: workers will exit at startup."
    fi

    # 3. No worker should be crash-looping. Before the fix these exit 1
    #    within ~25s of starting; after, they run until they self-reap.
    echo "== recent worker exits (expect none new after the deploy) =="
    local stopped
    stopped="$(aws ecs list-tasks --cluster "$CLUSTER" --family "$WORKER_FAMILY" \
        --desired-status STOPPED --region "$REGION" \
        --query 'taskArns[-3:]' --output text 2>/dev/null || true)"
    if [ -n "$stopped" ]; then
        # shellcheck disable=SC2086
        aws ecs describe-tasks --cluster "$CLUSTER" --region "$REGION" --tasks $stopped \
            --query 'tasks[].{stopped:stoppedAt,exit:containers[0].exitCode,reason:stoppedReason}' \
            --output json
    else
        echo "(no stopped worker tasks)"
    fi

    # 4. The diagnostic that names this exact failure. Silence here
    #    after a deploy is the signal you want; a hit means the grant
    #    still is not reaching the worker.
    echo "== worker log: TagResource denials in the last 15m =="
    aws logs tail "$LOG_GROUP" --region "$REGION" --since 15m --format short 2>/dev/null \
        | grep -c "ecs:TagResource denied" \
        || echo "0 (good)"

    # 5. Workers now publish, so a discovered task carries wool.* tags.
    #    Empty output with no running workers just means nothing is
    #    dispatching — run the smoke below to create one.
    echo "== tags on running workers (expect wool.version / wool.secure) =="
    local running
    running="$(aws ecs list-tasks --cluster "$CLUSTER" --family "$WORKER_FAMILY" \
        --desired-status RUNNING --region "$REGION" --query 'taskArns' --output text 2>/dev/null || true)"
    if [ -n "$running" ]; then
        # shellcheck disable=SC2086
        aws ecs describe-tasks --cluster "$CLUSTER" --region "$REGION" --tasks $running \
            --include TAGS --query 'tasks[].tags' --output json
    else
        echo "(no running workers)"
    fi
}

# ---------------------------------------------------------------------
# End-to-end smoke (dev only — do not dispatch load at prod casually).
#
# Not an AWS call, but it is the only check that proves the pipeline
# actually works rather than that the permission exists. Pick an
# uncached BED, dispatch, and watch the job leave "pending".
#
#   API=https://dev.cfdb.vis-api.link
#   ID=ENCFF247ILV
#
#   # side-effect-free: {"ready":false} means a GET would preprocess
#   curl -sS "$API/data/ENCODE/$ID/status"
#
#   # dispatch: expect 202 + Location: /jobs/<uuid>
#   curl -sS -D - -o /dev/null "$API/data/ENCODE/$ID"
#
#   # poll: pending -> running -> completed, ~200s on a cold Fargate start
#   watch -n 15 "curl -sS $API/jobs/<uuid>"
#
# A job that sits "pending" past ~5 minutes with no running worker means
# the fix has not taken. Check step 4 above first.
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
# NOT REQUIRED, and deliberately not automated here.
#
# cloudformation/backend.yml also changed in PR #89, but only by adding
# the WorkerTlsIdentity parameter and an env var rendered under
# ApiMtlsEnabled. Both environments run with empty cert ARNs, so that
# condition is false, the env var resolves to AWS::NoValue, and the
# deployed task definition is byte-identical. Deploying the backend
# stacks changes nothing today.
#
# It becomes necessary only before enabling mTLS, at which point both
# stacks need the same WorkerTlsIdentity value:
#
#   aws cloudformation deploy --region us-east-2 \
#     --stack-name cfdb-backend-<env> \
#     --template-file cloudformation/backend.yml \
#     --parameter-overrides WorkerTlsIdentity=cfdb-worker ... \
#     --capabilities CAPABILITY_IAM
#
# Already satisfied, verified 2026-08-06 — do NOT redo:
#   - GitHub prod environment carries PROD_BACKEND_CLUSTER and
#     PROD_BACKEND_SERVICE.
#   - cfdb-deploy already grants ecs:UpdateService / DescribeServices /
#     DescribeTasks / ListTasks on BOTH cluster ARNs.
# ---------------------------------------------------------------------

main() {
    local cmd="${1:-}"
    case "$cmd" in
        preflight) preflight ;;
        dev|prod)  preflight && deploy "$cmd" && verify "$cmd" ;;
        verify)    verify "${2:?usage: $0 verify dev|prod}" ;;
        *)
            sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'
            exit 2
            ;;
    esac
}

main "$@"
