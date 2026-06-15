"""Validated workflow deployment-profile snapshot.

The lifespan probes a handful of env vars to decide which subsystem
shape to wire (LocalFsCache vs S3Cache, LanDiscovery vs EcsDiscovery,
no provisioner vs EcsProvisioner). Encoding the decision as a single
``WorkflowProfile.from_env()`` call at boot lets the helpers branch
once on ``profile.kind`` instead of re-probing the env globals in
five different places.

Three profiles:

* ``"local"`` — :class:`LocalFsCache` + :class:`LanDiscovery` + no
  provisioner. The PoC / laptop-dev path; workers are started by hand
  via ``wool pool --spawn``.
* ``"s3-cached"`` — :class:`S3Cache` + :class:`LanDiscovery` + no
  provisioner. S3 artifacts but workers still LAN-discovered; useful
  for staging environments where workers run on EC2.
* ``"ecs"`` — :class:`S3Cache` + :class:`EcsDiscovery` +
  :class:`EcsProvisioner`. The production Fargate path.

A partial ECS config (``ECS_CLUSTER`` set without
``ECS_WORKER_TASK_DEFINITION`` or ``ECS_WORKER_SUBNETS``) raises at
``from_env()`` time so a typo cannot silently degrade to PoC.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

from cfdb import api
from cfdb.workflows.provisioner import AssignPublicIp


@dataclass(frozen=True)
class _S3Config:
    """S3-backed cache configuration."""

    bucket: str
    prefix: str


@dataclass(frozen=True)
class _EcsConfig:
    """ECS Fargate provisioner + discovery configuration."""

    cluster: str
    task_definition: str
    task_family: Optional[str]
    subnets: tuple[str, ...]
    security_groups: tuple[str, ...]
    assign_public_ip: AssignPublicIp


@dataclass(frozen=True)
class WorkflowProfile:
    """Workflow deployment profile snapshot taken at lifespan boot.

    Use :meth:`from_env` to validate and snapshot the env once. The
    lifespan helpers then branch on ``self.kind`` (or on the presence
    of ``self.s3`` / ``self.ecs``) rather than re-probing
    :mod:`cfdb.api` globals.
    """

    kind: Literal["local", "s3-cached", "ecs"]
    cache_root: Path
    workdir_root: Path
    s3: Optional[_S3Config] = None
    ecs: Optional[_EcsConfig] = None
    aws_endpoint_url: Optional[str] = None
    aws_region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> Optional["WorkflowProfile"]:
        """Validate and snapshot the workflow env config.

        Returns ``None`` when ``SYNC_DATA_DIR`` is unset — the workflow
        subsystem stays disabled and routers fall through to their
        direct-streaming paths. Raises :class:`RuntimeError` on partial
        ECS config so misconfiguration surfaces loudly at boot.
        """
        if not api.SYNC_DATA_DIR:
            return None

        sync_root = Path(api.SYNC_DATA_DIR)
        cache_root = sync_root / "cache"
        workdir_root = sync_root / "jobs"

        s3 = _s3_config_from_env()
        ecs = _ecs_config_from_env()

        kind: Literal["local", "s3-cached", "ecs"]
        if ecs is not None:
            kind = "ecs"
        elif s3 is not None:
            kind = "s3-cached"
        else:
            kind = "local"

        return cls(
            kind=kind,
            cache_root=cache_root,
            workdir_root=workdir_root,
            s3=s3,
            ecs=ecs,
            aws_endpoint_url=api.AWS_ENDPOINT_URL,
            aws_region=api.AWS_REGION,
        )


def _s3_config_from_env() -> Optional[_S3Config]:
    """Snapshot the S3 cache config from env globals.

    Returns ``None`` when ``WORKFLOW_S3_BUCKET`` is unset.
    """
    if not api.WORKFLOW_S3_BUCKET:
        return None
    return _S3Config(
        bucket=api.WORKFLOW_S3_BUCKET,
        prefix=api.WORKFLOW_S3_PREFIX,
    )


def _ecs_config_from_env() -> Optional[_EcsConfig]:
    """Snapshot the ECS provisioner + discovery config from env globals.

    Returns ``None`` when ``ECS_CLUSTER`` is unset (PoC / s3-cached
    paths). Raises :class:`RuntimeError` when ``ECS_CLUSTER`` is set
    but a required field is missing — partial ECS config is never
    intentional in production, so failing fast surfaces the
    misconfiguration at boot rather than degrading to a quietly broken
    deployment running PoC fallback.
    """
    if not api.ECS_CLUSTER:
        return None
    missing: list[str] = []
    if not api.ECS_WORKER_TASK_DEFINITION:
        missing.append("ECS_WORKER_TASK_DEFINITION")
    if not api.ECS_WORKER_SUBNETS:
        missing.append("ECS_WORKER_SUBNETS")
    if missing:
        raise RuntimeError(
            "ECS_CLUSTER is set but the ECS profile is incomplete; "
            f"missing: {', '.join(missing)}. Set the missing knob(s) "
            "or unset ECS_CLUSTER to fall back to the PoC profile."
        )
    return _EcsConfig(
        cluster=api.ECS_CLUSTER,
        task_definition=api.ECS_WORKER_TASK_DEFINITION,
        task_family=api.ECS_WORKER_TASK_FAMILY,
        subnets=tuple(api.ECS_WORKER_SUBNETS),
        security_groups=tuple(api.ECS_WORKER_SECURITY_GROUPS),
        assign_public_ip=api.ECS_WORKER_ASSIGN_PUBLIC_IP,
    )
