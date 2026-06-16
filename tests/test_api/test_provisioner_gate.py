"""Tests for ``WorkflowProfile.from_env``'s partial-ECS gate.

The Q24 refactor folded ``_maybe_build_provisioner`` into
:meth:`cfdb.api.profile.WorkflowProfile.from_env` — the Q26 raise on
partial ECS config now lives in the profile's ``_ecs_config_from_env``
helper. These tests exercise that contract.
"""

from __future__ import annotations

import pytest

from cfdb import api
from cfdb.api.profile import WorkflowProfile


class TestWorkflowProfileEcsGate:
    def test_from_env_with_no_ecs_env_produces_local_or_s3_profile(
        self, monkeypatch, tmp_path
    ):
        """Test that the PoC profile falls through to ``local`` without raising.

        Given:
            ``SYNC_DATA_DIR`` set so the workflow subsystem activates,
            and no ECS env vars set.
        When:
            ``WorkflowProfile.from_env`` is invoked.
        Then:
            It should return a profile of kind ``"local"`` with no
            ECS config attached, exercising the PoC path that the
            laptop-dev profile depends on.
        """
        # Arrange
        monkeypatch.setattr(api, "SYNC_DATA_DIR", str(tmp_path))
        monkeypatch.setattr(api, "WORKFLOW_S3_BUCKET", None)
        monkeypatch.setattr(api, "ECS_CLUSTER", None)
        monkeypatch.setattr(api, "ECS_WORKER_TASK_DEFINITION", None)
        monkeypatch.setattr(api, "ECS_WORKER_SUBNETS", [])

        # Act
        profile = WorkflowProfile.from_env()

        # Assert
        assert profile is not None
        assert profile.kind == "local"
        assert profile.ecs is None
        assert profile.s3 is None

    def test_from_env_with_cluster_but_missing_task_def_raises(
        self, monkeypatch, tmp_path
    ):
        """Test that a partial config (cluster but no task-def) raises.

        Given:
            ``SYNC_DATA_DIR`` and ``ECS_CLUSTER`` set but
            ``ECS_WORKER_TASK_DEFINITION`` unset.
        When:
            ``WorkflowProfile.from_env`` is invoked.
        Then:
            It should raise ``RuntimeError`` naming the missing knob
            so operators see the misconfiguration at boot rather than
            silently degrading to the PoC profile.
        """
        # Arrange
        monkeypatch.setattr(api, "SYNC_DATA_DIR", str(tmp_path))
        monkeypatch.setattr(api, "ECS_CLUSTER", "cfdb-prod")
        monkeypatch.setattr(api, "ECS_WORKER_TASK_DEFINITION", None)
        monkeypatch.setattr(api, "ECS_WORKER_SUBNETS", ["subnet-a"])

        # Act & assert
        with pytest.raises(RuntimeError, match="ECS_WORKER_TASK_DEFINITION"):
            WorkflowProfile.from_env()

    def test_from_env_with_cluster_but_empty_subnets_raises(
        self, monkeypatch, tmp_path
    ):
        """Test that a partial config (cluster but no subnets) raises.

        Given:
            ``SYNC_DATA_DIR``, ``ECS_CLUSTER``, and
            ``ECS_WORKER_TASK_DEFINITION`` set but
            ``ECS_WORKER_SUBNETS`` empty.
        When:
            ``WorkflowProfile.from_env`` is invoked.
        Then:
            It should raise ``RuntimeError`` naming the missing knob so
            an awsvpc-mode worker cannot be launched without subnets.
        """
        # Arrange
        monkeypatch.setattr(api, "SYNC_DATA_DIR", str(tmp_path))
        monkeypatch.setattr(api, "ECS_CLUSTER", "cfdb-prod")
        monkeypatch.setattr(api, "ECS_WORKER_TASK_DEFINITION", "cfdb-worker")
        monkeypatch.setattr(api, "ECS_WORKER_SUBNETS", [])

        # Act & assert
        with pytest.raises(RuntimeError, match="ECS_WORKER_SUBNETS"):
            WorkflowProfile.from_env()

    def test_from_env_with_sync_data_dir_unset_returns_none(self, monkeypatch):
        """Test that the workflow subsystem stays disabled when SYNC_DATA_DIR is unset.

        Given:
            ``SYNC_DATA_DIR`` unset.
        When:
            ``WorkflowProfile.from_env`` is invoked.
        Then:
            It should return None so the lifespan skips workflow
            initialization and routers fall back to their
            direct-streaming paths.
        """
        # Arrange
        monkeypatch.setattr(api, "SYNC_DATA_DIR", None)

        # Act
        profile = WorkflowProfile.from_env()

        # Assert
        assert profile is None
