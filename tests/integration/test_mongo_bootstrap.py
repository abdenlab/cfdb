"""Clean-checkout boot of the MongoDB image, end to end through docker.

Reproduces the exact failure shape that once took the container down on a
fresh clone: ``database/`` is gitignored, so a clean checkout has no dump
to restore, and the startup script's ``mongosh /scripts/create-indexes.js``
run hit ``NamespaceNotFound`` on the never-written ``jobs`` collection —
which, under ``set -e``, exited the container. The unit-level lockstep
tests pin the JS source with regexes; only a real ``docker build`` + boot
proves the assembled image survives the empty-database path and still
lands the partial-unique mutex index.

Docker-gated: the whole module skips when the docker CLI is absent or the
daemon is unreachable, mirroring how tool-dependent integration suites
skip without their binaries.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Generous ceilings: the first build may pull mongo:latest, and mongod plus
# mongorestore-skip plus index creation can take a while on a cold machine.
BUILD_TIMEOUT_S = 600
BOOT_DEADLINE_S = 180
SETTLE_SECONDS = 3


def _docker_usable() -> bool:
    """Return True when the docker CLI exists and its daemon answers."""
    if shutil.which("docker") is None:
        return False
    try:
        probe = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return probe.returncode == 0


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _docker_usable(),
        reason="docker CLI missing or daemon unreachable",
    ),
]


def _docker(*args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    """Run a docker CLI command, capturing text output without raising."""
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _container_logs(name: str) -> str:
    """Return the container's combined stdout/stderr log stream."""
    result = _docker("logs", name)
    return result.stdout + result.stderr


def _container_running(name: str) -> bool:
    """Return True while docker reports the container's state as running."""
    result = _docker("inspect", "--format", "{{.State.Running}}", name)
    return result.returncode == 0 and result.stdout.strip() == "true"


class TestMongoCleanCheckoutBoot:
    def test_should_boot_with_empty_dump_dir_and_create_jobs_indexes(
        self, tmp_path
    ):
        """Test that the MongoDB image survives a clean-checkout boot.

        Given:
            The image built from Dockerfile.mongodb, started with an
            empty directory mounted read-only at /data/database — the
            shape of a clean checkout, where the gitignored dump
            directory has nothing to restore.
        When:
            The container starts and its logs are polled through the
            startup script's initialization sequence.
        Then:
            It should stay running (no set -e exit), report the
            "No dump at /data/database" guidance instead of a failed
            restore, and hold the jobs partial-unique mutex index that
            create-indexes.js lands on the never-written collection.
        """
        # Arrange
        suffix = uuid.uuid4().hex[:12]
        image_tag = f"cfdb-mongodb-boot-test:{suffix}"
        container = f"cfdb-mongodb-boot-{suffix}"
        empty_dump_dir = tmp_path / "database"
        empty_dump_dir.mkdir()

        try:
            build = _docker(
                "build",
                "-f",
                str(REPO_ROOT / "Dockerfile.mongodb"),
                "-t",
                image_tag,
                str(REPO_ROOT),
                timeout=BUILD_TIMEOUT_S,
            )
            assert build.returncode == 0, (
                f"docker build failed:\n{build.stdout}\n{build.stderr}"
            )

            # Act
            run = _docker(
                "run",
                "--detach",
                "--name",
                container,
                "--volume",
                f"{empty_dump_dir}:/data/database:ro",
                image_tag,
            )
            assert run.returncode == 0, (
                f"docker run failed:\n{run.stdout}\n{run.stderr}"
            )

            deadline = time.monotonic() + BOOT_DEADLINE_S
            logs = ""
            while time.monotonic() < deadline:
                logs = _container_logs(container)
                if "Development initialization complete" in logs:
                    break
                assert _container_running(container), (
                    "container exited before initialization completed "
                    f"(the pre-fix set -e failure shape); logs:\n{logs}"
                )
                time.sleep(1)
            else:
                pytest.fail(
                    "initialization did not complete within "
                    f"{BOOT_DEADLINE_S}s; logs:\n{logs}"
                )

            # Assert
            assert "No dump at /data/database" in logs
            assert "Restoring database from /data/database" not in logs

            # A brief settle then a running re-check: the pre-fix failure
            # was an exit 1 at the end of the startup script, so "still
            # up after initialization" is the behavior under test.
            time.sleep(SETTLE_SECONDS)
            assert _container_running(container), (
                f"container exited after initialization; logs:\n"
                f"{_container_logs(container)}"
            )

            indexes = _docker(
                "exec",
                container,
                "mongosh",
                "cfdb",
                "--quiet",
                "--eval",
                "JSON.stringify(db.jobs.getIndexes())",
                timeout=120,
            )
            assert indexes.returncode == 0, (
                f"mongosh index query failed:\n{indexes.stdout}\n"
                f"{indexes.stderr}"
            )
            specs = {
                spec["name"]: spec for spec in json.loads(indexes.stdout)
            }
            mutex = specs["workflow_key_active_unique"]
            assert mutex["key"] == {"workflow_key": 1}
            assert mutex["unique"] is True
            assert mutex["partialFilterExpression"] == {"active": True}
        finally:
            _docker("rm", "--force", container)
            _docker("rmi", "--force", image_tag)
