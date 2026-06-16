"""Tests for processor subprocess helpers (run_shell, run_argv)."""

from __future__ import annotations

from pathlib import Path

import pytest

from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.processors.tools import (
    copy_from_cache,
    format_name,
    run_argv,
    run_shell,
    shell_quote,
)


class TestRunShell:
    @pytest.mark.asyncio
    async def test_run_shell_should_succeed_when_all_stages_exit_zero(self):
        """Test the happy path for a multi-stage pipeline.

        Given:
            A shell pipeline whose stages all exit 0.
        When:
            run_shell is awaited.
        Then:
            It should return normally without raising.
        """
        # Act & assert
        await run_shell("true | true | true")

    @pytest.mark.asyncio
    async def test_run_shell_should_raise_when_final_stage_fails(self):
        """Test that a non-zero final-stage exit propagates as RuntimeError.

        Given:
            A shell pipeline whose last stage exits non-zero.
        When:
            run_shell is awaited.
        Then:
            It should raise RuntimeError carrying the returncode.
        """
        # Act & assert
        with pytest.raises(RuntimeError, match="shell command failed"):
            await run_shell("true | false")

    @pytest.mark.asyncio
    async def test_run_shell_should_raise_when_non_terminal_stage_fails(self):
        """Test that pipefail traps failures in non-terminal pipeline stages.

        Given:
            A shell pipeline where an upstream stage exits non-zero but
            the final stage (``true``) would otherwise succeed.
        When:
            run_shell is awaited.
        Then:
            It should raise RuntimeError because pipefail is enabled —
            without it the whole pipeline would falsely succeed on the
            final stage's zero exit, and a corrupt artifact could be
            committed to the content-addressed cache.
        """
        # Act & assert
        with pytest.raises(RuntimeError, match="shell command failed"):
            await run_shell("false | true")


class TestRunArgv:
    @pytest.mark.asyncio
    async def test_run_argv_should_succeed_when_subprocess_exits_zero(self):
        """Test that ``run_argv`` is a no-op on a clean exit.

        Given:
            ``run_argv(["true"])``.
        When:
            Awaited.
        Then:
            It should return normally without raising.
        """
        # Act & assert
        await run_argv(["true"])

    @pytest.mark.asyncio
    async def test_run_argv_should_raise_runtime_error_on_non_zero_exit(self):
        """Test that ``run_argv`` raises on non-zero exit.

        Given:
            ``run_argv(["false"])``.
        When:
            Awaited.
        Then:
            It should raise ``RuntimeError`` matching ``false exited 1``.
        """
        # Act & assert
        with pytest.raises(RuntimeError, match="false exited 1"):
            await run_argv(["false"])


class TestShellQuote:
    def test_shell_quote_should_wrap_paths_with_spaces_in_single_quotes(self):
        """Test that ``shell_quote`` produces shlex-safe output.

        Given:
            A Path containing a space.
        When:
            ``shell_quote`` is called.
        Then:
            It should return a single-quoted string safe to interpolate
            into a shell pipeline.
        """
        # Arrange
        path = Path("/tmp/with space/x.bam")

        # Act
        quoted = shell_quote(path)

        # Assert
        assert quoted == "'/tmp/with space/x.bam'"


class TestFormatName:
    def test_format_name_should_return_value_when_file_format_is_dict(self):
        """Test that the happy path returns ``file_format['name']``.

        Given:
            A file_meta with ``file_format={"name": "BAM"}``.
        When:
            ``format_name`` is called.
        Then:
            It should return ``"BAM"``.
        """
        # Act
        result = format_name({"file_format": {"name": "BAM"}})

        # Assert
        assert result == "BAM"

    def test_format_name_should_return_none_when_field_missing(self):
        """Test that missing ``file_format`` resolves to None.

        Given:
            A file_meta with no ``file_format`` key.
        When:
            ``format_name`` is called.
        Then:
            It should return None.
        """
        # Act & assert
        assert format_name({}) is None

    def test_format_name_should_return_none_when_value_is_not_dict(self):
        """Test that a non-dict ``file_format`` resolves to None.

        Given:
            A file_meta with ``file_format="BAM"`` (string, not dict).
        When:
            ``format_name`` is called.
        Then:
            It should return None — the helper expects the canonical
            ``{"name": ...}`` shape.
        """
        # Act & assert
        assert format_name({"file_format": "BAM"}) is None


class TestCopyFromCache:
    @pytest.mark.asyncio
    async def test_copy_from_cache_should_create_parents_and_write_bytes(
        self, tmp_path
    ):
        """Test that ``copy_from_cache`` creates parent dirs and writes bytes.

        Given:
            A cache holding an artifact and a destination under a
            non-existent parent directory.
        When:
            ``copy_from_cache(cache, key, dest)`` is awaited.
        Then:
            The parent directory should be created and the destination
            should contain the cached bytes.
        """
        # Arrange
        cache = LocalFsCache(tmp_path / "cache")
        source = tmp_path / "src"
        source.write_bytes(b"cached")
        await cache.put("encode/x/data/aa-v0", source)
        dest = tmp_path / "deep" / "nested" / "out.bin"

        # Act
        await copy_from_cache(cache, "encode/x/data/aa-v0", dest)

        # Assert
        assert dest.exists()
        assert dest.read_bytes() == b"cached"
