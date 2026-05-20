"""Tests for the tabix-family preprocessing pipeline."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from cfdb.workflows import SORT_MEMORY_CAP, keys as key_utils
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors import tabix as tabix_module
from cfdb.workflows.processors.tabix import TabixIntervalProcessor
from tests.test_workflows import FIXTURE_MD5


def _file_meta(fmt: str) -> dict[str, Any]:
    """Return a file_meta the tabix processor can consume."""
    return {
        "dcc": {"dcc_abbreviation": "ENCODE"},
        "local_id": f"ENCFF-{fmt}",
        "md5": FIXTURE_MD5,
        "access_url": f"https://example.com/{fmt.lower()}",
        "file_format": {"name": fmt},
    }


async def _drain_run(proc, file_meta, workdir, cache_root):
    """Drive the processor's async-generator ``run`` to completion."""
    return [event async for event in proc.run(file_meta, workdir, cache_root)]


def _final_artifacts(events: list[dict]) -> dict[str, str]:
    """Return the artifact mapping from the terminal ``complete`` event."""
    for event in reversed(events):
        if event.get("event") == "complete":
            return dict(event.get("artifacts") or {})
    raise AssertionError("processor did not emit a `complete` event")


class _PipelineHarness:
    """Capture shell/argv invocations and synthesize expected outputs.

    Each processor shell command ends with a ``> 'out.bgz'`` redirect;
    the harness writes a placeholder file at that path so the next stage
    (tabix) can find the artifact. Tabix calls are captured via argv.
    The C4 stage-2 empty-input pre-check (``_count_data_lines``) is
    stubbed to return a positive count so the harness's placeholder
    bytes survive the guard — tests that need to exercise the empty
    branch should patch ``_count_data_lines`` themselves.
    """

    def __init__(self) -> None:
        self.shells: list[str] = []
        self.argvs: list[list[str]] = []

    def install(self, mocker) -> None:
        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"placeholder-source")
            return dest

        async def fake_shell(cmd: str) -> None:
            self.shells.append(cmd)
            # Synthesize whatever file the pipeline redirects into.
            if ">" in cmd:
                out = cmd.rsplit(">", 1)[-1].strip().strip("'\"")
                dest = Path(out)
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(b"pipeline-output")

        async def fake_run(argv: list[str]) -> None:
            self.argvs.append(list(argv))
            if argv[0] == "tabix":
                target = Path(argv[-1])
                (target.parent / (target.name + ".tbi")).write_bytes(b"tbi")

        async def fake_count_data_lines(self, _bgz_path, _fmt):
            return 1

        mocker.patch.object(tabix_module, "download_source", fake_download)
        mocker.patch.object(tabix_module, "run_argv", fake_run)
        mocker.patch.object(tabix_module, "run_shell", fake_shell)
        mocker.patch.object(
            TabixIntervalProcessor,
            "_count_data_lines",
            new=fake_count_data_lines,
        )


class TestTabixIntervalProcessor:
    @pytest.mark.parametrize(
        "fmt",
        ["VCF", "GFF", "GFF3", "GTF", "BED", "BroadPeak", "NarrowPeak", "bigBed"],
    )
    def test_needs_processing_should_accept_all_supported_formats(self, fmt):
        """Test that every format in supported_formats is accepted.

        Given:
            A file_meta for each of the declared supported formats.
        When:
            needs_processing is invoked.
        Then:
            It should return True so the registry routes the file here.
        """
        # Act & assert
        assert TabixIntervalProcessor().needs_processing(_file_meta(fmt)) is True

    def test_needs_processing_should_reject_bam(self):
        """Test that alignment formats are rejected.

        Given:
            A BAM file_meta.
        When:
            needs_processing is invoked.
        Then:
            It should return False so the BAM processor gets the file.
        """
        # Act & assert
        assert TabixIntervalProcessor().needs_processing(_file_meta("BAM")) is False

    @pytest.mark.asyncio
    async def test_run_should_preserve_vcf_header_via_grep_split(
        self, tmp_path, mocker
    ):
        """Test that VCF sort separates the header block from the data block.

        Given:
            A VCF file_meta and mocked tool invocations.
        When:
            run is awaited.
        Then:
            The emitted shell pipeline should ``grep '^#'`` for the
            header block before piping the ``grep -v '^#'`` body through
            a memory-capped sort.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        events = await _drain_run(
            TabixIntervalProcessor(), _file_meta("VCF"), workdir, cache_root
        )
        artifacts = _final_artifacts(events)

        # Assert
        big_pipeline = next(
            c for c in harness.shells if "bgzip" in c and "grep" in c
        )
        # The C1 wrapper escapes ``^#`` in double quotes inside an
        # ``sh -c '...'`` invocation so a real grep failure (sort OOM,
        # disk-full, etc.) propagates instead of being swallowed by a
        # bare ``|| true``. The substring assertions confirm both header
        # and body arms are still present and routed through that
        # exit-code-aware wrapper.
        assert 'grep "^#"' in big_pipeline
        assert 'grep -v "^#"' in big_pipeline
        assert "[ $rc -le 1 ] && exit 0 || exit $rc" in big_pipeline
        assert "LC_ALL=C sort" in big_pipeline
        assert "bgzip -c" in big_pipeline
        cache = LocalFsCache(cache_root)
        assert await cache.head(artifacts["data"]) is not None
        assert await cache.head(artifacts["index"]) is not None

    @pytest.mark.asyncio
    async def test_run_should_invoke_gffread_before_sort_for_gtf(
        self, tmp_path, mocker
    ):
        """Test that GTF conversion runs via gffread before sort.

        Given:
            A GTF file_meta and mocked tools.
        When:
            run is awaited.
        Then:
            The pipeline shell string must contain ``gffread -E`` piped
            into a sort stage and then bgzip.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("GTF"), workdir, cache_root
        )

        # Assert
        big_pipeline = next(c for c in harness.shells if "gffread -E" in c)
        assert "LC_ALL=C sort" in big_pipeline
        assert big_pipeline.index("gffread") < big_pipeline.index("sort")

    @pytest.mark.asyncio
    async def test_run_should_invoke_bigbedtobed_for_bigbed_inputs(
        self, tmp_path, mocker
    ):
        """Test that bigBed inputs pipe through bigBedToBed first.

        Given:
            A bigBed file_meta and mocked tools.
        When:
            run is awaited.
        Then:
            The shell pipeline starts with bigBedToBed writing to stdout
            and does not invoke zcat (bigBed is binary).
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("bigBed"), workdir, cache_root
        )

        # Assert
        pipeline = next(c for c in harness.shells if "bigBedToBed" in c)
        assert "stdout" in pipeline
        assert "zcat" not in pipeline

    @pytest.mark.asyncio
    async def test_run_should_apply_memory_cap_and_locale_to_sort(
        self, tmp_path, mocker
    ):
        """Test that sort invocations are memory-capped and locale-safe.

        Given:
            A BED file_meta and the harness observing all shell commands.
        When:
            run is awaited.
        Then:
            Every sort invocation must include ``LC_ALL=C``, ``-S`` with
            the module-level memory cap, and ``-T`` pointing at a tmpdir.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("BED"), workdir, cache_root
        )

        # Assert
        pipeline = next(c for c in harness.shells if "sort" in c and "bgzip" in c)
        assert "LC_ALL=C sort" in pipeline
        assert f"-S {SORT_MEMORY_CAP}" in pipeline
        assert "-T " in pipeline

    @pytest.mark.asyncio
    async def test_run_should_invoke_tabix_with_preset_matching_format(
        self, tmp_path, mocker
    ):
        """Test that tabix receives the correct preset per format.

        Given:
            A GFF3 file_meta.
        When:
            run is awaited.
        Then:
            tabix is invoked with ``-p gff`` — confirming GFF3 is routed
            onto the GFF preset rather than silently bypassed.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("GFF3"), workdir, cache_root
        )

        # Assert
        tabix_call = next(a for a in harness.argvs if a[0] == "tabix")
        assert tabix_call[:3] == ["tabix", "-p", "gff"]

    @pytest.mark.asyncio
    async def test_run_should_skip_stage_one_when_data_already_cached(
        self, tmp_path, mocker
    ):
        """Test that partial-commit recovery skips prep when data cached.

        Given:
            A cache seeded with the data artifact for a VCF workflow.
        When:
            run is awaited.
        Then:
            Only the tabix indexing should run.
        """
        # Arrange
        processor = TabixIntervalProcessor()
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        workdir.mkdir()
        cache_root.mkdir()

        data_key = key_utils.cache_key(
            dcc="encode",
            local_id="ENCFF-VCF",
            artifact_kind=ArtifactKind.DATA,
            md5=FIXTURE_MD5,
            processor_version=processor.processor_version,
        )
        cache = LocalFsCache(cache_root)
        seed = workdir / "seed.bgz"
        seed.write_bytes(b"cached-bgz")
        await cache.put(data_key, seed)

        calls: list[list[str]] = []

        async def fake_shell(cmd):
            raise AssertionError(f"stage 1 shell should not run: {cmd}")

        async def fake_download(_meta, _dest):
            raise AssertionError("download should not happen when data cached")

        async def fake_run(argv):
            calls.append(list(argv))
            if argv[0] == "tabix":
                target = Path(argv[-1])
                (target.parent / (target.name + ".tbi")).write_bytes(b"tbi")

        mocker.patch.object(tabix_module, "download_source", fake_download)
        mocker.patch.object(tabix_module, "run_argv", fake_run)
        mocker.patch.object(tabix_module, "run_shell", fake_shell)

        # Act
        events = await _drain_run(processor, _file_meta("VCF"), workdir, cache_root)
        artifacts = _final_artifacts(events)

        # Assert
        assert [a[0] for a in calls] == ["tabix"]
        assert await cache.head(artifacts["index"]) is not None


# Integration tests using real htslib live under tests/integration/
# per the project test guide — see
# tests/integration/test_direct_processors.py for the direct-call
# siblings and tests/integration/test_processor_e2e.py for the full
# Wool-dispatched variants.


class _FakeProc:
    """Synchronous async-process double for mocking ``create_subprocess_exec``."""

    def __init__(self, stdout: bytes, stderr: bytes, returncode: int) -> None:
        self._stdout = stdout
        self._stderr = stderr
        self.returncode = returncode
        self.pid = 4321

    async def communicate(self):
        return self._stdout, self._stderr


class TestTabixIntervalProcessorPipelines:
    @pytest.mark.parametrize("fmt", ["BroadPeak", "NarrowPeak", "GFF"])
    @pytest.mark.asyncio
    async def test_run_should_emit_plain_zcat_sort_bgzip_pipeline_for_plain_formats(
        self, tmp_path, mocker, fmt
    ):
        """Test (TX-001) that BroadPeak/NarrowPeak/GFF use the plain pipeline.

        Given:
            A file_meta for each plain-format input and a ``_PipelineHarness``.
        When:
            ``run`` is awaited.
        Then:
            The emitted shell pipeline should match ``zcat -f <src> |
            LC_ALL=C sort ... | bgzip -c > <bgz>`` and tabix should be
            invoked with the matching preset.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta(fmt), workdir, cache_root
        )

        # Assert
        pipeline = next(c for c in harness.shells if "bgzip" in c and "sort" in c)
        assert "zcat -f" in pipeline
        assert "LC_ALL=C sort" in pipeline
        assert "bgzip -c" in pipeline
        tabix_call = next(a for a in harness.argvs if a[0] == "tabix")
        assert tabix_call[1] == "-p"

    @pytest.mark.asyncio
    async def test_run_should_refuse_to_commit_empty_artifact(
        self, tmp_path, mocker
    ):
        """Test (TX-002) that header-only inputs are rejected before cache commit.

        Given:
            A BED file_meta with ``_count_data_lines`` patched to return 0.
        When:
            ``run`` is awaited.
        Then:
            It should raise ``RuntimeError`` matching "Refusing to commit
            empty" and the cache should remain untouched.
        """
        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"placeholder-source")
            return dest

        async def fake_shell(cmd):
            if ">" in cmd:
                out = cmd.rsplit(">", 1)[-1].strip().strip("'\"")
                Path(out).parent.mkdir(parents=True, exist_ok=True)
                Path(out).write_bytes(b"")

        async def fake_run(_argv):
            return None

        async def zero_count(self, _bgz, _fmt):
            return 0

        mocker.patch.object(tabix_module, "download_source", fake_download)
        mocker.patch.object(tabix_module, "run_shell", fake_shell)
        mocker.patch.object(tabix_module, "run_argv", fake_run)
        mocker.patch.object(
            TabixIntervalProcessor, "_count_data_lines", new=zero_count
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="Refusing to commit empty"):
            async for _event in TabixIntervalProcessor().run(
                _file_meta("BED"), workdir, cache_root
            ):
                pass
        cache = LocalFsCache(cache_root)
        # No data artifact should have been committed.
        data_key = key_utils.cache_key(
            dcc="encode",
            local_id="ENCFF-BED",
            artifact_kind=ArtifactKind.DATA,
            md5=FIXTURE_MD5,
            processor_version=TabixIntervalProcessor().processor_version,
        )
        assert await cache.head(data_key) is None

    @pytest.mark.asyncio
    async def test_run_should_use_bed_sort_keys_for_bed(self, tmp_path, mocker):
        """Test (TX-003) that BED inputs sort by ``-k1,1 -k2,2n``.

        Given:
            A BED file_meta.
        When:
            ``run`` is awaited and the shell command is captured.
        Then:
            The sort invocation should contain ``-k1,1`` and ``-k2,2n``.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("BED"), workdir, cache_root
        )

        # Assert
        pipeline = next(c for c in harness.shells if "sort" in c and "bgzip" in c)
        assert "-k1,1" in pipeline
        assert "-k2,2n" in pipeline

    @pytest.mark.asyncio
    async def test_run_should_use_gff_sort_keys_for_gff(self, tmp_path, mocker):
        """Test (TX-004) that GFF inputs sort by ``-k1,1 -k4,4n``.

        Given:
            A GFF file_meta.
        When:
            ``run`` is awaited and the shell command is captured.
        Then:
            The sort invocation should contain ``-k1,1`` and ``-k4,4n``.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("GFF"), workdir, cache_root
        )

        # Assert
        pipeline = next(c for c in harness.shells if "sort" in c and "bgzip" in c)
        assert "-k1,1" in pipeline
        assert "-k4,4n" in pipeline

    @pytest.mark.asyncio
    async def test_run_should_use_vcf_sort_keys_for_vcf(self, tmp_path, mocker):
        """Test (TX-005) that VCF inputs sort by ``-k1,1 -k2,2n``.

        Given:
            A VCF file_meta.
        When:
            ``run`` is awaited and the shell command is captured.
        Then:
            The sort invocation should contain ``-k1,1`` and ``-k2,2n``.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("VCF"), workdir, cache_root
        )

        # Assert
        pipeline = next(
            c for c in harness.shells if "bgzip" in c and "grep" in c
        )
        assert "-k1,1" in pipeline
        assert "-k2,2n" in pipeline

    @pytest.mark.asyncio
    async def test_run_should_route_gtf_through_gff_preset(self, tmp_path, mocker):
        """Test (TX-006) that GTF inputs invoke tabix with ``-p gff``.

        Given:
            A GTF file_meta.
        When:
            ``run`` is awaited.
        Then:
            tabix should be invoked with ``-p gff`` because GTF maps onto
            the GFF tabix preset after gffread conversion.
        """
        # Arrange
        harness = _PipelineHarness()
        harness.install(mocker)
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        # Act
        await _drain_run(
            TabixIntervalProcessor(), _file_meta("GTF"), workdir, cache_root
        )

        # Assert
        tabix_call = next(a for a in harness.argvs if a[0] == "tabix")
        assert tabix_call[:3] == ["tabix", "-p", "gff"]


class TestCountDataLines:
    @pytest.mark.asyncio
    async def test_count_data_lines_should_return_int_when_subprocess_succeeds(
        self, tmp_path, mocker
    ):
        """Test (TX-007) that successful bgzip pipeline output is parsed.

        Given:
            A pre-built bgz path and a mocked subprocess returning
            ``"3\\n"`` on stdout.
        When:
            ``_count_data_lines`` is awaited.
        Then:
            It should return ``3``.
        """
        # Arrange
        fake = _FakeProc(b"3\n", b"", 0)

        async def fake_exec(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_exec", fake_exec)
        bgz = tmp_path / "out.bgz"
        bgz.write_bytes(b"placeholder")

        # Act
        count = await TabixIntervalProcessor()._count_data_lines(bgz, "BED")

        # Assert
        assert count == 3

    @pytest.mark.asyncio
    async def test_count_data_lines_should_raise_runtime_error_on_bgzip_failure(
        self, tmp_path, mocker
    ):
        """Test (TX-008) that a bgzip failure surfaces as RuntimeError.

        Given:
            A mocked subprocess that exits non-zero with stderr
            ``"corrupt"``.
        When:
            ``_count_data_lines`` is awaited.
        Then:
            It should raise ``RuntimeError`` mentioning the path and the
            stderr text.
        """
        # Arrange
        fake = _FakeProc(b"", b"corrupt", 2)

        async def fake_exec(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_exec", fake_exec)
        bgz = tmp_path / "out.bgz"
        bgz.write_bytes(b"placeholder")

        # Act & assert
        with pytest.raises(RuntimeError, match="corrupt"):
            await TabixIntervalProcessor()._count_data_lines(bgz, "BED")
