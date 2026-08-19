"""Tests for the BAM/SAM preprocessing pipeline."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from cfdb.workflows import SAMTOOLS_MEMORY_CAP, keys as key_utils
from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.events import Complete
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors import bam as bam_module
from cfdb.workflows.processors.bam import (
    BamIndexProcessor,
    read_bam_header,
    validate_sam_header,
)
from tests.test_workflows import FIXTURE_MD5


async def _fake_peek_valid_header(file_meta, **kwargs):
    """Stub peek_decompressed_prefix with a well-formed tab-delimited header."""
    return b"@HD\tVN:1.0\n@SQ\tSN:chr1\tLN:1000\n"


class TestValidateSamHeader:
    def test_should_raise_on_space_delimited_sq_line(self):
        """Test that a space-delimited @SQ header is rejected.

        Given:
            A SAM header whose @SQ record uses spaces (the legacy
            modENCODE shape) instead of tabs between fields.
        When:
            validate_sam_header is called.
        Then:
            It should raise RuntimeError naming the space-delimited
            line, so the workflow fails fast with an actionable error
            rather than samtools' opaque "fail to read the header".
        """
        # Arrange — spaces between fields, a legitimate space inside AS
        header = "@HD VN:1.0\n@SQ SN:chr2L AS:FlyBase r5 LN:23011544\n"

        # Act & assert
        with pytest.raises(RuntimeError, match="space-delimited"):
            validate_sam_header(header)

    def test_should_pass_well_formed_tab_delimited_header(self):
        """Test that a tab-delimited header is accepted.

        Given:
            A SAM header whose records are TAB-delimited, with a space
            preserved inside a tag value.
        When:
            validate_sam_header is called.
        Then:
            It should return None without raising.
        """
        # Arrange
        header = "@HD\tVN:1.0\n@SQ\tSN:chr2L\tAS:FlyBase r5\tLN:23011544\n"

        # Act & assert — no exception
        assert validate_sam_header(header) is None

    def test_should_ignore_comment_records_without_tabs(self):
        """Test that a tab-less @CO comment does not trip the check.

        Given:
            A header with a valid tab-delimited @SQ and an @CO comment
            line carrying free text with no tab.
        When:
            validate_sam_header is called.
        Then:
            It should not raise, since @CO is free text and excluded
            from the delimiter check.
        """
        # Arrange
        header = "@SQ\tSN:chr1\tLN:1000\n@CO free text comment with no tab\n"

        # Act & assert — no exception
        assert validate_sam_header(header) is None

    def test_should_be_noop_for_headerless_input(self):
        """Test that input with no header records is left to samtools.

        Given:
            Header text containing no @-prefixed multi-field records.
        When:
            validate_sam_header is called.
        Then:
            It should return None — the truly-headerless case is a
            different failure surfaced by samtools itself.
        """
        # Act & assert
        assert validate_sam_header("") is None


def _file_meta(fmt: str = "BAM") -> dict[str, Any]:
    """Return a BAM/SAM file_meta with fields the processor reads."""
    return {
        "dcc": {"dcc_abbreviation": "ENCODE"},
        "local_id": "ENCFF123",
        "md5": FIXTURE_MD5,
        "access_url": "https://example.com/x.bam",
        "file_format": {"name": fmt},
    }


async def _drain_run(proc, file_meta, workdir, cache_root):
    """Drive the processor's async-generator ``run`` to completion.

    Returns the list of events yielded. Tests historically expected
    ``run`` to return a ``{kind: cache_key}`` dict; the executor's
    streaming-routine refactor turned ``run`` into an
    ``AsyncIterator[dict]``. This helper bridges that signature change
    and surfaces the final ``complete`` event's artifact mapping under
    the same dict key callers used to assert on.
    """
    return [
        event
        async for event in proc.run(file_meta, workdir, LocalFsCache(cache_root))
    ]


def _final_artifacts(events: list) -> dict[str, str]:
    """Return the artifact mapping carried by the terminal ``Complete`` event."""
    for event in reversed(events):
        if isinstance(event, Complete):
            return dict(event.artifacts)
    raise AssertionError("processor did not emit a `Complete` event")


async def _async_false() -> bool:
    """Coroutine returning False — used to stub ``_records_appear_sorted``."""
    return False


class TestBamIndexProcessor:
    def test_processor_id_should_be_the_pinned_literal(self):
        """Test that the shipped identity is exactly "bam-index".

        Given:
            The shipped BamIndexProcessor class.
        When:
            processor_id is read off it.
        Then:
            It should be exactly "bam-index". The literal is asserted
            rather than derived because it is a wire constant — every
            cached BAI is keyed under it, so changing the string silently
            invalidates the processor's whole cached corpus.
        """
        # Act & assert
        assert BamIndexProcessor.processor_id == "bam-index"

    def test_needs_processing_should_accept_bam(self):
        """Test that the processor claims BAM inputs.

        Given:
            A BamIndexProcessor and a file_meta whose format is BAM.
        When:
            needs_processing is invoked.
        Then:
            It should return True.
        """
        # Act & assert
        assert BamIndexProcessor().needs_processing(_file_meta("BAM")) is True

    def test_needs_processing_should_accept_sam(self):
        """Test that the processor claims SAM inputs.

        Given:
            A BamIndexProcessor and a file_meta whose format is SAM.
        When:
            needs_processing is invoked.
        Then:
            It should return True — SAM is routed through the same
            pipeline after a view-to-BAM conversion.
        """
        # Act & assert
        assert BamIndexProcessor().needs_processing(_file_meta("SAM")) is True

    def test_needs_processing_should_reject_vcf(self):
        """Test that the processor rejects non-alignment formats.

        Given:
            A BamIndexProcessor and a VCF file_meta.
        When:
            needs_processing is invoked.
        Then:
            It should return False so the registry can delegate to the
            tabix processor.
        """
        # Act & assert
        assert BamIndexProcessor().needs_processing(_file_meta("VCF")) is False

    def test_artifact_kinds_produced_should_advertise_index_only_for_bam(self):
        """Test that BAM advertises only the INDEX artifact.

        Given:
            A BAM file_meta. The DCC corpus publishes BAMs that are
            already coordinate-sorted, so cfdb caches only the BAI and
            ``/data`` falls through to upstream streaming.
        When:
            artifact_kinds_produced is invoked with that file_meta.
        Then:
            It should return ``(INDEX,)`` — no DATA — so the router's
            cache-stream helper skips dispatch for the data path.
        """
        # Act
        kinds = BamIndexProcessor().artifact_kinds_produced(_file_meta("BAM"))

        # Assert
        assert kinds == (ArtifactKind.INDEX,)

    def test_artifact_kinds_produced_should_advertise_data_and_index_for_sam(
        self,
    ):
        """Test that SAM advertises both DATA and INDEX artifacts.

        Given:
            A SAM file_meta. SAM always requires conversion + sort,
            and there is no upstream BAM to stream from for ``/data``,
            so cfdb caches both the sorted BAM and its BAI.
        When:
            artifact_kinds_produced is invoked with that file_meta.
        Then:
            It should return ``(DATA, INDEX)``.
        """
        # Act
        kinds = BamIndexProcessor().artifact_kinds_produced(_file_meta("SAM"))

        # Assert
        assert kinds == (ArtifactKind.DATA, ArtifactKind.INDEX)

    @pytest.mark.asyncio
    async def test_run_should_produce_index_only_for_pre_sorted_bam(
        self, tmp_path, mocker
    ):
        """Test that BAM run skips the sort and writes only the INDEX cache entry.

        Given:
            A cold cache, a BAM file_meta, mocked download + a header
            reader returning ``@HD ... SO:coordinate``, and mocked
            ``samtools index``.
        When:
            run is awaited.
        Then:
            ``samtools sort`` must NOT be invoked, and the returned
            mapping carries only the INDEX cache key.
        """
        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"bam-bytes")
            return dest

        async def fake_header(_path):
            return "@HD\tVN:1.6\tSO:coordinate\n"

        run_calls: list[list[str]] = []

        async def fake_run(argv):
            run_calls.append(list(argv))
            if argv[:2] == ["samtools", "index"]:
                # The bam path is always the last argv element; the
                # production call shape is ``samtools index -@ N <bam>``.
                target = Path(argv[-1])
                (target.parent / (target.name + ".bai")).write_bytes(b"bai")
            else:
                raise AssertionError(f"unexpected argv for BAM run: {argv}")

        mocker.patch.object(bam_module, "download_source", fake_download)
        mocker.patch.object(bam_module, "read_bam_header", fake_header)
        mocker.patch.object(bam_module, "run_argv", fake_run)

        # Act
        events = await _drain_run(
            BamIndexProcessor(), _file_meta("BAM"), workdir, cache_root
        )

        # Assert
        cache = LocalFsCache(cache_root)
        artifacts = _final_artifacts(events)
        assert "data" not in artifacts
        assert await cache.head(artifacts["index"]) is not None
        assert [argv[:2] for argv in run_calls] == [["samtools", "index"]]

    @pytest.mark.asyncio
    async def test_run_should_raise_when_bam_is_not_coordinate_sorted(
        self, tmp_path, mocker
    ):
        """Test that an unsorted BAM surfaces a clear error.

        Given:
            A BAM file_meta whose header reports ``SO:queryname`` rather
            than ``SO:coordinate``.
        When:
            run is awaited.
        Then:
            It should raise RuntimeError naming the bad sort order so
            operators can route the file through a sort-aware processor
            instead of producing a broken BAI.
        """
        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"bam-bytes")
            return dest

        async def fake_header(_path):
            return "@HD\tVN:1.6\tSO:queryname\n"

        async def fake_run(_argv):
            raise AssertionError("samtools should not run for unsorted BAM")

        mocker.patch.object(bam_module, "download_source", fake_download)
        mocker.patch.object(bam_module, "read_bam_header", fake_header)
        mocker.patch.object(bam_module, "run_argv", fake_run)
        # The C22 fallback record-scan must also fail or it would
        # silently rescue an unsorted BAM. Force the scan to report
        # "not sorted" so the strict-rejection branch fires.
        mocker.patch.object(
            BamIndexProcessor,
            "_records_appear_sorted",
            new=lambda self, *_a, **_kw: _async_false(),
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="not coordinate-sorted"):
            async for _event in BamIndexProcessor().run(
                _file_meta("BAM"), workdir, LocalFsCache(cache_root)
            ):
                pass

    @pytest.mark.asyncio
    async def test_run_should_skip_index_step_when_index_already_cached(
        self, tmp_path, mocker
    ):
        """Test that BAM run is a no-op when the index is already cached.

        Given:
            A cache already containing the BAI for this BAM.
        When:
            run is awaited.
        Then:
            It should not download, read the header, or run any
            samtools command — the cache hit short-circuits the entire
            pipeline.
        """
        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        processor = BamIndexProcessor()
        index_key = key_utils.cache_key(
            dcc="encode",
            local_id="ENCFF123",
            artifact_kind=ArtifactKind.INDEX,
            md5=FIXTURE_MD5,
            processor_id=processor.processor_id,
            processor_version=processor.processor_version,
        )
        cache = LocalFsCache(cache_root)
        seed = workdir / "preexisting.bai"
        workdir.mkdir(parents=True)
        seed.write_bytes(b"already-indexed")
        await cache.put(index_key, seed)

        async def fail_call(*_a, **_kw):
            raise AssertionError("nothing should be called when index cached")

        mocker.patch.object(bam_module, "download_source", fail_call)
        mocker.patch.object(bam_module, "read_bam_header", fail_call)
        mocker.patch.object(bam_module, "run_argv", fail_call)

        # Act
        events = await _drain_run(
            processor, _file_meta("BAM"), workdir, cache_root
        )

        # Assert
        assert _final_artifacts(events) == {ArtifactKind.INDEX.value: index_key}

    @pytest.mark.asyncio
    async def test_run_should_apply_memory_cap_to_samtools_sort_for_sam(
        self, tmp_path, mocker
    ):
        """Test that the SAM convert+sort pipeline carries the memory cap.

        Given:
            A SAM file_meta and mocked shell/argv runners.
        When:
            run is awaited.
        Then:
            The shell pipeline must contain ``samtools sort -m
            <memory cap>`` and a ``-T`` prefix under the workdir, so
            large SAMs spill to disk rather than OOMing the worker.
        """
        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        shells: list[str] = []

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"sam-bytes")
            return dest

        async def fake_shell(cmd):
            shells.append(cmd)
            parts = cmd.split()
            out_idx = parts.index("-o") + 1
            out_path = parts[out_idx].strip("'\"")
            dest = Path(out_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"sorted")

        async def fake_run(argv):
            if argv[:2] == ["samtools", "index"]:
                # The bam path is always the last argv element; the
                # production call shape is ``samtools index -@ N <bam>``.
                target = Path(argv[-1])
                (target.parent / (target.name + ".bai")).write_bytes(b"bai")

        mocker.patch.object(
            bam_module, "peek_decompressed_prefix", _fake_peek_valid_header
        )
        mocker.patch.object(bam_module, "download_source", fake_download)
        mocker.patch.object(bam_module, "run_shell", fake_shell)
        mocker.patch.object(bam_module, "run_argv", fake_run)

        # Act
        await _drain_run(
            BamIndexProcessor(), _file_meta("SAM"), workdir, cache_root
        )

        # Assert
        assert len(shells) == 1
        pipeline = shells[0]
        assert f"-m {SAMTOOLS_MEMORY_CAP}" in pipeline
        assert "-T " in pipeline

    @pytest.mark.asyncio
    async def test_run_should_skip_sort_when_data_artifact_cached_for_sam(
        self, tmp_path, mocker
    ):
        """Test that partial-commit recovery skips stage 1 on SAM retry.

        Given:
            A SAM workflow whose stage-1 sorted-BAM artifact is already
            in the cache.
        When:
            run is awaited.
        Then:
            It should skip the convert+sort pipeline entirely, copy the
            cached sorted BAM back to the workdir, and run only
            ``samtools index``.
        """
        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        processor = BamIndexProcessor()
        data_key = key_utils.cache_key(
            dcc="encode",
            local_id="ENCFF123",
            artifact_kind=ArtifactKind.DATA,
            md5=FIXTURE_MD5,
            processor_id=processor.processor_id,
            processor_version=processor.processor_version,
        )
        cache = LocalFsCache(cache_root)
        stage1_seed = workdir / "prepopulate.bam"
        workdir.mkdir(parents=True)
        stage1_seed.write_bytes(b"already-sorted")
        await cache.put(data_key, stage1_seed)

        run_calls: list[list[str]] = []

        async def fake_run(argv):
            run_calls.append(argv)
            if argv[:2] == ["samtools", "index"]:
                # The bam path is always the last argv element; the
                # production call shape is ``samtools index -@ N <bam>``.
                target = Path(argv[-1])
                (target.parent / (target.name + ".bai")).write_bytes(b"bai")
            else:
                raise AssertionError(
                    f"sort should not run when data already cached: {argv}"
                )

        async def fake_download(_meta, _dest):
            raise AssertionError("download should not happen when data cached")

        async def fake_shell(_cmd):
            raise AssertionError(
                "convert+sort pipeline should not run when data cached"
            )

        mocker.patch.object(bam_module, "download_source", fake_download)
        mocker.patch.object(bam_module, "run_argv", fake_run)
        mocker.patch.object(bam_module, "run_shell", fake_shell)

        # Act
        events = await _drain_run(
            processor, _file_meta("SAM"), workdir, cache_root
        )

        # Assert
        artifacts = _final_artifacts(events)
        assert [argv[:2] for argv in run_calls] == [["samtools", "index"]]
        assert await cache.head(artifacts["index"]) is not None
        assert await cache.head(artifacts["data"]) is not None

    @pytest.mark.asyncio
    async def test_run_should_stream_sam_through_view_and_sort_pipeline(
        self, tmp_path, mocker
    ):
        """Test that SAM inputs flow through a single view | sort shell pipeline.

        Given:
            A SAM file_meta and mocked shell/argv runners.
        When:
            run is awaited.
        Then:
            A single shell pipeline must chain ``samtools view -bS`` into
            ``samtools sort`` (no intermediate BAM file is materialized),
            and the subsequent ``samtools index`` is invoked via argv.
        """
        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        shells: list[str] = []
        argvs: list[list[str]] = []

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"sam-bytes")
            return dest

        async def fake_shell(cmd):
            shells.append(cmd)
            # The pipeline ends in ``-o 'sorted.bam'`` for sort; extract
            # and write the expected output file.
            if "-o" in cmd:
                parts = cmd.split()
                out_idx = parts.index("-o") + 1
                out_path = parts[out_idx].strip("'\"")
                dest = Path(out_path)
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(b"sorted")

        async def fake_run(argv):
            argvs.append(list(argv))
            if argv[:2] == ["samtools", "index"]:
                # The bam path is always the last argv element; the
                # production call shape is ``samtools index -@ N <bam>``.
                target = Path(argv[-1])
                (target.parent / (target.name + ".bai")).write_bytes(b"bai")

        mocker.patch.object(
            bam_module, "peek_decompressed_prefix", _fake_peek_valid_header
        )
        mocker.patch.object(bam_module, "download_source", fake_download)
        mocker.patch.object(bam_module, "run_argv", fake_run)
        mocker.patch.object(bam_module, "run_shell", fake_shell)

        # Act
        await _drain_run(
            BamIndexProcessor(), _file_meta("SAM"), workdir, cache_root
        )

        # Assert
        assert len(shells) == 1
        pipeline = shells[0]
        assert "samtools view -bS" in pipeline
        assert "| samtools sort" in pipeline
        assert "-m" in pipeline
        assert "-T" in pipeline
        index_argv = next(a for a in argvs if a[0] == "samtools" and a[1] == "index")
        assert index_argv[:2] == ["samtools", "index"]


# Integration tests using real samtools live under tests/integration/
# per the project test guide — see
# tests/integration/test_direct_processors.py for the direct-call
# sibling and tests/integration/test_processor_e2e.py for the full
# Wool-dispatched variant.


class _FakeProc:
    """Synchronous async-process double for mocking ``create_subprocess_*``.

    Mirrors enough of ``asyncio.subprocess.Process`` for the bam-module
    helpers to consume: ``communicate`` returns the configured
    stdout/stderr bytes and exposes ``returncode`` and ``pid``.
    """

    def __init__(self, stdout: bytes, stderr: bytes, returncode: int) -> None:
        self._stdout = stdout
        self._stderr = stderr
        self.returncode = returncode
        self.pid = 1234

    async def communicate(self):
        return self._stdout, self._stderr


class TestReadBamHeader:
    @pytest.mark.asyncio
    async def test_read_bam_header_should_return_decoded_stdout_on_zero_exit(
        self, tmp_path, mocker
    ):
        """Test that a successful samtools call returns its stdout.

        Given:
            ``samtools view -H`` exits 0 with a single ``@HD`` line on
            stdout.
        When:
            ``read_bam_header`` is awaited against a BAM path.
        Then:
            It should return the decoded stdout verbatim.
        """
        # Arrange
        fake = _FakeProc(b"@HD\tVN:1.6\tSO:coordinate\n", b"", 0)

        async def fake_exec(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_exec", fake_exec)

        # Act
        header = await read_bam_header(tmp_path / "x.bam")

        # Assert
        assert "@HD" in header
        assert "SO:coordinate" in header

    @pytest.mark.asyncio
    async def test_read_bam_header_should_raise_runtime_error_on_non_zero_exit(
        self, tmp_path, mocker
    ):
        """Test that a non-zero samtools exit surfaces a clear RuntimeError.

        Given:
            ``samtools view -H`` exits 2 with stderr ``bad bam``.
        When:
            ``read_bam_header`` is awaited.
        Then:
            It should raise ``RuntimeError`` mentioning the tool name and
            stderr text so operators can triage.
        """
        # Arrange
        fake = _FakeProc(b"", b"bad bam", 2)

        async def fake_exec(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_exec", fake_exec)

        # Act & assert
        with pytest.raises(RuntimeError, match="samtools view -H"):
            await read_bam_header(tmp_path / "x.bam")


class TestRecordsAppearSorted:
    @pytest.mark.asyncio
    async def test_records_appear_sorted_should_return_true_for_monotone_positions(
        self, tmp_path, mocker
    ):
        """Test that monotone POS within one reference returns True.

        Given:
            A mocked shell that emits 5 tab-separated lines with strictly
            increasing POS within a single reference.
        When:
            ``_records_appear_sorted`` is awaited.
        Then:
            It should return True.
        """
        # Arrange
        stdout = b"\n".join(
            f"r1\t0\tchr1\t{i}\t60\t*\t*\t*\t*\t*\t*".encode()
            for i in range(100, 105)
        )
        fake = _FakeProc(stdout, b"", 0)

        async def fake_shell(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_shell", fake_shell)

        # Act
        ok = await BamIndexProcessor()._records_appear_sorted(tmp_path / "x.bam")

        # Assert
        assert ok is True

    @pytest.mark.asyncio
    async def test_records_appear_sorted_should_return_false_for_decreasing_positions(
        self, tmp_path, mocker
    ):
        """Test that decreasing POS within one reference returns False.

        Given:
            A mocked shell whose stdout shows POS decreasing within a
            single reference.
        When:
            ``_records_appear_sorted`` is awaited.
        Then:
            It should return False so an unsorted BAM is rejected.
        """
        # Arrange
        stdout = (
            b"r1\t0\tchr1\t200\t60\t*\t*\t*\t*\t*\t*\n"
            b"r1\t0\tchr1\t100\t60\t*\t*\t*\t*\t*\t*\n"
        )
        fake = _FakeProc(stdout, b"", 0)

        async def fake_shell(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_shell", fake_shell)

        # Act
        ok = await BamIndexProcessor()._records_appear_sorted(tmp_path / "x.bam")

        # Assert
        assert ok is False

    @pytest.mark.asyncio
    async def test_records_appear_sorted_should_treat_sigpipe_as_success(
        self, tmp_path, mocker
    ):
        """Test that SIGPIPE (141) with sorted output is treated as success.

        Given:
            A mocked shell that exits 141 (SIGPIPE — head closed pipe)
            with partial but sorted stdout.
        When:
            ``_records_appear_sorted`` is awaited.
        Then:
            It should return True — SIGPIPE is expected when ``head``
            closes the pipe early.
        """
        # Arrange
        stdout = (
            b"r1\t0\tchr1\t100\t60\t*\t*\t*\t*\t*\t*\n"
            b"r1\t0\tchr1\t200\t60\t*\t*\t*\t*\t*\t*\n"
        )
        fake = _FakeProc(stdout, b"", 141)

        async def fake_shell(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_shell", fake_shell)

        # Act
        ok = await BamIndexProcessor()._records_appear_sorted(tmp_path / "x.bam")

        # Assert
        assert ok is True

    @pytest.mark.asyncio
    async def test_records_appear_sorted_should_return_false_on_hard_failure(
        self, tmp_path, mocker
    ):
        """Test that exit 2 (non-SIGPIPE failure) reports False.

        Given:
            A mocked shell exiting 2 (a hard failure, not SIGPIPE).
        When:
            ``_records_appear_sorted`` is awaited.
        Then:
            It should return False so an unverifiable BAM is rejected
            rather than waved through.
        """
        # Arrange
        fake = _FakeProc(b"", b"truncated", 2)

        async def fake_shell(*_args, **_kwargs):
            return fake

        mocker.patch.object(asyncio, "create_subprocess_shell", fake_shell)

        # Act
        ok = await BamIndexProcessor()._records_appear_sorted(tmp_path / "x.bam")

        # Assert
        assert ok is False


class TestVerifySortedRecoveryPath:
    @pytest.mark.asyncio
    async def test_verify_sorted_should_accept_records_only_when_hd_missing(
        self, tmp_path, mocker
    ):
        """Test that records-scan recovers BAMs missing @HD.

        Given:
            A BAM whose header lacks an ``@HD`` line but whose records
            scan reports as sorted.
        When:
            ``_verify_sorted`` is awaited.
        Then:
            It should return normally (no raise) — the recovery path
            accepts the BAM based on the record-scan fallback.
        """
        # Arrange
        async def fake_header(_path):
            return "@SQ\tSN:chr1\tLN:1000\n"

        mocker.patch.object(bam_module, "read_bam_header", fake_header)

        async def fake_records_sorted(self, *_a, **_kw):
            return True

        mocker.patch.object(
            BamIndexProcessor, "_records_appear_sorted", new=fake_records_sorted
        )

        # Act
        await BamIndexProcessor()._verify_sorted(tmp_path / "x.bam")

        # Assert — no exception is the success signal
        # (sentinel value to make the assertion explicit)
        assert True
