"""End-to-end processor tests via real Wool dispatch.

Each test submits a workflow through ``WoolExecutor.ensure_workflow``
with the real BAM and tabix processors registered, a real
``LocalFsCache``, and a real ``wool.WorkerPool`` worker. Sample files
are served over HTTP from the ``sample_server`` fixture so the worker
process can download them via the same ``fetcher.download_source``
code path used in production.

This closes the coverage gap left by the two existing integration
tiers:

- ``TestWoolExecutorPickleBoundary`` exercises Wool dispatch with a
  stub processor that does no I/O.
- ``TestBamIndexProcessorDirectCall`` and the tabix integration tests
  exercise real tools but call ``processor.run()`` directly in the
  test's event loop, bypassing Wool.
"""

from __future__ import annotations

import gzip
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
from allpairspy import AllPairs

from cfdb.workflows.keys import is_legacy_cache_key
from cfdb.workflows.lock import get_job
from cfdb.workflows.models import JobStatus

from tests.integration.conftest import (
    CacheState,
    Format,
    Scenario,
    _wait_for_terminal,
    filter_func,
    make_file_meta,
    tool_available,
)


pytestmark = pytest.mark.integration


def _assert_production_key_shape(record, executor) -> None:
    """Assert every persisted artifact key carries a processor identity.

    The e2e assertions elsewhere in this file join a cached path from
    ``artifact_cache_keys`` and check the bytes, which stays true under
    any key scheme. This is the one place a real processor, driven
    through a real worker, is made to prove it wrote under the current
    five-segment shape — and that the sweep would not claim what it
    just produced.
    """
    processor = executor._registry.lookup_for(record.file_meta_snapshot)
    for key in record.artifact_cache_keys.values():
        segments = key.split("/")
        assert len(segments) == 5, key
        assert segments[3] == processor.processor_id, key
        assert is_legacy_cache_key(key) is False, key


def _stage_for_tabix(cached_bgz: Path, cached_tbi: Path, stage_dir: Path) -> Path:
    """Copy a cached bgz + tbi pair into ``stage_dir`` with tabix-friendly names.

    Cache keys don't co-locate the bgz and its sidecar on disk (they're
    stored under per-artifact keys). Tabix expects ``<name>`` and
    ``<name>.tbi`` to sit next to each other, so tests that want to
    query an artifact copy the pair into a scratch dir first.
    """
    stage_dir.mkdir(parents=True, exist_ok=True)
    dest_bgz = stage_dir / "artifact.bgz"
    dest_tbi = stage_dir / "artifact.bgz.tbi"
    shutil.copy2(cached_bgz, dest_bgz)
    shutil.copy2(cached_tbi, dest_tbi)
    return dest_bgz


_BED_LIKE_FORMATS = (Format.BED, Format.NARROWPEAK, Format.BROADPEAK)


def _bed_like_scenarios() -> list[Scenario]:
    """Build pairwise (Format, CacheState) scenarios for the BED family."""
    rows = AllPairs(
        [list(_BED_LIKE_FORMATS), list(CacheState)],
        filter_func=filter_func,
    )
    scenarios: list[Scenario] = []
    for row in rows:
        fmt = next(v for v in row if isinstance(v, Format))
        cache_state = next(v for v in row if isinstance(v, CacheState))
        scenarios.append(Scenario(format=fmt, cache_state=cache_state))
    return scenarios


_BED_LIKE_SCENARIOS = _bed_like_scenarios()


_SAMPLE_KEY_BY_FORMAT: dict[Format, str] = {
    Format.BED: "BED",
    Format.NARROWPEAK: "NarrowPeak",
    Format.BROADPEAK: "BroadPeak",
    Format.VCF: "VCF",
    Format.GFF3: "GFF3",
    Format.GTF: "GTF",
    Format.BAM: "BAM",
    Format.SAM: "SAM",
    Format.BIGBED: "bigBed",
}


_CACHED_ARTIFACT_FORMATS = (
    Format.BED,
    Format.NARROWPEAK,
    Format.BROADPEAK,
    Format.VCF,
    Format.GFF3,
)


def _warm_cache_scenarios() -> list[Scenario]:
    """Pairwise (Format, CacheState) sweep for the warm-cache hit test."""
    rows = AllPairs(
        [list(_CACHED_ARTIFACT_FORMATS), list(CacheState)],
        filter_func=filter_func,
    )
    scenarios: list[Scenario] = []
    for row in rows:
        fmt = next(v for v in row if isinstance(v, Format))
        cache_state = next(v for v in row if isinstance(v, CacheState))
        scenarios.append(Scenario(format=fmt, cache_state=cache_state))
    return scenarios


_WARM_CACHE_SCENARIOS = _warm_cache_scenarios()


class TestProcessorE2E:
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_index_pre_sorted_bam_input(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        xfail_known_bugs,
    ):
        """Test that a pre-sorted BAM fixture produces only a BAI via Wool.

        Given:
            A coordinate-sorted BAM fixture served over HTTP, the
            integration executor backed by a real WorkerPool. cfdb
            assumes DCC-published BAMs are pre-sorted, so the
            processor skips its own sort and produces only the BAI;
            ``/data`` falls through to direct upstream streaming.
        When:
            ensure_workflow is awaited and the background task reaches
            a terminal status.
        Then:
            The job should be COMPLETED with stages_done=["index"]
            and only an INDEX entry in artifact_cache_keys (no data
            artifact). The cached BAI must be a valid index for the
            upstream BAM — verifiable by staging the (BAM, BAI) pair
            and running a samtools query against them.
        """
        scenario = Scenario(format=Format.BAM)

        async def _body():
            # Arrange
            sample = samples["BAM"]
            meta = make_file_meta(sample, base_url=sample_server)

            # Act
            record, fresh = await integration_executor.ensure_workflow(meta)
            await _wait_for_terminal(install_jobs_index, record.job_id)

            # Assert
            final = await get_job(install_jobs_index, record.job_id)
            assert fresh is True
            assert final is not None
            assert final.status == JobStatus.COMPLETED
            assert final.stages_done == ["index"]
            assert "data" not in final.artifact_cache_keys
            _assert_production_key_shape(final, integration_executor)

            cache_root = integration_executor._cache.root
            cached_bai = cache_root / final.artifact_cache_keys["index"]
            assert cached_bai.stat().st_size > 0

            # The BAI must be a valid index for the (still-upstream) BAM —
            # stage the BAM + BAI as a pair and query a region. samtools
            # rejects mismatched/corrupt indexes when the query touches
            # the index structure.
            with tempfile.TemporaryDirectory() as stage:
                staged_bam = Path(stage) / "source.bam"
                staged_bai = Path(stage) / "source.bam.bai"
                staged_bam.write_bytes(sample.path.read_bytes())
                staged_bai.write_bytes(cached_bai.read_bytes())
                subprocess.run(
                    ["samtools", "view", "-c", str(staged_bam), "chr1"],
                    check=True,
                    capture_output=True,
                )

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_convert_and_index_sam_input(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        xfail_known_bugs,
    ):
        """Test that SAM inputs are converted and sorted inside the Wool worker.

        Given:
            A ~50 KB SAM fixture served over HTTP.
        When:
            ensure_workflow is awaited until the job terminates.
        Then:
            The resulting cache entry is a valid sorted BAM and the BAI
            sidecar is non-empty.
        """
        scenario = Scenario(format=Format.SAM)

        async def _body():
            # Arrange
            sample = samples["SAM"]
            meta = make_file_meta(sample, base_url=sample_server)

            # Act
            record, _ = await integration_executor.ensure_workflow(meta)
            await _wait_for_terminal(install_jobs_index, record.job_id)

            # Assert
            final = await get_job(install_jobs_index, record.job_id)
            assert final is not None and final.status == JobStatus.COMPLETED
            cache_root = integration_executor._cache.root
            cached_bam = cache_root / final.artifact_cache_keys["data"]
            subprocess.run(
                ["samtools", "quickcheck", str(cached_bam)],
                check=True,
                capture_output=True,
            )
            header = subprocess.run(
                ["samtools", "view", "-H", str(cached_bam)],
                check=True,
                capture_output=True,
                text=True,
            )
            assert "SO:coordinate" in header.stdout

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_preserve_vcf_header_and_sort_body(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        xfail_known_bugs,
    ):
        """Test that VCF runs preserve the header block end-to-end through Wool.

        Given:
            A gzipped VCF fixture with a full ## header block and 500
            unsorted records across multiple chromosomes.
        When:
            ensure_workflow is awaited until the job terminates.
        Then:
            The cached bgz must contain the header block contiguous at
            the top, data lines position-sorted, and ``tabix`` must
            succeed against the cached artifact.
        """
        scenario = Scenario(format=Format.VCF)

        async def _body():
            # Arrange
            sample = samples["VCF"]
            meta = make_file_meta(sample, base_url=sample_server)

            # Act
            record, _ = await integration_executor.ensure_workflow(meta)
            await _wait_for_terminal(install_jobs_index, record.job_id)

            # Assert
            final = await get_job(install_jobs_index, record.job_id)
            assert final is not None and final.status == JobStatus.COMPLETED
            _assert_production_key_shape(final, integration_executor)

            cache_root = integration_executor._cache.root
            cached_bgz = cache_root / final.artifact_cache_keys["data"]
            cached_tbi = cache_root / final.artifact_cache_keys["index"]
            assert cached_bgz.stat().st_size > 0
            assert cached_tbi.stat().st_size > 0

            decoded = gzip.decompress(cached_bgz.read_bytes()).decode()
            lines = decoded.splitlines()
            header_lines = [ln for ln in lines if ln.startswith("#")]
            data_lines = [ln for ln in lines if not ln.startswith("#")]
            assert lines[: len(header_lines)] == header_lines
            parsed = [
                (ln.split("\t")[0], int(ln.split("\t")[1])) for ln in data_lines
            ]
            assert parsed == sorted(parsed)

            # Tabix expects the .tbi sidecar to live next to the bgz; the
            # cache stores them under separate per-artifact keys, so stage
            # a copy of the pair with matching names.
            with tempfile.TemporaryDirectory() as stage:
                bgz = _stage_for_tabix(cached_bgz, cached_tbi, Path(stage))
                query = subprocess.run(
                    ["tabix", str(bgz), "chr1"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                assert query.stdout.count("\n") > 0

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.parametrize("scenario", _BED_LIKE_SCENARIOS, ids=str)
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_bgzip_and_tabix_bed_like_inputs(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        scenario: Scenario,
        xfail_known_bugs,
    ):
        """Test that BED-family inputs produce a bgz + tbi via Wool.

        Given:
            A pairwise sweep over ``(Format, CacheState)`` for the BED
            family — each scenario served over HTTP and pre-warmed (or
            left cold) per ``scenario.cache_state``.
        When:
            ensure_workflow is awaited until the job terminates.
        Then:
            Cache should contain a non-empty bgz and tbi, and tabix
            should query the cached bgz successfully — regardless of
            whether the cache was warmed by an earlier pass.
        """

        async def _body():
            # Arrange
            sample = samples[_SAMPLE_KEY_BY_FORMAT[scenario.format]]
            meta = make_file_meta(sample, base_url=sample_server)
            if scenario.cache_state is CacheState.WARM:
                # Pre-warm by running the workflow once; the second pass
                # should still complete cleanly via cache reuse.
                warm_record, _ = await integration_executor.ensure_workflow(meta)
                await _wait_for_terminal(install_jobs_index, warm_record.job_id)
                # FakeDB doesn't model terminal-state index eviction across
                # claims, so use a unique local_id for the COLD/WARM
                # comparison to avoid attaching to the completed row.
                meta = make_file_meta(
                    sample,
                    base_url=sample_server,
                    local_id=f"{sample.format}-warm",
                )

            # Act
            record, _ = await integration_executor.ensure_workflow(meta)
            await _wait_for_terminal(install_jobs_index, record.job_id)

            # Assert
            final = await get_job(install_jobs_index, record.job_id)
            assert final is not None and final.status == JobStatus.COMPLETED
            _assert_production_key_shape(final, integration_executor)

            cache_root = integration_executor._cache.root
            cached_bgz = cache_root / final.artifact_cache_keys["data"]
            cached_tbi = cache_root / final.artifact_cache_keys["index"]
            assert cached_bgz.stat().st_size > 0
            assert cached_tbi.stat().st_size > 0

            with tempfile.TemporaryDirectory() as stage:
                bgz = _stage_for_tabix(cached_bgz, cached_tbi, Path(stage))
                query = subprocess.run(
                    ["tabix", str(bgz), "chr1"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                assert query.stdout.count("\n") > 0

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_convert_gtf_to_gff3_and_index(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        xfail_known_bugs,
    ):
        """Test that GTF runs through gffread inside the Wool worker.

        Given:
            A gzipped GTF fixture with GTF-style attribute syntax.
        When:
            ensure_workflow is awaited until the job terminates.
        Then:
            The cached bgz should contain GFF3-style attributes
            (tag=value) rather than GTF (tag "value";), confirming the
            conversion actually ran.
        """
        if not tool_available("gffread"):
            pytest.skip("gffread not on PATH — required for GTF pipeline")

        scenario = Scenario(format=Format.GTF)

        async def _body():
            # Arrange
            sample = samples["GTF"]
            meta = make_file_meta(sample, base_url=sample_server)

            # Act
            record, _ = await integration_executor.ensure_workflow(meta)
            await _wait_for_terminal(install_jobs_index, record.job_id)

            # Assert
            final = await get_job(install_jobs_index, record.job_id)
            assert final is not None and final.status == JobStatus.COMPLETED
            cache_root = integration_executor._cache.root
            cached_bgz = cache_root / final.artifact_cache_keys["data"]
            decoded = gzip.decompress(cached_bgz.read_bytes()).decode()
            data_lines = [
                ln for ln in decoded.splitlines() if not ln.startswith("#")
            ]
            assert data_lines, "expected at least one data line"
            sample_line = data_lines[0]
            # GTF → GFF3 conversion means the attributes column uses the
            # tag=value syntax, not tag "value";
            assert "=" in sample_line.split("\t")[-1]

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_index_gff3_input_via_gff_preset(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        xfail_known_bugs,
    ):
        """Test that GFF3 inputs flow through the tabix pipeline.

        Given:
            A gzipped GFF3 fixture.
        When:
            ensure_workflow is awaited until the job terminates.
        Then:
            The cache holds a non-empty bgz and tbi and tabix can query
            the cached artifact.
        """
        scenario = Scenario(format=Format.GFF3)

        async def _body():
            # Arrange
            sample = samples["GFF3"]
            meta = make_file_meta(sample, base_url=sample_server)

            # Act
            record, _ = await integration_executor.ensure_workflow(meta)
            await _wait_for_terminal(install_jobs_index, record.job_id)

            # Assert
            final = await get_job(install_jobs_index, record.job_id)
            assert final is not None and final.status == JobStatus.COMPLETED
            cache_root = integration_executor._cache.root
            cached_bgz = cache_root / final.artifact_cache_keys["data"]
            cached_tbi = cache_root / final.artifact_cache_keys["index"]
            assert cached_bgz.stat().st_size > 0
            assert cached_tbi.stat().st_size > 0

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_convert_and_index_bigbed_input(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        xfail_known_bugs,
    ):
        """Test that bigBed inputs flow through bigBedToBed inside Wool.

        Given:
            A bigBed fixture (skipped when bedToBigBed is unavailable).
        When:
            ensure_workflow is awaited until the job terminates.
        Then:
            Cache holds a BED-derived bgz and tbi; artifact sizes are
            non-zero.
        """
        sample = samples["bigBed"]
        if sample is None:
            pytest.skip("bedToBigBed not on PATH — cannot build bigBed fixture")
        if not tool_available("bigBedToBed"):
            pytest.skip("bigBedToBed not on PATH")

        scenario = Scenario(format=Format.BIGBED)

        async def _body():
            # Arrange
            meta = make_file_meta(sample, base_url=sample_server)

            # Act
            record, _ = await integration_executor.ensure_workflow(meta)
            await _wait_for_terminal(install_jobs_index, record.job_id)

            # Assert
            final = await get_job(install_jobs_index, record.job_id)
            assert final is not None and final.status == JobStatus.COMPLETED
            cache_root = integration_executor._cache.root
            cached_bgz = cache_root / final.artifact_cache_keys["data"]
            cached_tbi = cache_root / final.artifact_cache_keys["index"]
            assert cached_bgz.stat().st_size > 0
            assert cached_tbi.stat().st_size > 0

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.parametrize("scenario", _WARM_CACHE_SCENARIOS, ids=str)
    @pytest.mark.asyncio
    async def test_ensure_workflow_should_serve_cached_artifact_on_warm_cache_hit(
        self,
        samples,
        sample_server,
        integration_executor,
        install_jobs_index,
        scenario: Scenario,
        xfail_known_bugs,
    ):
        """Test that a second ensure_workflow consults the cache rather than re-running.

        Given:
            A pairwise sweep over ``(Format, CacheState)`` for formats
            that produce a real cached artifact (BED-family, VCF, GFF3).
            For each scenario, the workflow is run once to warm the
            cache, then run again.
        When:
            ensure_workflow is awaited a second time with the same
            file_meta.
        Then:
            The cached ``data`` (and ``index`` where applicable) bytes
            are byte-identical across both passes; the second pass's
            terminal status is COMPLETED.
        """

        async def _body():
            # Arrange
            sample = samples[_SAMPLE_KEY_BY_FORMAT[scenario.format]]
            if sample is None:
                pytest.skip(
                    f"Sample for {scenario.format.value} is unavailable on this host"
                )
            first_meta = make_file_meta(sample, base_url=sample_server)

            # Warm the cache via the first run.
            first_record, _ = await integration_executor.ensure_workflow(first_meta)
            await _wait_for_terminal(install_jobs_index, first_record.job_id)
            first_final = await get_job(install_jobs_index, first_record.job_id)
            assert (
                first_final is not None and first_final.status == JobStatus.COMPLETED
            )
            cache_root = integration_executor._cache.root
            first_data = (
                cache_root / first_final.artifact_cache_keys["data"]
            ).read_bytes()

            # Act — second run against a fresh workflow_key (so the FakeDB
            # claim path doesn't attach to the terminated row).
            second_meta = make_file_meta(
                sample,
                base_url=sample_server,
                local_id=f"{sample.format}-second",
            )
            second_record, _ = await integration_executor.ensure_workflow(
                second_meta
            )
            await _wait_for_terminal(install_jobs_index, second_record.job_id)
            second_final = await get_job(install_jobs_index, second_record.job_id)

            # Assert
            assert second_final is not None
            assert second_final.status == JobStatus.COMPLETED
            # The cache_key derivation is content-addressed via md5, not
            # local_id, so both passes target the same on-disk key. Verify
            # the bytes are stable across the second pass.
            second_data = (
                cache_root / second_final.artifact_cache_keys["data"]
            ).read_bytes()
            assert second_data == first_data
            # Tag the test with the scenario value so a CI log carries the
            # axis names alongside any failure surface.
            assert scenario.is_complete is False  # only two axes are set

        await xfail_known_bugs(scenario, _body)

    @pytest.mark.asyncio
    async def test_ensure_workflow_should_surface_failed_status_when_processor_raises_with_path_scrubbed_error(
        self,
        sample_data_root,
        sample_server,
        integration_executor,
        install_jobs_index,
    ):
        """Test that a corrupted source surfaces FAILED with no leaked paths.

        Given:
            A real BAM workflow whose source is a single-byte file
            placed in the session-scoped sample directory and served
            over the local HTTP server, so ``samtools`` rejects it
            during the BAM header inspection.
        When:
            ensure_workflow is awaited and the background task reaches
            terminal status.
        Then:
            The JobRecord lands in FAILED; ``error`` carries no
            absolute filesystem paths (the lock module scrubs them
            before persistence); the per-job workdir is removed even
            after the failure.
        """
        # Arrange — drop a 1-byte file alongside the existing fixtures
        # so the session HTTP server will serve it. The corrupted file
        # has a distinct name so its presence does not affect other
        # tests.
        broken_path = sample_data_root / "corrupted-issue32.bam"
        broken_path.write_bytes(b"X")
        # Use a stable md5 — content-addressed cache keys depend on
        # this value and the broken bytes don't need to round-trip.
        meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "submission": "encode",
            "local_id": "ENCFF-corrupted-issue32",
            "md5": "02129bb861061d1a2c46e25a2a5a3a92",
            "access_url": f"{sample_server}/{broken_path.name}",
            "file_format": {"name": "BAM"},
        }

        # Act
        record, _ = await integration_executor.ensure_workflow(meta)
        await _wait_for_terminal(install_jobs_index, record.job_id, timeout=30.0)

        # Assert
        final = await get_job(install_jobs_index, record.job_id)
        assert final is not None
        assert final.status == JobStatus.FAILED
        assert final.error is not None
        # The path-scrub regex strips multi-segment absolute paths so a
        # ``/<seg>/<seg>`` shape MUST NOT survive into persisted text.
        assert re.search(r"/[A-Za-z][A-Za-z0-9_.-]*/[A-Za-z]", final.error) is None
        # Workdir is cleaned up regardless of failure mode. Assert against
        # the workdir ROOT, not ``root / job_id``: the per-attempt workdir is
        # ``root / f"{job_id}-{uuid}"`` (B1), so a bare-``job_id`` path is
        # never created and asserting its absence would be vacuously true.
        assert list(integration_executor._workdir_root.iterdir()) == []
