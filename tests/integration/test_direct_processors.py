"""Direct-call integration tests for the BAM and tabix processors.

These tests run each processor's ``run()`` method directly in the
test's event loop with real ``samtools`` / ``htslib`` on PATH, but
without the Wool dispatch boundary. They complement the full
via-Wool suite in ``test_processor_e2e.py`` by isolating the
processor logic — when a change breaks the e2e tests you can run
these to know whether the regression is in the processor pipeline
itself or in the Wool/cache/executor scaffolding around it.

Inputs are tiny (inline strings) for the BED case and the shared
``make_samples`` fixture set for the VCF + GTF cases; the
deterministic samples carry stable md5s so cache keys are stable
across runs.
"""

from __future__ import annotations

import gzip
import shutil
import subprocess

import pytest

from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.processors import bam as bam_module
from cfdb.workflows.processors import tabix as tabix_module
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.tabix import TabixIntervalProcessor


pytestmark = pytest.mark.integration


class TestBamIndexProcessorDirectCall:
    @pytest.mark.asyncio
    async def test_run_should_produce_bai_for_pre_sorted_bam_with_real_samtools(
        self, tmp_path, mocker
    ):
        """Test the BAM index-only pipeline against real samtools in-process.

        Given:
            A minimal coordinate-sorted BAM written to disk and a
            stubbed downloader that places the file at the expected
            workdir path. cfdb assumes DCC-published BAMs are
            pre-sorted upstream and produces only the BAI.
        When:
            run is awaited with samtools on PATH.
        Then:
            The cache should contain a non-empty BAI under the INDEX
            key, and no DATA artifact should be produced.
        """
        if shutil.which("samtools") is None:
            pytest.skip("samtools not on PATH")

        # Arrange — write a minimal valid pre-sorted BAM via samtools
        # from inline SAM. The SAM header carries SO:coordinate and the
        # single read trivially satisfies coordinate ordering.
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        workdir.mkdir()
        cache_root.mkdir()

        sam_path = tmp_path / "input.sam"
        sam_path.write_text(
            "@HD\tVN:1.6\tSO:coordinate\n"
            "@SQ\tSN:chr1\tLN:100\n"
            "read1\t0\tchr1\t10\t60\t5M\t*\t0\t0\tACGTA\tIIIII\n"
        )
        bam_input = tmp_path / "input.bam"
        subprocess.run(
            ["samtools", "view", "-bS", str(sam_path), "-o", str(bam_input)],
            check=True,
        )

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(bam_input, dest)
            return dest

        mocker.patch.object(bam_module, "download_source", fake_download)

        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "int-test",
            "md5": "d41d8cd98f00b204e9800998ecf8427e",
            "access_url": "https://encode-public.s3.amazonaws.com/x.bam",
            "file_format": {"name": "BAM"},
        }

        # Act — drain the processor's async-generator event stream and
        # extract the artifact mapping from the terminal ``complete`` event.
        events = [
            e async for e in BamIndexProcessor().run(file_meta, workdir, cache_root)
        ]
        complete = next(e for e in reversed(events) if e.get("event") == "complete")
        artifacts = complete["artifacts"]

        # Assert — only the index is produced; data falls through to upstream.
        cache = LocalFsCache(cache_root)
        assert "data" not in artifacts
        index_entry = await cache.head(artifacts["index"])
        assert index_entry is not None and index_entry.size > 0

    @pytest.mark.asyncio
    async def test_run_should_skip_stage_one_when_data_artifact_already_cached_for_sam(
        self, tmp_path, mocker
    ):
        """Test that SAM stage-1 is skipped when the data cache is warm.

        Given:
            An uncompressed SAM on disk, a stubbed downloader, and a
            stage-1 ``data`` cache entry already populated with a
            valid sorted BAM.
        When:
            ``BamIndexProcessor().run`` is awaited.
        Then:
            The event stream carries ``stage_complete`` for both
            artifacts plus ``complete``; the SAM-conversion shell call
            never fires (``run_shell`` patched to record invocations);
            only the stage-2 BAI is freshly written.
        """
        for tool in ("samtools",):
            if shutil.which(tool) is None:
                pytest.skip(f"{tool} not on PATH")

        # Arrange — write a sorted BAM the cache will pre-populate.
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        workdir.mkdir()
        cache_root.mkdir()

        sam_source = tmp_path / "source.sam"
        sam_source.write_text(
            "@HD\tVN:1.6\tSO:coordinate\n"
            "@SQ\tSN:chr1\tLN:100\n"
            "read1\t0\tchr1\t10\t60\t5M\t*\t0\t0\tACGTA\tIIIII\n"
        )
        prebuilt_bam = tmp_path / "prebuilt.bam"
        subprocess.run(
            [
                "samtools",
                "view",
                "-bS",
                str(sam_source),
                "-o",
                str(prebuilt_bam),
            ],
            check=True,
        )

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(sam_source, dest)
            return dest

        mocker.patch.object(bam_module, "download_source", fake_download)

        # Capture invocations of run_shell so the assertion can prove
        # the convert+sort pipeline never ran when the data cache is warm.
        shell_invocations: list[str] = []
        original_run_shell = bam_module.run_shell

        async def recording_run_shell(cmd: str) -> None:
            shell_invocations.append(cmd)
            await original_run_shell(cmd)

        mocker.patch.object(bam_module, "run_shell", recording_run_shell)

        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "int-warm-sam",
            "md5": "098f6bcd4621d373cade4e832627b4f6",
            "access_url": "https://encode-public.s3.amazonaws.com/x.sam",
            "file_format": {"name": "SAM"},
        }

        # Prime the stage-1 cache with the prebuilt BAM so the
        # processor's ``cache.head(data_key)`` short-circuits the
        # convert+sort pipeline.
        cache = LocalFsCache(cache_root)
        from cfdb.workflows import keys as key_utils
        from cfdb.workflows.models import ArtifactKind

        data_key = key_utils.cache_key(
            dcc="encode",
            local_id="int-warm-sam",
            artifact_kind=ArtifactKind.DATA,
            md5="098f6bcd4621d373cade4e832627b4f6",
            processor_version=BamIndexProcessor.processor_version,
        )
        await cache.put(data_key, prebuilt_bam)

        # Act
        events = [
            e async for e in BamIndexProcessor().run(file_meta, workdir, cache_root)
        ]

        # Assert
        kinds = [
            (e.get("event"), e.get("kind"))
            for e in events
            if e.get("event") == "stage_complete"
        ]
        assert (
            "stage_complete",
            ArtifactKind.DATA.value,
        ) in kinds
        assert (
            "stage_complete",
            ArtifactKind.INDEX.value,
        ) in kinds
        assert any(e.get("event") == "complete" for e in events)
        assert shell_invocations == [], (
            "Stage-1 convert+sort pipeline must not run when the data "
            f"cache is warm; got invocations: {shell_invocations!r}"
        )
        index_entry = await cache.head(
            key_utils.cache_key(
                dcc="encode",
                local_id="int-warm-sam",
                artifact_kind=ArtifactKind.INDEX,
                md5="098f6bcd4621d373cade4e832627b4f6",
                processor_version=BamIndexProcessor.processor_version,
            )
        )
        assert index_entry is not None and index_entry.size > 0


class TestTabixIntervalProcessorDirectCall:
    @pytest.mark.asyncio
    async def test_run_should_index_bed_with_real_htslib(
        self, tmp_path, mocker
    ):
        """Test the BED pipeline against real bgzip+tabix in-process.

        Given:
            A small uncompressed BED file and a stubbed downloader.
        When:
            run is awaited with htslib tools on PATH.
        Then:
            The cache should contain a non-empty bgzipped BED and TBI.
        """
        for tool in ("bgzip", "tabix", "sort", "zcat"):
            if shutil.which(tool) is None:
                pytest.skip(f"{tool} not on PATH")

        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        workdir.mkdir()
        cache_root.mkdir()

        bed_input = tmp_path / "input.bed"
        bed_input.write_text(
            "chr1\t100\t200\n"
            "chr1\t300\t400\n"
            "chr2\t50\t150\n"
        )

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(bed_input, dest)
            return dest

        mocker.patch.object(tabix_module, "download_source", fake_download)

        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "int-bed",
            "md5": "5d41402abc4b2a76b9719d911017c592",
            "access_url": "https://encode-public.s3.amazonaws.com/x.bed",
            "file_format": {"name": "BED"},
        }

        # Act
        events = [
            e
            async for e in TabixIntervalProcessor().run(
                file_meta, workdir, cache_root
            )
        ]
        complete = next(e for e in reversed(events) if e.get("event") == "complete")
        artifacts = complete["artifacts"]

        # Assert
        cache = LocalFsCache(cache_root)
        data_entry = await cache.head(artifacts["data"])
        index_entry = await cache.head(artifacts["index"])
        assert data_entry is not None and data_entry.size > 0
        assert index_entry is not None and index_entry.size > 0

    @pytest.mark.asyncio
    async def test_run_should_preserve_vcf_header_block_end_to_end(
        self, tmp_path, mocker, samples
    ):
        """Test that a real VCF pipeline produces a header-preserving output.

        Given:
            The deterministic gzipped VCF from ``make_samples`` (full
            ## header block, unordered data lines), real htslib tools
            on PATH, and a stubbed downloader that copies the fixture
            into the workdir.
        When:
            run is awaited.
        Then:
            The cached artifact's first lines are the ## / #CHROM
            header block followed by position-sorted data lines — the
            exact layout tabix expects.
        """
        for tool in ("bgzip", "tabix", "sort", "zcat"):
            if shutil.which(tool) is None:
                pytest.skip(f"{tool} not on PATH")

        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        workdir.mkdir()
        cache_root.mkdir()

        sample = samples["VCF"]

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(sample.path, dest)
            return dest

        mocker.patch.object(tabix_module, "download_source", fake_download)

        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "int-vcf",
            "md5": sample.md5,
            "access_url": f"https://example.invalid/{sample.path.name}",
            "file_format": {"name": "VCF"},
        }

        # Act
        events = [
            e
            async for e in TabixIntervalProcessor().run(
                file_meta, workdir, cache_root
            )
        ]
        complete = next(e for e in reversed(events) if e.get("event") == "complete")
        artifacts = complete["artifacts"]

        # Assert
        cache = LocalFsCache(cache_root)
        bgz_path = tmp_path / "out.bgz"
        with bgz_path.open("wb") as out:
            async for chunk in cache.get(artifacts["data"]):
                out.write(chunk)

        decoded = gzip.decompress(bgz_path.read_bytes()).decode()
        lines = decoded.splitlines()
        header_lines = [ln for ln in lines if ln.startswith("#")]
        data_lines = [ln for ln in lines if not ln.startswith("#")]
        # Header block is contiguous at the top.
        assert lines[: len(header_lines)] == header_lines
        # Data lines are sorted by (chrom, pos).
        parsed = [(ln.split("\t")[0], int(ln.split("\t")[1])) for ln in data_lines]
        assert parsed == sorted(parsed)

    @pytest.mark.asyncio
    async def test_run_should_branch_on_gtf_input_via_real_gffread(
        self, tmp_path, mocker
    ):
        """Test that GTF inputs run through ``gffread`` and emit GFF3 attrs.

        Given:
            A small inline GTF file and a stubbed downloader; real
            ``gffread``, ``bgzip``, ``tabix``, ``sort`` on PATH.
        When:
            ``TabixIntervalProcessor().run`` is awaited.
        Then:
            The cached ``data`` bgz carries GFF3-style attributes
            (``tag=value``) rather than GTF-style (``tag "value";``);
            the ``index`` artifact is a valid TBI; the terminal event
            is ``complete``.
        """
        for tool in ("bgzip", "tabix", "sort", "zcat", "gffread"):
            if shutil.which(tool) is None:
                pytest.skip(f"{tool} not on PATH")

        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        workdir.mkdir()
        cache_root.mkdir()

        gtf_input = tmp_path / "input.gtf"
        gtf_input.write_text(
            'chr1\tcfdb\texon\t100\t200\t.\t+\t0\t'
            'gene_id "GENE0001"; transcript_id "TX0001";\n'
            'chr1\tcfdb\texon\t300\t400\t.\t+\t0\t'
            'gene_id "GENE0002"; transcript_id "TX0002";\n'
        )

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(gtf_input, dest)
            return dest

        mocker.patch.object(tabix_module, "download_source", fake_download)

        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "int-gtf",
            "md5": "1bc29b36f623ba82aaf6724fd3b16718",
            "access_url": "https://encode-public.s3.amazonaws.com/x.gtf",
            "file_format": {"name": "GTF"},
        }

        # Act
        events = [
            e
            async for e in TabixIntervalProcessor().run(
                file_meta, workdir, cache_root
            )
        ]

        # Assert
        complete = next(
            (e for e in reversed(events) if e.get("event") == "complete"), None
        )
        assert complete is not None, "expected a terminal complete event"
        artifacts = complete["artifacts"]
        cache = LocalFsCache(cache_root)

        bgz_path = tmp_path / "out.bgz"
        with bgz_path.open("wb") as out:
            async for chunk in cache.get(artifacts["data"]):
                out.write(chunk)
        decoded = gzip.decompress(bgz_path.read_bytes()).decode()
        data_lines = [
            ln for ln in decoded.splitlines() if ln and not ln.startswith("#")
        ]
        assert data_lines, "expected at least one data line"
        attrs_col = data_lines[0].split("\t")[-1]
        # GFF3 attributes are key=value pairs joined by ``;`` —
        # GTF attributes look like ``tag "value";``. The conversion
        # MUST produce the former.
        assert "=" in attrs_col
        assert '"' not in attrs_col

        index_entry = await cache.head(artifacts["index"])
        assert index_entry is not None and index_entry.size > 0

    @pytest.mark.asyncio
    async def test_run_should_reject_empty_artifact_when_source_has_only_headers(
        self, tmp_path, mocker
    ):
        """Test that header-only VCFs raise rather than commit empty bgz.

        Given:
            A VCF source containing only ``##`` and ``#CHROM`` header
            lines and a stubbed downloader; real htslib tools on PATH.
        When:
            ``TabixIntervalProcessor().run`` is awaited.
        Then:
            A ``RuntimeError`` carrying the canonical "Refusing to
            commit empty …" message propagates out of the processor;
            no ``data``/``index`` entry lands in the cache so a retry
            against a fixed source can succeed cleanly.
        """
        for tool in ("bgzip", "tabix", "sort", "zcat"):
            if shutil.which(tool) is None:
                pytest.skip(f"{tool} not on PATH")

        # Arrange
        workdir = tmp_path / "work"
        cache_root = tmp_path / "cache"
        workdir.mkdir()
        cache_root.mkdir()

        empty_vcf = tmp_path / "empty.vcf"
        empty_vcf.write_text(
            "##fileformat=VCFv4.2\n"
            "##contig=<ID=chr1,length=1000>\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        )

        async def fake_download(_meta, dest):
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(empty_vcf, dest)
            return dest

        mocker.patch.object(tabix_module, "download_source", fake_download)

        file_meta = {
            "dcc": {"dcc_abbreviation": "encode"},
            "local_id": "int-vcf-empty",
            "md5": "c4ca4238a0b923820dcc509a6f75849b",
            "access_url": "https://encode-public.s3.amazonaws.com/x.vcf",
            "file_format": {"name": "VCF"},
        }

        # Act & assert
        with pytest.raises(RuntimeError, match="Refusing to commit empty"):
            async for _ in TabixIntervalProcessor().run(
                file_meta, workdir, cache_root
            ):
                pass

        # Cache MUST stay empty so a fixed-source retry can run cleanly.
        cache = LocalFsCache(cache_root)
        from cfdb.workflows import keys as key_utils
        from cfdb.workflows.models import ArtifactKind

        data_key = key_utils.cache_key(
            dcc="encode",
            local_id="int-vcf-empty",
            artifact_kind=ArtifactKind.DATA,
            md5="c4ca4238a0b923820dcc509a6f75849b",
            processor_version=TabixIntervalProcessor.processor_version,
        )
        index_key = key_utils.cache_key(
            dcc="encode",
            local_id="int-vcf-empty",
            artifact_kind=ArtifactKind.INDEX,
            md5="c4ca4238a0b923820dcc509a6f75849b",
            processor_version=TabixIntervalProcessor.processor_version,
        )
        assert await cache.head(data_key) is None
        assert await cache.head(index_key) is None
