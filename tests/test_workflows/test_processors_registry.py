"""Tests for the processor registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry, default_registry


class _BamOnly(Processor):
    processor_version = 0
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> dict[str, str]:
        return {}


class _TabixOnly(Processor):
    processor_version = 0
    supported_formats = frozenset({"VCF", "BED"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> dict[str, str]:
        return {}


class TestProcessorRegistry:
    def test_lookup_for_should_return_matching_processor(self):
        """Test that lookup_for picks the processor claiming the file format.

        Given:
            A registry with two processors (BAM-only, VCF/BED-only).
        When:
            lookup_for is called with a VCF file.
        Then:
            It should return the tabix processor because VCF is in its
            supported_formats.
        """
        # Arrange
        registry = ProcessorRegistry()
        bam = _BamOnly()
        tabix = _TabixOnly()
        registry.register(bam)
        registry.register(tabix)

        # Act
        result = registry.lookup_for({"file_format": {"name": "VCF"}})

        # Assert
        assert result is tabix

    def test_lookup_for_should_return_none_when_no_processor_matches(self):
        """Test that lookup_for returns None for unknown formats.

        Given:
            A registry with a BAM-only processor.
        When:
            lookup_for is called with a FASTA file.
        Then:
            It should return None since no registered processor supports
            FASTA (HiGlass territory, out of scope).
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_BamOnly())

        # Act
        result = registry.lookup_for({"file_format": {"name": "FASTA"}})

        # Assert
        assert result is None

    def test_lookup_for_should_return_none_when_format_missing(self):
        """Test that lookup_for handles metadata missing file_format.

        Given:
            A registry and a metadata dict without file_format.
        When:
            lookup_for is called.
        Then:
            It should return None rather than raise.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_BamOnly())

        # Act & assert
        assert registry.lookup_for({}) is None

    def test_lookup_for_should_honor_registration_order_when_duplicate_support(self):
        """Test that the first registered processor wins when both match.

        Given:
            Two processors both claiming BAM, registered in a known order.
        When:
            lookup_for is called with a BAM file.
        Then:
            It should return the processor registered first, matching the
            documented "first match wins" contract.
        """
        # Arrange
        first = _BamOnly()
        second = _BamOnly()
        registry = ProcessorRegistry()
        registry.register(first)
        registry.register(second)

        # Act
        result = registry.lookup_for({"file_format": {"name": "BAM"}})

        # Assert
        assert result is first


class TestDefaultRegistry:
    def test_default_registry_should_include_passthrough_processor(self):
        """Test that default_registry ships with the passthrough processor.

        Given:
            A freshly-built default registry.
        When:
            lookup_for is called for bigWig.
        Then:
            It should return a PassthroughProcessor so the router treats
            Gosling-native formats as no-op workflows out of the box.
        """
        # Act
        result = default_registry().lookup_for({"file_format": {"name": "bigWig"}})

        # Assert
        assert isinstance(result, PassthroughProcessor)
