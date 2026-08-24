"""Tests for the processor registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry, default_registry
from cfdb.workflows.processors.tabix import TabixIntervalProcessor


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


#: A near-twin of ``_BamOnly``. It exists because the duplicate-identity
#: guard rejects two instances of one class, so the "first registration
#: wins" contract needs two *distinct* classes claiming BAM to be
#: exercised at all.
class _BamAlternate(Processor):
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


#: Two distinct classes pinning one identity — the collision the
#: class-name default cannot catch, and the one the registry must.
class _PinnedIncumbent(Processor):
    processor_id = "pinned-identity"
    processor_version = 0
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.INDEX,)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> dict[str, str]:
        return {}


class _PinnedNewcomer(Processor):
    processor_id = "pinned-identity"
    processor_version = 0
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.INDEX,)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> dict[str, str]:
        return {}


class _PinnedDisjointFormats(Processor):
    processor_id = "pinned-identity"
    processor_version = 0
    supported_formats = frozenset({"VCF"})
    artifact_kinds = (ArtifactKind.INDEX,)

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


#: Identities differing only in case. ``normalize_processor_id`` accepts
#: both -- the registry is what refuses the pair, because the identity
#: segment becomes a directory name and case-insensitive filesystems fold
#: the two together.
class _CasePinned(Processor):
    processor_id = "BedProcessor"
    processor_version = 0
    supported_formats = frozenset({"BED"})
    artifact_kinds = (ArtifactKind.INDEX,)

    async def run(self, file_meta, workdir, cache_root):
        return {}


class _CaseVariant(Processor):
    processor_id = "BEDProcessor"
    processor_version = 0
    supported_formats = frozenset({"BED"})
    artifact_kinds = (ArtifactKind.INDEX,)

    async def run(self, file_meta, workdir, cache_root):
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
            Two distinct processors both claiming BAM, registered in a
            known order.
        When:
            lookup_for is called with a BAM file.
        Then:
            It should accept both registrations and return the processor
            registered first — the duplicate-identity guard is scoped to
            ``processor_id`` alone, so two distinct processors claiming
            one format remain legal and order-resolved.
        """
        # Arrange
        first = _BamOnly()
        second = _BamAlternate()
        registry = ProcessorRegistry()
        registry.register(first)
        registry.register(second)

        # Act
        result = registry.lookup_for({"file_format": {"name": "BAM"}})

        # Assert
        assert result is first

    def test_register_should_raise_when_two_classes_declare_one_processor_id(self):
        """Test that the registry refuses two processors sharing an identity.

        Given:
            A registry holding a processor, and a processor of a
            different class that declares the same ``processor_id`` — the
            realistic collision, since a new processor copy-pasting a
            pinned identity is the one case the class-name default cannot
            catch.
        When:
            register is called for the second.
        Then:
            It should raise ValueError naming the duplicated identity and
            the incumbent's class, so an operator can find the conflict
            without reading the registry.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_PinnedIncumbent())

        # Act & assert
        with pytest.raises(ValueError, match=r"'pinned-identity'.*_PinnedIncumbent"):
            registry.register(_PinnedNewcomer())

    def test_register_should_raise_when_the_same_processor_class_registers_twice(self):
        """Test that re-registering one processor is rejected too.

        Given:
            A registry already holding a processor, and a second instance
            of that same class.
        When:
            register is called for the second.
        Then:
            It should raise ValueError — registration is not idempotent,
            so double-wiring a registry is caught rather than silently
            duplicating a lookup candidate.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_BamOnly())

        # Act & assert
        with pytest.raises(ValueError, match="processor_id"):
            registry.register(_BamOnly())

    def test_register_should_reject_a_duplicate_identity_across_disjoint_formats(self):
        """Test that disjoint formats are not an escape from the guard.

        Given:
            Two processors sharing an identity but claiming completely
            different ``supported_formats``.
        When:
            register is called for the second.
        Then:
            It should still raise ValueError. Cache keys are scoped by
            identity and artifact kind, not by format, so disjoint
            formats do not prevent the two from aliasing.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_PinnedIncumbent())

        # Act & assert
        with pytest.raises(ValueError, match="processor_id"):
            registry.register(_PinnedDisjointFormats())

    def test_lookup_for_should_resolve_to_the_incumbent_after_a_rejected_register(self):
        """Test that a rejected registration leaves the registry unchanged.

        Given:
            A registry whose second register call raised on a duplicate
            identity.
        When:
            lookup_for is called for a format the rejected processor also
            claimed.
        Then:
            It should return the incumbent, so a failed registration
            leaves no partial state behind for lookup to trip over.
        """
        # Arrange
        registry = ProcessorRegistry()
        incumbent = _PinnedIncumbent()
        registry.register(incumbent)
        with pytest.raises(ValueError):
            registry.register(_PinnedNewcomer())

        # Act
        result = registry.lookup_for({"file_format": {"name": "BAM"}})

        # Assert
        assert result is incumbent


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

    def test_default_registry_should_accept_the_shipped_processor_wiring(self):
        """Test that the production wiring survives the duplicate guard.

        Given:
            A default registry and the two processors the API registers
            onto it during startup.
        When:
            Both are registered, mirroring the application lifespan.
        Then:
            It should accept both and resolve each format to its
            processor — proving the three shipped identities are pairwise
            distinct, so shipping a colliding one fails in CI rather than
            crash-looping the API at boot.
        """
        # Arrange
        registry = default_registry()

        # Act
        registry.register(BamIndexProcessor())
        registry.register(TabixIntervalProcessor())

        # Assert
        assert isinstance(
            registry.lookup_for({"file_format": {"name": "BAM"}}), BamIndexProcessor
        )
        assert isinstance(
            registry.lookup_for({"file_format": {"name": "BED"}}),
            TabixIntervalProcessor,
        )
        assert isinstance(
            registry.lookup_for({"file_format": {"name": "bigWig"}}),
            PassthroughProcessor,
        )

    def test_default_registry_should_return_an_independent_registry_each_call(self):
        """Test that each call yields a registry with no shared state.

        Given:
            Two independent calls to default_registry.
        When:
            A processor is registered onto the first only.
        Then:
            The second should still accept its own instance of that
            processor — this is why a lifespan that runs twice (a reload,
            a worker restart) cannot trip the duplicate guard at startup.
        """
        # Arrange
        first = default_registry()
        first.register(BamIndexProcessor())

        # Act
        second = default_registry()
        second.register(BamIndexProcessor())

        # Assert
        assert isinstance(
            second.lookup_for({"file_format": {"name": "BAM"}}), BamIndexProcessor
        )


class TestIdentityFolding:
    def test_register_should_raise_when_two_identities_differ_only_in_case(self):
        """Test that case-variant identities are refused as a collision.

        Given:
            A registry holding a processor, and one whose identity
            differs from it only in case.
        When:
            register is called for the second.
        Then:
            It should raise ValueError. cache_key preserves case, so the
            two derive distinct keys — but the identity segment is a
            directory name, and a case-insensitive filesystem (APFS by
            default) folds them onto one directory, so the second
            processor reads back the first's artifacts as cache hits.
        """
        # Arrange
        registry = ProcessorRegistry()
        registry.register(_CasePinned())

        # Act & assert
        with pytest.raises(ValueError, match="folded"):
            registry.register(_CaseVariant())
