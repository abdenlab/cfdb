"""Tests for the Processor abstract base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cfdb.workflows import keys as key_utils
from cfdb.workflows.events import Complete
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.base import Processor
from tests.test_workflows import FIXTURE_MD5


class _ConcreteProcessor(Processor):
    """Minimal concrete subclass for exercising the ABC defaults."""

    processor_version = 1
    supported_formats = frozenset({"BAM"})
    artifact_kinds = (ArtifactKind.DATA, ArtifactKind.INDEX)

    async def run(
        self,
        file_meta: dict[str, Any],
        workdir: Path,
        cache_root: Path,
    ) -> dict[str, str]:
        return {}


class TestProcessor:
    def test_needs_processing_should_return_true_when_format_supported(self):
        """Test that needs_processing accepts files whose format is supported.

        Given:
            A Processor subclass with BAM in supported_formats and a file
            whose file_format.name is "BAM".
        When:
            needs_processing is invoked.
        Then:
            It should return True so the router dispatches a workflow.
        """
        # Arrange
        proc = _ConcreteProcessor()
        meta = {"file_format": {"name": "BAM"}}

        # Act & assert
        assert proc.needs_processing(meta) is True

    def test_needs_processing_should_return_false_when_format_unsupported(self):
        """Test that needs_processing rejects unsupported formats.

        Given:
            A Processor subclass without VCF in supported_formats and a
            file whose file_format.name is "VCF".
        When:
            needs_processing is invoked.
        Then:
            It should return False so the registry can try the next
            processor.
        """
        # Arrange
        proc = _ConcreteProcessor()
        meta = {"file_format": {"name": "VCF"}}

        # Act & assert
        assert proc.needs_processing(meta) is False

    def test_needs_processing_should_return_false_when_file_format_missing(self):
        """Test that needs_processing tolerates missing metadata gracefully.

        Given:
            A metadata dict with no file_format field.
        When:
            needs_processing is invoked.
        Then:
            It should return False rather than raising, since some DCC
            rows do arrive without a mapped format.
        """
        # Arrange
        proc = _ConcreteProcessor()

        # Act & assert
        assert proc.needs_processing({}) is False

    def test_artifact_kinds_produced_should_expose_class_ordered_tuple(self):
        """Test that artifact_kinds_produced reflects declaration order.

        Given:
            A subclass declaring (DATA, INDEX) as its artifact_kinds.
        When:
            artifact_kinds_produced is invoked on an instance.
        Then:
            It should return the same ordered tuple so downstream callers
            preserve the stage ordering.
        """
        # Act
        kinds = _ConcreteProcessor().artifact_kinds_produced()

        # Assert
        assert kinds == (ArtifactKind.DATA, ArtifactKind.INDEX)

    def test_artifact_kinds_produced_should_return_class_tuple_when_file_meta_none(
        self,
    ):
        """Test that the default ``artifact_kinds_produced`` ignores file_meta.

        Given:
            A subclass declaring ``(INDEX,)`` as its ``artifact_kinds``.
        When:
            ``artifact_kinds_produced(None)`` is called.
        Then:
            It should return the class-level ``artifact_kinds`` tuple
            unchanged — the default ignores its argument.
        """

        # Arrange
        class _IndexOnly(Processor):
            processor_version = 1
            supported_formats = frozenset({"BAM"})
            artifact_kinds = (ArtifactKind.INDEX,)

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        # Act
        kinds = _IndexOnly().artifact_kinds_produced(None)

        # Assert
        assert kinds == (ArtifactKind.INDEX,)

    def test_processor_id_should_default_to_class_name_when_undeclared(self):
        """Test that a subclass without an explicit id gets its class name.

        Given:
            A Processor subclass that declares no ``processor_id``.
        When:
            The attribute is read off the class.
        Then:
            It should be the class's own name, so a processor author who
            forgets still derives keys distinct from every other
            processor's rather than silently aliasing them.
        """
        # Act & assert
        assert _ConcreteProcessor.processor_id == "_ConcreteProcessor"

    def test_processor_id_should_preserve_an_explicitly_declared_value(self):
        """Test that a declared processor_id survives the default.

        Given:
            A Processor subclass declaring ``processor_id`` in its body.
        When:
            The attribute is read off the class.
        Then:
            It should keep the declared value, so the identity — and
            therefore every artifact keyed under it — survives a rename
            of the class.
        """

        # Arrange
        class _Pinned(Processor):
            processor_id = "pinned-identity"

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        # Act & assert
        assert _Pinned.processor_id == "pinned-identity"

    def test_processor_id_should_default_to_class_name_when_declared_empty(self):
        """Test that a blank declaration is treated as no declaration.

        Given:
            A Processor subclass declaring ``processor_id = ""``.
        When:
            The attribute is read off the class.
        Then:
            It should be the class's own name — an empty identity is the
            same failure as a missing one, so it takes the same safe
            default rather than collapsing the key segment.
        """

        # Arrange
        class _Blank(Processor):
            processor_id = ""

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        # Act & assert
        assert _Blank.processor_id == "_Blank"

    def test_processor_id_should_raise_when_declared_whitespace_only(self):
        """Test that a malformed identity fails when the class is written.

        Given:
            A Processor subclass declaring a whitespace-only
            ``processor_id`` — truthy, so it is not treated as absent.
        When:
            The class is declared.
        Then:
            It should raise ValueError at definition time, so the mistake
            surfaces on import rather than per-request inside a worker,
            far from the class that caused it.
        """
        # Act & assert
        with pytest.raises(ValueError, match="processor_id"):

            class _Blank(Processor):
                processor_id = "   "

                async def run(self, file_meta, workdir, cache):
                    yield Complete(artifacts={})

    def test_processor_id_should_raise_when_declared_id_collides_with_artifact_kind(
        self,
    ):
        """Test that a processor cannot take an artifact kind as its name.

        Given:
            A Processor subclass declaring ``processor_id = "index"``.
        When:
            The class is declared.
        Then:
            It should raise ValueError. Such an identity would let an
            over-specified purge prefix reduce this processor's live keys
            to something indistinguishable from a retired one.
        """
        # Act & assert
        with pytest.raises(ValueError, match="artifact kind"):

            class _KindNamed(Processor):
                processor_id = ArtifactKind.INDEX.value

                async def run(self, file_meta, workdir, cache):
                    yield Complete(artifacts={})

    def test_processor_id_should_be_distinct_at_every_level_of_a_hierarchy(self):
        """Test that no two levels of an inheritance chain share an identity.

        Given:
            A three-level chain of Processor subclasses, none declaring
            an identity.
        When:
            Each class's ``processor_id`` is read.
        Then:
            Each should carry its own class name, so a deep hierarchy
            cannot alias any two of its members' caches.
        """

        # Arrange
        class _Level1(Processor):
            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        class _Level2(_Level1):
            pass

        class _Level3(_Level2):
            pass

        # Act
        ids = {_Level1.processor_id, _Level2.processor_id, _Level3.processor_id}

        # Assert
        assert ids == {"_Level1", "_Level2", "_Level3"}

    def test_processor_id_should_ignore_a_value_supplied_by_a_mixin(self):
        """Test that only the class's own body can declare an identity.

        Given:
            A plain mixin declaring ``processor_id``, and a Processor
            subclass inheriting from that mixin.
        When:
            The subclass's attribute is read.
        Then:
            It should be the subclass's class name, not the mixin's
            value — the default is applied from the class's own
            ``__dict__`` rather than the MRO, so factoring a pinned
            identity into a mixin silently loses it.
        """

        # Arrange
        class _IdentityMixin:
            processor_id = "from-mixin"

        class _Mixed(_IdentityMixin, Processor):
            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        # Act & assert
        assert _Mixed.processor_id == "_Mixed"

    def test_processor_id_should_not_be_inherited_by_a_subclass(self):
        """Test that subclassing a pinned processor mints a fresh identity.

        Given:
            A subclass of a processor that declares its own
            ``processor_id``.
        When:
            The subclass's attribute is read.
        Then:
            It should be the subclass's class name rather than the
            inherited value — the subclass may emit different bytes at
            the same processor_version, which is exactly the aliasing the
            identity prevents.
        """

        # Arrange
        class _Base(Processor):
            processor_id = "base-identity"

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        class _Derived(_Base):
            pass

        # Act & assert
        assert _Derived.processor_id == "_Derived"

    def test_cache_key_for_should_match_keys_module_with_processor_identity(self):
        """Test that cache_key_for is the canonical key the router probes.

        Given:
            A processor with processor_version 1 and a complete file_meta.
        When:
            cache_key_for is called for the DATA artifact.
        Then:
            It should equal the key keys.cache_key derives with the same
            identity, processor id, and version — so the router probe,
            the processor put, and the StageComplete event all agree.
        """
        # Arrange
        meta = {
            "dcc": {"dcc_abbreviation": "ENCODE"},
            "local_id": "ENCFF1",
            "md5": FIXTURE_MD5,
        }

        # Act
        key = _ConcreteProcessor().cache_key_for(meta, ArtifactKind.DATA)

        # Assert
        assert key == key_utils.cache_key(
            dcc="ENCODE",
            local_id="ENCFF1",
            artifact_kind=ArtifactKind.DATA,
            md5=FIXTURE_MD5,
            processor_id="_ConcreteProcessor",
            processor_version=1,
        )

    def test_cache_key_for_should_differ_between_processors_at_equal_version(self):
        """Test that two processors never derive the same artifact key.

        Given:
            Two Processor subclasses at the same ``processor_version``,
            both asked for the INDEX artifact of the same file — the
            shape that would let one serve the other's artifact as a
            cache hit.
        When:
            cache_key_for is called on each.
        Then:
            It should return distinct keys, so the collision is
            impossible rather than merely absent from today's registry.
        """

        # Arrange
        class _FirstProcessor(Processor):
            processor_version = 2

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        class _SecondProcessor(Processor):
            processor_version = 2

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        meta = {
            "dcc": {"dcc_abbreviation": "ENCODE"},
            "local_id": "ENCFF1",
            "md5": FIXTURE_MD5,
        }

        # Act
        first = _FirstProcessor().cache_key_for(meta, ArtifactKind.INDEX)
        second = _SecondProcessor().cache_key_for(meta, ArtifactKind.INDEX)

        # Assert
        assert first != second

    def test_cache_key_for_should_place_the_identity_in_its_own_segment(self):
        """Test that the identity actually occupies the identity slot.

        Given:
            A processor whose identity was defaulted to its class name
            and a complete file_meta.
        When:
            cache_key_for is called for the DATA artifact and the result
            is split on "/".
        Then:
            The fourth segment should be the processor's identity, so the
            key shape the purge sweep reasons about is the one the
            processor actually writes under.
        """
        # Arrange
        meta = {
            "dcc": {"dcc_abbreviation": "ENCODE"},
            "local_id": "ENCFF1",
            "md5": FIXTURE_MD5,
        }

        # Act
        segments = _ConcreteProcessor().cache_key_for(meta, ArtifactKind.DATA).split("/")

        # Assert
        assert segments[3] == _ConcreteProcessor.processor_id

    def test_cache_key_for_should_alias_when_two_processors_declare_one_identity(self):
        """Test the honest boundary of the base class's separation guarantee.

        Given:
            Two Processor subclasses that both declare the *same*
            explicit identity, for the same file and artifact kind.
        When:
            cache_key_for is called on each.
        Then:
            It should return equal keys. The class-name default is a
            safety net for processors that declare nothing, not a
            guarantee — enforcement of uniqueness lives in
            ``ProcessorRegistry.register``, and this pins where the
            responsibility actually sits.
        """

        # Arrange
        class _FirstDeclared(Processor):
            processor_id = "shared-identity"

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        class _SecondDeclared(Processor):
            processor_id = "shared-identity"

            async def run(self, file_meta, workdir, cache):
                yield Complete(artifacts={})

        meta = {
            "dcc": {"dcc_abbreviation": "ENCODE"},
            "local_id": "ENCFF1",
            "md5": FIXTURE_MD5,
        }

        # Act
        first = _FirstDeclared().cache_key_for(meta, ArtifactKind.INDEX)
        second = _SecondDeclared().cache_key_for(meta, ArtifactKind.INDEX)

        # Assert
        assert first == second

    def test_cache_key_for_should_raise_when_file_meta_incomplete(self):
        """Test that cache_key_for surfaces incomplete metadata loudly.

        Given:
            A file_meta missing local_id and md5.
        When:
            cache_key_for is called.
        Then:
            It should raise ValueError (via extract_identity) so the
            router treats the file as workflow-not-applicable.
        """
        # Act & assert
        with pytest.raises(ValueError):
            _ConcreteProcessor().cache_key_for(
                {"dcc": {"dcc_abbreviation": "ENCODE"}}, ArtifactKind.DATA
            )

    def test_processor_should_be_abstract_and_reject_direct_instantiation(self):
        """Test that ``Processor`` cannot be instantiated directly.

        Given:
            A direct attempt to construct the abstract base class
            without overriding ``run``.
        When:
            ``Processor()`` is invoked.
        Then:
            It should raise ``TypeError`` because the ABC's abstract
            method enforcement blocks instantiation.
        """
        # Act & assert
        with pytest.raises(TypeError):
            Processor()  # type: ignore[abstract]
