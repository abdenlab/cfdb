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

    def test_cache_key_for_should_match_keys_module_with_processor_version(self):
        """Test that cache_key_for is the canonical key the router probes.

        Given:
            A processor with processor_version 1 and a complete file_meta.
        When:
            cache_key_for is called for the DATA artifact.
        Then:
            It should equal the key keys.cache_key derives with the same
            identity and the processor's version — so the router probe,
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
            processor_version=1,
        )

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
