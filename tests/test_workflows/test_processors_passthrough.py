"""Tests for the passthrough processor."""

from __future__ import annotations

import pytest

from cfdb.workflows.processors.passthrough import PassthroughProcessor


class TestPassthroughProcessor:
    def test_processor_id_should_be_the_pinned_literal(self):
        """Test that the shipped identity is exactly "passthrough".

        Given:
            The shipped PassthroughProcessor class.
        When:
            processor_id is read off it.
        Then:
            It should be exactly "passthrough". The literal is asserted
            rather than derived because it is a wire constant — every
            cached artifact is keyed under it, so changing the string
            silently invalidates the processor's whole cached corpus.
        """
        # Act & assert
        assert PassthroughProcessor.processor_id == "passthrough"

    def test_needs_processing_should_return_false_for_csv(self):
        """Test that PassthroughProcessor reports no work for CSV inputs.

        Given:
            A metadata dict with file_format.name == "CSV".
        When:
            needs_processing is invoked.
        Then:
            It should return False so the router serves the CSV directly.
        """
        # Arrange
        proc = PassthroughProcessor()
        meta = {"file_format": {"name": "CSV"}}

        # Act & assert
        assert proc.needs_processing(meta) is False

    def test_needs_processing_should_return_false_for_tsv(self):
        """Test that PassthroughProcessor reports no work for TSV inputs.

        Given:
            A metadata dict with file_format.name == "TSV".
        When:
            needs_processing is invoked.
        Then:
            It should return False so the router serves the TSV directly.
        """
        # Arrange
        proc = PassthroughProcessor()
        meta = {"file_format": {"name": "TSV"}}

        # Act & assert
        assert proc.needs_processing(meta) is False

    def test_needs_processing_should_return_false_for_bigwig(self):
        """Test that PassthroughProcessor reports no work for bigWig inputs.

        Given:
            A metadata dict with file_format.name == "bigWig".
        When:
            needs_processing is invoked.
        Then:
            It should return False because bigWig is self-indexed.
        """
        # Arrange
        proc = PassthroughProcessor()
        meta = {"file_format": {"name": "bigWig"}}

        # Act & assert
        assert proc.needs_processing(meta) is False

    @pytest.mark.asyncio
    async def test_run_should_raise_runtime_error_when_invoked(self, tmp_path):
        """Test that PassthroughProcessor.run never participates in a workflow.

        Given:
            A PassthroughProcessor instance.
        When:
            run is awaited despite needs_processing returning False.
        Then:
            It should raise RuntimeError so that a misrouted dispatch
            surfaces loudly rather than silently producing no artifacts.
        """
        # Arrange
        proc = PassthroughProcessor()

        # Act & assert
        with pytest.raises(RuntimeError, match="must not be called"):
            async for _event in proc.run(
                {"file_format": {"name": "CSV"}}, tmp_path, tmp_path
            ):
                pass
