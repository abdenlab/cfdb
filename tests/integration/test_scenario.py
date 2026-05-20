"""Unit tests for the Scenario model used by the integration sweeps.

These tests are intentionally fast and synchronous — they exercise the
``Scenario`` dataclass's algebra (``__or__``, ``is_complete``,
``__str__``) without touching the wool pool, the cache, or any real
fixtures. The integration marker is applied for consistency with the
rest of the suite's parametrize/skip logic, but the tests run in
milliseconds.
"""

from __future__ import annotations

import pytest

from tests.integration.conftest import (
    CacheState,
    Concurrency,
    Endpoint,
    Format,
    Method,
    MutexBackend,
    PickleBoundary,
    Scenario,
)


pytestmark = pytest.mark.integration


class TestScenario:
    def test_or_should_merge_disjoint_fields_into_a_single_scenario(self):
        """Test that __or__ merges two partial scenarios into a richer one.

        Given:
            Two ``Scenario`` instances whose set fields do not overlap.
        When:
            They are combined via the ``|`` operator.
        Then:
            The merged scenario carries every set field from both
            operands and remains None on every still-unset field.
        """
        # Arrange
        left = Scenario(format=Format.BAM, endpoint=Endpoint.DATA)
        right = Scenario(method=Method.GET, cache_state=CacheState.WARM)

        # Act
        merged = left | right

        # Assert
        assert merged.format is Format.BAM
        assert merged.endpoint is Endpoint.DATA
        assert merged.method is Method.GET
        assert merged.cache_state is CacheState.WARM
        assert merged.concurrency is None
        assert merged.mutex_backend is None
        assert merged.pickle_boundary is None

    def test_or_should_prefer_non_none_field_on_partial_overlap(self):
        """Test that __or__ keeps a non-None field when the other side is None.

        Given:
            Two scenarios where one sets a field and the other leaves
            the same field as None.
        When:
            They are combined via the ``|`` operator.
        Then:
            The merged scenario carries the non-None value (rather than
            silently overwriting it with None).
        """
        # Arrange
        left = Scenario(format=Format.SAM, endpoint=Endpoint.INDEX)
        right = Scenario(method=Method.GET)  # format/endpoint None

        # Act
        merged = left | right

        # Assert
        assert merged.format is Format.SAM
        assert merged.endpoint is Endpoint.INDEX
        assert merged.method is Method.GET

    def test_or_should_raise_value_error_when_non_none_fields_conflict(self):
        """Test that __or__ refuses to silently overwrite a conflicting field.

        Given:
            Two scenarios that disagree on a single field's value.
        When:
            They are combined via the ``|`` operator.
        Then:
            A ``ValueError`` is raised referencing the conflicting field
            and the test does not silently overwrite the left operand.
        """
        # Arrange
        left = Scenario(format=Format.BAM)
        right = Scenario(format=Format.VCF)

        # Act & assert
        with pytest.raises(ValueError, match="format"):
            _ = left | right

    def test_is_complete_should_return_true_only_when_every_field_is_set(self):
        """Test that is_complete distinguishes partial from full scenarios.

        Given:
            A partial scenario and a scenario covering every dimension.
        When:
            ``is_complete`` is read on each.
        Then:
            Partial returns False; fully-populated returns True.
        """
        # Arrange
        partial = Scenario(format=Format.BAM)
        complete = Scenario(
            format=Format.BAM,
            endpoint=Endpoint.INDEX,
            method=Method.GET,
            cache_state=CacheState.COLD,
            concurrency=Concurrency.N2,
            mutex_backend=MutexBackend.FAKE,
            pickle_boundary=PickleBoundary.WOOL_WORKER,
        )

        # Act & assert
        assert partial.is_complete is False
        assert complete.is_complete is True

    def test_str_should_join_set_field_names_with_dashes_and_skip_none(self):
        """Test that __str__ formats set fields and skips None for pytest IDs.

        Given:
            A scenario with two enum fields set and the rest None.
        When:
            ``str(scenario)`` is invoked.
        Then:
            The returned string carries the set enum names joined by
            dashes; unset fields contribute no token.
        """
        # Arrange
        scenario = Scenario(format=Format.BAM, endpoint=Endpoint.INDEX)

        # Act
        rendered = str(scenario)

        # Assert
        assert rendered == "BAM-INDEX"

    def test_str_should_return_empty_marker_when_no_fields_are_set(self):
        """Test that __str__ produces a stable placeholder for empty scenarios.

        Given:
            A scenario with no fields set.
        When:
            ``str(scenario)`` is invoked.
        Then:
            The returned string is a non-empty placeholder so pytest's
            parametrize ID generator never collapses to an unsafe ``""``.
        """
        # Arrange
        empty = Scenario()

        # Act
        rendered = str(empty)

        # Assert
        assert rendered == "EMPTY"
