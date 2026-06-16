"""Unit tests for workflow and cache key derivation."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.workflows.keys import (
    cache_key,
    normalize_dcc,
    normalize_local_id,
    normalize_md5,
    workflow_key,
)
from cfdb.workflows.models import ArtifactKind
from tests.test_workflows import FIXTURE_MD5

#: Mixed-case variant used to exercise normalization round-trips.
_FIXTURE_MD5_UPPER = FIXTURE_MD5.upper()


def test_normalize_dcc_should_strip_whitespace_and_lowercase():
    """Test that normalize_dcc canonicalizes its input.

    Given:
        A DCC abbreviation with mixed case and surrounding whitespace.
    When:
        normalize_dcc is called.
    Then:
        It should return the trimmed, lowercased form so the value
        embedded in workflow_key matches the value persisted on
        JobRecord.dcc.
    """
    # Act & assert
    assert normalize_dcc("  ENCODE  ") == "encode"
    assert normalize_dcc("4DN_DCIC") == "4dn_dcic"


def test_normalize_md5_should_strip_whitespace_and_lowercase():
    """Test that normalize_md5 canonicalizes hex digests.

    Given:
        A 32-char md5 string with mixed-case hex and whitespace.
    When:
        normalize_md5 is called.
    Then:
        It should return the trimmed, lowercased canonical form.
    """
    # Act & assert
    assert normalize_md5(f"  {_FIXTURE_MD5_UPPER}  ") == FIXTURE_MD5


def test_normalize_md5_should_raise_when_empty():
    """Test that normalize_md5 rejects empty inputs.

    Given:
        An empty md5 string.
    When:
        normalize_md5 is called.
    Then:
        It should raise ValueError because md5 is load-bearing for
        cache-key derivation.
    """
    # Act & assert
    with pytest.raises(ValueError, match="md5"):
        normalize_md5("")


def test_normalize_md5_should_raise_when_wrong_length():
    """Test that normalize_md5 rejects digests that aren't 32 hex chars.

    Given:
        An 8-character hex string (the historical "deadbeef" fixture).
    When:
        normalize_md5 is called.
    Then:
        It should raise ValueError so callers cannot accidentally pass a
        truncated or sentinel md5 that would alias multiple inputs to
        the same content-addressed cache slot.
    """
    # Act & assert
    with pytest.raises(ValueError, match="md5"):
        normalize_md5("deadbeef")


class TestNormalizeLocalId:
    def test_normalize_local_id_should_strip_whitespace_and_preserve_case(self):
        """Test that normalize_local_id trims whitespace but preserves case.

        Given:
            A local_id with surrounding whitespace and mixed case.
        When:
            normalize_local_id is called.
        Then:
            It should return the trimmed value with case preserved —
            upstream DCCs treat local_ids as opaque accessions.
        """
        # Act
        result = normalize_local_id("  ENCFF123abc  ")

        # Assert
        assert result == "ENCFF123abc"

    def test_normalize_local_id_should_raise_when_local_id_contains_forward_slash(self):
        """Test that forward slashes are rejected as a path-traversal guard.

        Given:
            A local_id containing ``/``.
        When:
            normalize_local_id is called.
        Then:
            It should raise ValueError matching "forbidden chars" so the
            value cannot smuggle a directory segment into cache paths.
        """
        # Act & assert
        with pytest.raises(ValueError, match="forbidden chars"):
            normalize_local_id("ENCFF/oops")

    def test_normalize_local_id_should_raise_when_local_id_contains_backslash(self):
        """Test that backslashes are rejected.

        Given:
            A local_id containing ``\\``.
        When:
            normalize_local_id is called.
        Then:
            It should raise ValueError so Windows-style path separators
            cannot escape into cache paths.
        """
        # Act & assert
        with pytest.raises(ValueError):
            normalize_local_id("ENCFF\\oops")

    def test_normalize_local_id_should_raise_when_local_id_contains_null_byte(self):
        """Test that null bytes are rejected.

        Given:
            A local_id containing ``\\x00``.
        When:
            normalize_local_id is called.
        Then:
            It should raise ValueError so an embedded null cannot be
            smuggled into a shell pipeline argument.
        """
        # Act & assert
        with pytest.raises(ValueError):
            normalize_local_id("ENCFF\x00oops")

    def test_normalize_local_id_should_raise_when_empty(self):
        """Test that empty input is rejected.

        Given:
            An empty string.
        When:
            normalize_local_id is called.
        Then:
            It should raise ValueError because local_id is required for
            workflow/cache key derivation.
        """
        # Act & assert
        with pytest.raises(ValueError, match="local_id"):
            normalize_local_id("")


class TestWorkflowKey:
    def test_workflow_key_should_return_normalized_slash_joined_key(self):
        """Test that workflow_key returns the documented segment layout.

        Given:
            Valid dcc, local_id, md5, and pipeline_version inputs.
        When:
            workflow_key is called.
        Then:
            It should return ``{dcc}/{local_id}/{md5}/v{pipeline_version}``
            with dcc and md5 normalized to lowercase.
        """
        # Act
        key = workflow_key("ENCODE", "ENCFF123ABC", _FIXTURE_MD5_UPPER, 1)

        # Assert
        assert key == f"encode/ENCFF123ABC/{FIXTURE_MD5}/v1"

    def test_workflow_key_should_be_stable_across_dcc_case_variants(self):
        """Test that workflow_key ignores case differences in dcc input.

        Given:
            Two calls whose only difference is the casing of the ``dcc`` argument.
        When:
            workflow_key is called for both.
        Then:
            It should produce identical keys so that mutex lookups converge.
        """
        # Act
        a = workflow_key("encode", "x", FIXTURE_MD5, 1)
        b = workflow_key("ENCODE", "x", FIXTURE_MD5, 1)

        # Assert
        assert a == b

    def test_workflow_key_should_be_stable_across_md5_case_variants(self):
        """Test that workflow_key ignores case differences in md5 input.

        Given:
            Two calls whose only difference is the casing of the ``md5`` argument.
        When:
            workflow_key is called for both.
        Then:
            It should produce identical keys.
        """
        # Act
        a = workflow_key("4dn", "x", _FIXTURE_MD5_UPPER, 0)
        b = workflow_key("4dn", "x", FIXTURE_MD5, 0)

        # Assert
        assert a == b

    def test_workflow_key_should_raise_when_md5_empty(self):
        """Test that workflow_key rejects an empty md5.

        Given:
            An empty string supplied as ``md5``.
        When:
            workflow_key is called.
        Then:
            It should raise ValueError, because md5 is load-bearing for
            content-addressed cache keys.
        """
        # Act & assert
        with pytest.raises(ValueError, match="md5"):
            workflow_key("encode", "x", "", 0)

    def test_workflow_key_should_raise_when_local_id_empty(self):
        """Test that workflow_key rejects an empty local_id.

        Given:
            An empty string supplied as ``local_id``.
        When:
            workflow_key is called.
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="local_id"):
            workflow_key("encode", "", FIXTURE_MD5, 0)

    def test_workflow_key_should_raise_when_pipeline_version_negative(self):
        """Test that workflow_key rejects a negative pipeline version.

        Given:
            A negative integer supplied as ``pipeline_version``.
        When:
            workflow_key is called.
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="pipeline_version"):
            workflow_key("encode", "x", FIXTURE_MD5, -1)

    @settings(max_examples=50)
    @given(
        dcc=st.text(
            alphabet=st.characters(whitelist_categories=("L", "N")),
            min_size=1,
            max_size=8,
        ),
        local_id=st.text(
            alphabet=st.characters(whitelist_categories=("L", "N")),
            min_size=1,
            max_size=32,
        ),
        # Hypothesis-generated 32-char hex md5 — exercises normalization
        # across the entire valid input space rather than a single
        # constant.
        md5=st.text(alphabet="abcdef0123456789", min_size=32, max_size=32),
        version=st.integers(min_value=0, max_value=9_999),
    )
    def test_workflow_key_should_be_deterministic(self, dcc, local_id, md5, version):
        """Test that workflow_key is a pure function of its inputs.

        Given:
            Any valid combination of dcc, local_id, md5, and pipeline_version.
        When:
            workflow_key is called twice with those same inputs.
        Then:
            It should return identical keys both times.
        """
        # Act
        a = workflow_key(dcc, local_id, md5, version)
        b = workflow_key(dcc, local_id, md5, version)

        # Assert
        assert a == b


class TestCacheKey:
    def test_cache_key_should_return_artifact_scoped_key(self):
        """Test that cache_key places artifact_kind in its own segment.

        Given:
            Valid inputs including an explicit ``ArtifactKind``.
        When:
            cache_key is called.
        Then:
            It should return
            ``{dcc}/{local_id}/{artifact_kind}/{md5}-v{processor_version}``.
        """
        # Act
        key = cache_key(
            "ENCODE", "ENCFF123ABC", ArtifactKind.DATA, _FIXTURE_MD5_UPPER, 2
        )

        # Assert
        assert key == f"encode/ENCFF123ABC/data/{FIXTURE_MD5}-v2"

    def test_cache_key_should_differ_between_artifact_kinds(self):
        """Test that data and index artifact keys are distinct.

        Given:
            Two calls identical except for artifact_kind.
        When:
            cache_key is called for DATA and INDEX.
        Then:
            It should return distinct keys so that the caches for the two
            artifact kinds never alias.
        """
        # Act
        data_key = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, 0)
        index_key = cache_key("encode", "x", ArtifactKind.INDEX, FIXTURE_MD5, 0)

        # Assert
        assert data_key != index_key

    def test_cache_key_should_raise_when_md5_empty(self):
        """Test that cache_key rejects an empty md5.

        Given:
            An empty string supplied as ``md5``.
        When:
            cache_key is called.
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="md5"):
            cache_key("encode", "x", ArtifactKind.DATA, "", 0)

    def test_cache_key_should_raise_when_processor_version_negative(self):
        """Test that cache_key rejects a negative processor version.

        Given:
            A negative integer supplied as ``processor_version``.
        When:
            cache_key is called.
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="processor_version"):
            cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, -1)

    def test_cache_key_should_version_distinctly(self):
        """Test that bumping processor_version yields a distinct key.

        Given:
            Two calls identical except for processor_version.
        When:
            cache_key is called for each.
        Then:
            It should return different keys so that version bumps naturally
            trigger re-processing without purge.
        """
        # Act
        v0 = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, 0)
        v1 = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, 1)

        # Assert
        assert v0 != v1
