"""Unit tests for workflow and cache key derivation."""

from __future__ import annotations

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from cfdb.workflows.keys import (
    cache_key,
    is_legacy_cache_key,
    normalize_dcc,
    normalize_local_id,
    normalize_md5,
    normalize_processor_id,
    workflow_key,
)
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.tabix import TabixIntervalProcessor
from tests.test_workflows import FIXTURE_MD5

#: Mixed-case variant used to exercise normalization round-trips.
_FIXTURE_MD5_UPPER = FIXTURE_MD5.upper()

#: Shared strategies for the cache-key property tests. The alphabets
#: match what ``normalize_dcc`` / ``normalize_local_id`` accept: letters
#: and digits only, so no draw trips a separator guard and turns a
#: property about key *content* into one about key *validity*.
_DCC_STRATEGY = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")), min_size=1, max_size=8
)
_LOCAL_ID_STRATEGY = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")), min_size=1, max_size=32
)
_MD5_STRATEGY = st.text(alphabet="abcdef0123456789", min_size=32, max_size=32)
_ARTIFACT_KIND_STRATEGY = st.sampled_from(list(ArtifactKind))
_VERSION_STRATEGY = st.integers(min_value=0, max_value=9_999)

#: Processor identities drawn from exactly the alphabet
#: ``normalize_processor_id`` admits — ASCII letters, digits, and the
#: ``-``/``_``/``.`` joiners — excluding the values it reserves. Drawing
#: from the Unicode letter category instead would generate identities the
#: normalizer rejects (``"ª"``), turning a property about key *content*
#: into one about key *validity*.
_PROCESSOR_ID_STRATEGY = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.",
    min_size=1,
    max_size=24,
).filter(
    lambda value: value not in (".", "..")
    and value not in {kind.value for kind in ArtifactKind}
)


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


@pytest.mark.parametrize("dcc", [".", ".."])
def test_normalize_dcc_should_raise_when_path_traversal(dcc):
    """Test that a traversal dcc is rejected at derivation.

    Given:
        A dcc of ``.`` or ``..``.
    When:
        normalize_dcc is called.
    Then:
        It should raise ValueError, for the same reason
        normalize_local_id does — the value becomes the leading segment
        of every key derived for that source, and the local backend
        silently collapses ``.`` out of the resulting path.
    """
    # Act & assert
    with pytest.raises(ValueError, match="traversal"):
        normalize_dcc(dcc)


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

    @pytest.mark.parametrize("local_id", [".", ".."])
    def test_normalize_local_id_should_raise_when_path_traversal(self, local_id):
        """Test that a traversal local_id is rejected at derivation.

        Given:
            A local_id of ``.`` or ``..``.
        When:
            normalize_local_id is called.
        Then:
            It should raise ValueError. This is the one key component
            that comes from third-party DCC metadata, and the cache
            backend catches only ``..`` — ``.`` is silently collapsed by
            path resolution, landing one logical key at five segments on
            S3 and four on disk.
        """
        # Act & assert
        with pytest.raises(ValueError, match="traversal"):
            normalize_local_id(local_id)


class TestNormalizeProcessorId:
    def test_normalize_processor_id_should_strip_whitespace_and_preserve_case(self):
        """Test that normalize_processor_id trims but preserves case.

        Given:
            A processor id with surrounding whitespace and mixed case.
        When:
            normalize_processor_id is called.
        Then:
            It should return the trimmed value with case preserved, so
            two class-name defaults differing only in case stay distinct.
        """
        # Act
        result = normalize_processor_id("  TabixIntervalProcessor  ")

        # Assert
        assert result == "TabixIntervalProcessor"

    def test_normalize_processor_id_should_raise_when_processor_id_contains_forward_slash(
        self,
    ):
        """Test that forward slashes are rejected as a key-shape guard.

        Given:
            A processor id containing ``/``.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError matching "forbidden chars" so the
            id cannot silently add segments to the cache key.
        """
        # Act & assert
        with pytest.raises(ValueError, match="forbidden chars"):
            normalize_processor_id("tabix/interval")

    def test_normalize_processor_id_should_raise_when_processor_id_contains_backslash(
        self,
    ):
        """Test that backslashes are rejected.

        Given:
            A processor id containing ``\\``.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError so Windows-style path separators
            cannot escape into cache paths.
        """
        # Act & assert
        with pytest.raises(ValueError, match="forbidden chars"):
            normalize_processor_id("tabix\\interval")

    def test_normalize_processor_id_should_raise_when_processor_id_contains_null_byte(
        self,
    ):
        """Test that null bytes are rejected.

        Given:
            A processor id containing ``\\x00``.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError so an embedded null cannot reach a
            cache path or a shell pipeline argument.
        """
        # Act & assert
        with pytest.raises(ValueError, match="forbidden chars"):
            normalize_processor_id("tabix\x00interval")

    @pytest.mark.parametrize(
        "processor_id",
        [
            pytest.param("tabix\ninterval", id="newline"),
            pytest.param("tabix\x01interval", id="control-char"),
            pytest.param("tabix​interval", id="zero-width-space"),
            pytest.param("tabix‮interval", id="rtl-override"),
            pytest.param("tabix／interval", id="fullwidth-solidus"),
            pytest.param("tabix∕interval", id="division-slash"),
            pytest.param("tabix%2Finterval", id="percent-encoded-slash"),
            pytest.param("tabix interval", id="inner-space"),
        ],
    )
    def test_normalize_processor_id_should_raise_when_outside_the_allowlist(
        self, processor_id
    ):
        """Test that only the documented alphabet reaches a cache key.

        Given:
            An identity carrying a character outside ``[A-Za-z0-9._-]``.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError. A denylist of the separators
            would admit every one of these, and each is invisible in
            review while addressing a different cache entry — a
            zero-width space renders identically to the id beside it.
        """
        # Act & assert
        with pytest.raises(ValueError, match="forbidden chars"):
            normalize_processor_id(processor_id)

    @pytest.mark.parametrize(
        "processor_id",
        [
            pytest.param(7, id="int"),
            pytest.param(b"tabix-interval", id="bytes"),
            pytest.param(["tabix-interval"], id="list"),
        ],
    )
    def test_normalize_processor_id_should_raise_when_not_a_string(self, processor_id):
        """Test that a non-string identity raises ValueError, not AttributeError.

        Given:
            An identity that is not a ``str`` — the shape a ``__slots__``
            member descriptor or a copy-pasted ``processor_version``
            takes.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError naming the type. Both request-path
            callers catch ValueError to fall through to direct upstream
            streaming, so an AttributeError escaping ``.strip()`` would
            surface as a 500 instead of that fall-through.
        """
        # Act & assert
        with pytest.raises(ValueError, match="must be a str"):
            normalize_processor_id(processor_id)

    def test_normalize_processor_id_should_raise_when_empty(self):
        """Test that an empty processor id is rejected.

        Given:
            An empty string.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError, because an absent identity is
            exactly the aliasing the segment exists to prevent.
        """
        # Act & assert
        with pytest.raises(ValueError, match="processor_id"):
            normalize_processor_id("")

    def test_normalize_processor_id_should_raise_when_whitespace_only(self):
        """Test that a whitespace-only processor id is rejected.

        Given:
            A string of spaces.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError — the strip runs before the empty
            check, so blanks cannot collapse the identity segment.
        """
        # Act & assert
        with pytest.raises(ValueError, match="processor_id"):
            normalize_processor_id("   ")

    @pytest.mark.parametrize("traversal", [".", ".."])
    def test_normalize_processor_id_should_raise_when_path_traversal(self, traversal):
        """Test that traversal segments are rejected at derivation.

        Given:
            A processor id of "." or "..", which traverses a path segment
            without containing a separator.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError, so the value fails here rather
            than later and elsewhere, in the cache backend's own key
            validation deep inside a running workflow.
        """
        # Act & assert
        with pytest.raises(ValueError, match="traversal"):
            normalize_processor_id(traversal)

    @pytest.mark.parametrize("kind", [kind.value for kind in ArtifactKind])
    def test_normalize_processor_id_should_raise_when_it_collides_with_artifact_kind(
        self, kind
    ):
        """Test that an identity cannot impersonate an artifact kind.

        Given:
            A processor id equal to an ArtifactKind value.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError. Such an id would place an
            artifact-kind string in the identity segment, letting an
            over-specified purge prefix strip a live key down to
            something is_legacy_cache_key would claim.
        """
        # Act & assert
        with pytest.raises(ValueError, match="artifact kind"):
            normalize_processor_id(kind)

    def test_normalize_processor_id_should_raise_when_padding_hides_a_forbidden_char(
        self,
    ):
        """Test that stripping is not a route around the character check.

        Given:
            A processor id whose forbidden separator is surrounded by
            whitespace.
        When:
            normalize_processor_id is called.
        Then:
            It should raise ValueError — the strip removes padding, not
            the violation inside it.
        """
        # Act & assert
        with pytest.raises(ValueError, match="forbidden chars"):
            normalize_processor_id("  a/b  ")

    def test_normalize_processor_id_should_keep_two_case_variants_distinct(self):
        """Test that case preservation separates two similar identities.

        Given:
            Two processor ids differing only in letter case.
        When:
            normalize_processor_id is called on each.
        Then:
            It should return two different values, so case-folding can
            never merge one processor's cache with another's.
        """
        # Act
        mixed = normalize_processor_id("BedProcessor")
        upper = normalize_processor_id("BEDProcessor")

        # Assert
        assert mixed != upper


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
    def test_cache_key_should_return_artifact_and_processor_scoped_key(self):
        """Test that cache_key gives artifact_kind and processor own segments.

        Given:
            Valid inputs including an explicit ``ArtifactKind`` and a
            processor identity.
        When:
            cache_key is called.
        Then:
            It should return ``{dcc}/{local_id}/{artifact_kind}/
            {processor_id}/{md5}-v{processor_version}``.
        """
        # Act
        key = cache_key(
            "ENCODE",
            "ENCFF123ABC",
            ArtifactKind.DATA,
            _FIXTURE_MD5_UPPER,
            "tabix-interval",
            2,
        )

        # Assert
        assert key == f"encode/ENCFF123ABC/data/tabix-interval/{FIXTURE_MD5}-v2"

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
        data_key = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "p", 0)
        index_key = cache_key("encode", "x", ArtifactKind.INDEX, FIXTURE_MD5, "p", 0)

        # Assert
        assert data_key != index_key

    def test_cache_key_should_differ_between_processors_at_equal_version(self):
        """Test that the processor identity alone separates two processors.

        Given:
            Two calls for the same file and artifact kind at the same
            ``processor_version``, differing only in ``processor_id`` —
            the shape that let ``TabixIntervalProcessor`` and
            ``BamIndexProcessor`` (both at version 2) alias.
        When:
            cache_key is called for each.
        Then:
            It should return distinct keys, so neither processor can read
            back the other's artifacts as a cache hit.
        """
        # Act
        tabix = cache_key(
            "encode", "x", ArtifactKind.INDEX, FIXTURE_MD5, "tabix-interval", 2
        )
        bedpe = cache_key(
            "encode", "x", ArtifactKind.INDEX, FIXTURE_MD5, "bedpe-interval", 2
        )

        # Assert
        assert tabix != bedpe

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
            cache_key("encode", "x", ArtifactKind.DATA, "", "p", 0)

    def test_cache_key_should_raise_when_processor_id_empty(self):
        """Test that cache_key rejects a missing processor identity.

        Given:
            An empty string supplied as ``processor_id``.
        When:
            cache_key is called.
        Then:
            It should raise ValueError rather than mint a key with a
            collapsed identity segment.
        """
        # Act & assert
        with pytest.raises(ValueError, match="processor_id"):
            cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "", 0)

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
            cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "p", -1)

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
        v0 = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "p", 0)
        v1 = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "p", 1)

        # Assert
        assert v0 != v1

    def test_cache_key_should_trim_the_processor_id_segment(self):
        """Test that a padded identity addresses the same artifact.

        Given:
            Two calls identical except that one's processor_id carries
            surrounding whitespace.
        When:
            cache_key is called for each.
        Then:
            It should return identical keys, so a stray space in a
            declared identity cannot fork a processor's cache in two.
        """
        # Act
        padded = cache_key(
            "encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "  tabix-interval  ", 2
        )
        clean = cache_key(
            "encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "tabix-interval", 2
        )

        # Assert
        assert padded == clean

    def test_cache_key_should_raise_when_processor_id_contains_separator(self):
        """Test that a separator in the identity cannot add a segment.

        Given:
            A processor_id containing a forward slash.
        When:
            cache_key is called.
        Then:
            It should raise ValueError rather than emit a six-segment
            key, so the identity can never restructure the key shape.
        """
        # Act & assert
        with pytest.raises(ValueError, match="forbidden chars"):
            cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "tabix/oops", 2)

    def test_cache_key_should_differ_between_case_variant_processor_ids(self):
        """Test that identity case survives the whole derivation path.

        Given:
            Two calls identical except for the case of processor_id.
        When:
            cache_key is called for each.
        Then:
            It should return distinct keys, so two processors whose names
            differ only in case never alias.
        """
        # Act
        mixed = cache_key(
            "encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "BedProcessor", 2
        )
        upper = cache_key(
            "encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "BEDProcessor", 2
        )

        # Assert
        assert mixed != upper

    def test_cache_key_should_not_confuse_the_identity_with_the_version_suffix(self):
        """Test that the identity and version segments stay separable.

        Given:
            Two calls at the same version, one with processor_id "p" and
            the other with "p-v1" — a value that resembles an identity
            with a version already folded into it.
        When:
            cache_key is called for each.
        Then:
            It should return distinct keys, so the identity segment and
            the version suffix cannot be read as one another.
        """
        # Act
        plain = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "p", 1)
        suffixed = cache_key("encode", "x", ArtifactKind.DATA, FIXTURE_MD5, "p-v1", 1)

        # Assert
        assert plain != suffixed

    @settings(max_examples=50)
    @given(
        dcc=_DCC_STRATEGY,
        local_id=_LOCAL_ID_STRATEGY,
        md5=_MD5_STRATEGY,
        artifact_kind=_ARTIFACT_KIND_STRATEGY,
        processor_id=_PROCESSOR_ID_STRATEGY,
        version=_VERSION_STRATEGY,
    )
    def test_cache_key_should_be_deterministic(
        self, dcc, local_id, md5, artifact_kind, processor_id, version
    ):
        """Test that cache_key is a pure function of its inputs.

        Given:
            Any valid combination of dcc, local_id, artifact_kind, md5,
            processor_id, and processor_version.
        When:
            cache_key is called twice with those same inputs.
        Then:
            It should return identical keys of exactly five slash-separated
            segments, pinning the shape as well as the purity.
        """
        # Act
        a = cache_key(dcc, local_id, artifact_kind, md5, processor_id, version)
        b = cache_key(dcc, local_id, artifact_kind, md5, processor_id, version)

        # Assert
        assert a == b
        assert len(a.split("/")) == 5

    @settings(max_examples=50)
    @given(
        dcc=_DCC_STRATEGY,
        local_id=_LOCAL_ID_STRATEGY,
        md5=_MD5_STRATEGY,
        artifact_kind=_ARTIFACT_KIND_STRATEGY,
        first_id=_PROCESSOR_ID_STRATEGY,
        second_id=_PROCESSOR_ID_STRATEGY,
        version=_VERSION_STRATEGY,
    )
    def test_cache_key_should_separate_any_two_distinct_processor_ids(
        self, dcc, local_id, md5, artifact_kind, first_id, second_id, version
    ):
        """Test that the identity segment separates processors universally.

        Given:
            Any two distinct processor identities, with the file, artifact
            kind, and processor version held identical between them.
        When:
            cache_key is called for each.
        Then:
            It should return distinct keys across the whole input domain,
            so the collision this issue closes is impossible rather than
            merely absent from the two ids a single example samples.
        """
        # Arrange
        assume(normalize_processor_id(first_id) != normalize_processor_id(second_id))

        # Act
        first = cache_key(dcc, local_id, artifact_kind, md5, first_id, version)
        second = cache_key(dcc, local_id, artifact_kind, md5, second_id, version)

        # Assert
        assert first != second


class TestIsLegacyCacheKey:
    def test_is_legacy_cache_key_should_return_true_for_retired_shape(self):
        """Test that a pre-#109 key is recognised as legacy.

        Given:
            A four-segment key with no processor identity, of the shape
            the pipeline minted before the identity segment existed.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return True so the purge sweep claims it.
        """
        # Act & assert
        assert is_legacy_cache_key(f"encode/ENCFF732YBO/index/{FIXTURE_MD5}-v2") is True

    def test_is_legacy_cache_key_should_return_false_for_current_shape(self):
        """Test that a key carrying a processor identity is not legacy.

        Given:
            A key derived by the current ``cache_key``.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return False, so a live artifact is never swept.
        """
        # Arrange
        key = cache_key(
            "encode", "ENCFF732YBO", ArtifactKind.INDEX, FIXTURE_MD5, "tabix-interval", 2
        )

        # Act & assert
        assert is_legacy_cache_key(key) is False

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param("encode/ENCFF1/index/notes.txt", id="leaf-not-content-address"),
            pytest.param(f"encode//index/{FIXTURE_MD5}-v2", id="blank-local-id"),
            pytest.param(f"/ENCFF1/index/{FIXTURE_MD5}-v2", id="blank-leading-segment"),
            pytest.param("", id="empty-string"),
            pytest.param(f"encode/ENCFF1/{FIXTURE_MD5}-v2", id="three-segments"),
            pytest.param(
                f"encode/ENCFF1/index/tabix/extra/{FIXTURE_MD5}-v2", id="six-segments"
            ),
            pytest.param(f"encode/ENCFF1/index/{FIXTURE_MD5}-v2/", id="trailing-slash"),
            pytest.param("encode/ENCFF1/index/", id="s3-directory-marker"),
            pytest.param(
                f"encode/ENCFF1/index/{_FIXTURE_MD5_UPPER}-v2", id="uppercase-md5-leaf"
            ),
            pytest.param(f"encode/ENCFF1/index/{FIXTURE_MD5[:31]}-v2", id="short-md5"),
            pytest.param(f"encode/ENCFF1/index/{FIXTURE_MD5}", id="no-version-suffix"),
            pytest.param(f"encode/ENCFF1/index/{FIXTURE_MD5}-v", id="empty-version"),
            pytest.param(f"encode/ENCFF1/index/{FIXTURE_MD5}-v2.tbi", id="leaf-suffix"),
            pytest.param(f"encode/ENCFF1/index/{FIXTURE_MD5}-v2 ", id="trailing-space"),
        ],
    )
    def test_is_legacy_cache_key_should_return_false_for_unclaimable_keys(self, key):
        """Test that the sweep never claims a key it does not own.

        Given:
            A key that is malformed, foreign, or of the wrong segment
            count — the shapes an unrelated object sharing the bucket
            could take.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return False. This predicate gates an irreversible
            delete, so a false positive is data loss while a false
            negative only leaves a stale object behind.
        """
        # Act & assert
        assert is_legacy_cache_key(key) is False

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param(f"logs/2024/01/{FIXTURE_MD5}-v2", id="foreign-object"),
            pytest.param(f"encode/ENCFF1/wat/{FIXTURE_MD5}-v2", id="not-an-artifact-kind"),
        ],
    )
    def test_is_legacy_cache_key_should_require_a_real_artifact_kind(self, key):
        """Test that a four-segment shape alone does not make a key ours.

        Given:
            A four-segment key with a content-addressed leaf whose third
            segment is not an ArtifactKind — an unrelated object that
            happens to match the retired scheme's shape.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return False. Every retired key carried a real
            artifact kind by construction, so requiring one costs no true
            positive and keeps the sweep off objects it does not own.
        """
        # Act & assert
        assert is_legacy_cache_key(key) is False

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param(f"encode/./data/{FIXTURE_MD5}-v2", id="dot-segment"),
            pytest.param(f"encode/../data/{FIXTURE_MD5}-v2", id="dotdot-segment"),
            pytest.param(f"./ENCFF1/data/{FIXTURE_MD5}-v2", id="leading-dot"),
        ],
    )
    def test_is_legacy_cache_key_should_reject_a_traversal_segment(self, key):
        """Test that a shape the producer could never mint is not claimed.

        Given:
            A four-segment key carrying a ``.`` or ``..`` segment.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return False. The cache backend refused ``..`` at
            put time, so the retired scheme provably never wrote such a
            key — and purge_s3 deletes through delete_objects directly,
            bypassing that validation, so the predicate is the only thing
            standing between a foreign object and an irreversible delete.
        """
        # Act & assert
        assert is_legacy_cache_key(key) is False

    def test_is_legacy_cache_key_should_return_false_for_an_over_stripped_current_key(
        self,
    ):
        """Test that a mis-prefixed live key is never claimed.

        Given:
            A current five-segment key with its leading dcc segment
            removed, exactly as ``purge_s3`` produces when handed a
            prefix carrying one segment too many.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return False, because the processor identity lands
            in the artifact-kind slot and fails that check. Without it a
            single mistyped WORKFLOW_S3_PREFIX would delete a live cache.
        """
        # Arrange
        live = cache_key(
            "encode", "ENCFF1", ArtifactKind.INDEX, FIXTURE_MD5, "tabix-interval", 2
        )

        # Act & assert
        assert is_legacy_cache_key(live.split("/", 1)[1]) is False

    def test_is_legacy_cache_key_should_return_false_for_a_workflow_key(self):
        """Test that the mutex namespace is not swept.

        Given:
            A key produced by workflow_key, which is also four segments
            but carries a bare ``v{n}`` leaf.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return False, so a mutex key stored alongside the
            cache is never mistaken for a retired artifact.
        """
        # Arrange
        mutex = workflow_key("encode", "ENCFF1", FIXTURE_MD5, 1)

        # Act & assert
        assert is_legacy_cache_key(mutex) is False

    @pytest.mark.parametrize("version", ["v0", "v10", "v01"])
    def test_is_legacy_cache_key_should_claim_any_non_negative_version(self, version):
        """Test that the whole retired version range is reclaimed.

        Given:
            Legacy-shaped keys whose leaf carries a single-digit,
            multi-digit, or zero-padded version.
        When:
            is_legacy_cache_key is called.
        Then:
            It should return True for each, so no corner of the retired
            population is left behind as unreachable storage cost.
        """
        # Act & assert
        assert is_legacy_cache_key(f"encode/ENCFF1/index/{FIXTURE_MD5}-{version}")

    @settings(max_examples=50)
    @given(
        dcc=_DCC_STRATEGY,
        local_id=_LOCAL_ID_STRATEGY,
        md5=_MD5_STRATEGY,
        artifact_kind=_ARTIFACT_KIND_STRATEGY,
        processor_id=_PROCESSOR_ID_STRATEGY,
        version=_VERSION_STRATEGY,
    )
    def test_is_legacy_cache_key_should_never_claim_a_derived_key(
        self, dcc, local_id, md5, artifact_kind, processor_id, version
    ):
        """Test that no key the pipeline can mint is ever sweepable.

        Given:
            Any key derived by cache_key from any valid inputs.
        When:
            is_legacy_cache_key is called on it.
        Then:
            It should always return False. This is the safety property
            that makes ``purge --apply`` sound: whatever the live
            pipeline writes, the sweep cannot delete it.
        """
        # Act
        derived = cache_key(dcc, local_id, artifact_kind, md5, processor_id, version)

        # Assert
        assert is_legacy_cache_key(derived) is False

    @settings(max_examples=50)
    @given(
        dcc=_DCC_STRATEGY,
        local_id=_LOCAL_ID_STRATEGY,
        md5=_MD5_STRATEGY,
        artifact_kind=_ARTIFACT_KIND_STRATEGY,
        version=_VERSION_STRATEGY,
    )
    def test_is_legacy_cache_key_should_claim_every_retired_key(
        self, dcc, local_id, md5, artifact_kind, version
    ):
        """Test that the sweep reclaims the entire retired population.

        Given:
            Any key assembled in the retired four-segment scheme from
            valid components.
        When:
            is_legacy_cache_key is called on it.
        Then:
            It should always return True, so the migration leaves no
            orphaned artifact paying storage cost forever.
        """
        # Arrange
        retired = (
            f"{normalize_dcc(dcc)}/{normalize_local_id(local_id)}/"
            f"{artifact_kind.value}/{normalize_md5(md5)}-v{version}"
        )

        # Act & assert
        assert is_legacy_cache_key(retired) is True


class TestShippedProcessorIdentities:
    def test_shipped_processor_ids_should_be_disjoint_from_artifact_kinds(self):
        """Test that no shipped identity collides with an artifact kind.

        Given:
            The three processors the API wires at startup.
        When:
            Their identities are compared against every ArtifactKind
            value.
        Then:
            The two sets should be disjoint. normalize_processor_id
            rejects the collision at class-definition time, so adding a
            member to ArtifactKind that matches a shipped identity would
            crash-loop the API on import — this pins the constraint in CI
            instead, where the enum is edited.
        """
        # Arrange
        shipped = {
            BamIndexProcessor.processor_id,
            PassthroughProcessor.processor_id,
            TabixIntervalProcessor.processor_id,
        }

        # Act & assert
        assert shipped.isdisjoint({kind.value for kind in ArtifactKind})
