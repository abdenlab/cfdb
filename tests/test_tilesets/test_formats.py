"""Tests for contact-map classification in ``cfdb.tilesets.formats``."""

from __future__ import annotations

from pathlib import PurePosixPath

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.tilesets.formats import (
    LINKS_PRESENTATION_SUFFIX,
    MatrixSource,
    is_bbi_interaction_source,
    matrix_source_kind,
    needs_materialization,
    split_bbi_presentation,
)

#: The suffixes the module claims, restated literally so the property
#: below asserts against the documented contract rather than against the
#: module's own lookup table.
_CONTACT_MAP_SUFFIXES = frozenset({".mcool", ".cool", ".hic"})

#: JSON-shaped scalars a stale or hand-edited document could carry in any
#: field.
_json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=False),
    st.text(),
)

#: Filename values biased toward the classification boundaries: real
#: contact-map names, casing variants, dotfiles, buried extensions, other
#: HDF5 formats, and outright non-strings.
_filename_values = st.one_of(
    _json_scalars,
    st.sampled_from(
        [
            "sample.mcool",
            "SAMPLE.MCOOL",
            "x.cool",
            "x.hic",
            ".mcool",
            "sample.mcool.gz",
            "a.tar.mcool",
            "dir/x.cool",
            "",
            "matrix.h5ad",
            "raw.h5",
        ]
    ),
    st.lists(st.text(), max_size=2),
)

#: file_format values spanning the right shape, the right shape with the
#: wrong name (including the case-variant ``hdf5``), and non-dict garbage.
_file_format_values = st.one_of(
    _json_scalars,
    st.dictionaries(st.text(), _json_scalars, max_size=3),
    st.just({"name": "HDF5"}),
    st.just({"name": "hdf5"}),
)

#: Arbitrary JSON-like file documents, each field independently absent or
#: malformed.
_documents = st.fixed_dictionaries(
    {},
    optional={
        "filename": _filename_values,
        "file_format": _file_format_values,
        "extra": _json_scalars,
    },
)


@st.composite
def _cased_contact_map_cases(draw):
    """Draw ``(filename, expected_kind)`` with a randomly-cased suffix."""
    stem = draw(st.text(min_size=1).filter(lambda s: "/" not in s))
    kind = draw(st.sampled_from(list(MatrixSource)))
    suffix = "." + kind.value
    upper_mask = draw(
        st.lists(st.booleans(), min_size=len(suffix), max_size=len(suffix))
    )
    cased = "".join(
        char.upper() if upper else char for char, upper in zip(suffix, upper_mask)
    )
    return stem + cased, kind


def _hdf5_doc(filename: str | None) -> dict:
    """Return a file document labelled HDF5 with the given filename."""
    doc: dict = {"file_format": {"name": "HDF5"}}
    if filename is not None:
        doc["filename"] = filename
    return doc


class TestMatrixSourceKind:
    """Extension is the only discriminator among HDF5-labelled files."""

    @pytest.mark.parametrize(
        ("filename", "expected"),
        [
            ("sample.mcool", MatrixSource.MCOOL),
            ("sample.cool", MatrixSource.COOL),
            ("sample.hic", MatrixSource.HIC),
        ],
    )
    def test_should_classify_each_contact_map_container(self, filename, expected):
        """Test that each supported container is recognized.

        Given:
            An HDF5-labelled document whose filename carries a contact-map
            extension.
        When:
            matrix_source_kind is called.
        Then:
            It should return the matching MatrixSource.
        """
        # Act
        result = matrix_source_kind(_hdf5_doc(filename))

        # Assert
        assert result is expected

    def test_should_classify_case_insensitively(self):
        """Test that an upper-cased extension still classifies.

        Given:
            A document named with an upper-cased ``.MCOOL`` extension.
        When:
            matrix_source_kind is called.
        Then:
            It should return MCOOL — upstream filename casing is not
            something the DCC guarantees.
        """
        # Act
        result = matrix_source_kind(_hdf5_doc("SAMPLE.MCOOL"))

        # Assert
        assert result is MatrixSource.MCOOL

    @pytest.mark.parametrize("filename", ["matrix.h5ad", "raw.h5", "data.hdf5"])
    def test_should_reject_other_hdf5_formats(self, filename):
        """Test that the HDF5 ontology collision is actually escaped.

        Given:
            A document sharing the ``HDF5`` EDAM term with contact maps —
            an AnnData matrix or a bare HDF5 container.
        When:
            matrix_source_kind is called.
        Then:
            It should return None. This is the whole reason the module
            exists: ``file_format.name`` is ``"HDF5"`` for all of these,
            so the registry alone cannot tell them apart.
        """
        # Act
        result = matrix_source_kind(_hdf5_doc(filename))

        # Assert
        assert result is None

    def test_should_reject_a_non_hdf5_format_with_a_matching_extension(self):
        """Test that the format gate runs before the extension check.

        Given:
            A document named ``sample.mcool`` but labelled ``BED``.
        When:
            matrix_source_kind is called.
        Then:
            It should return None — a filename alone must not be able to
            make a non-HDF5 record look like a contact map.
        """
        # Arrange
        doc = {"file_format": {"name": "BED"}, "filename": "sample.mcool"}

        # Act
        result = matrix_source_kind(doc)

        # Assert
        assert result is None

    @pytest.mark.parametrize(
        "doc",
        [
            {},
            {"filename": "sample.mcool"},
            {"file_format": None, "filename": "sample.mcool"},
            {"file_format": "HDF5", "filename": "sample.mcool"},
            {"file_format": {"name": "HDF5"}},
            {"file_format": {"name": "HDF5"}, "filename": ""},
            {"file_format": {"name": "HDF5"}, "filename": None},
        ],
        ids=[
            "empty",
            "no-format",
            "null-format",
            "format-not-a-dict",
            "no-filename",
            "empty-filename",
            "null-filename",
        ],
    )
    def test_should_return_none_for_an_incomplete_document(self, doc):
        """Test that a malformed document classifies as not-a-contact-map.

        Given:
            A document missing or mistyping ``file_format`` or ``filename``.
        When:
            matrix_source_kind is called.
        Then:
            It should return None rather than raising, so a stale or
            hand-edited record degrades to "not tileable" instead of
            500ing a request.
        """
        # Act
        result = matrix_source_kind(doc)

        # Assert
        assert result is None

    @pytest.mark.parametrize(
        "filename",
        [".mcool", "sample.mcool.gz"],
        ids=["dotfile", "buried-before-gz"],
    )
    def test_should_return_none_when_the_extension_is_not_the_final_suffix(
        self, filename
    ):
        """Test that only the final suffix can classify a file.

        Given:
            An HDF5-labelled document named ``.mcool`` — a dotfile whose
            whole name is the extension — or ``sample.mcool.gz``, where
            the contact-map extension sits before a final ``.gz``.
        When:
            matrix_source_kind is called.
        Then:
            It should return None for both — a contact-map extension
            anywhere but the final suffix position must not classify.
        """
        # Act
        result = matrix_source_kind(_hdf5_doc(filename))

        # Assert
        assert result is None

    @settings(max_examples=50)
    @given(doc=_documents)
    def test_should_classify_totally_and_soundly(self, doc):
        """Test totality and soundness over arbitrary documents.

        Given:
            An arbitrary JSON-like document — filename possibly a
            non-string, ``file_format`` possibly a non-dict, either field
            possibly absent, plus nested garbage.
        When:
            matrix_source_kind is called.
        Then:
            It should never raise, and return non-None exactly when the
            format name is ``"HDF5"``, the filename is a non-empty
            string, and its lower-cased final suffix is one of
            ``.mcool``/``.cool``/``.hic`` — with the returned kind
            matching that suffix.
        """
        # Act
        result = matrix_source_kind(doc)

        # Assert
        file_format = doc.get("file_format")
        filename = doc.get("filename")
        classifiable = (
            isinstance(file_format, dict)
            and file_format.get("name") == "HDF5"
            and isinstance(filename, str)
            and bool(filename)
            and PurePosixPath(filename.lower()).suffix in _CONTACT_MAP_SUFFIXES
        )
        if classifiable:
            assert isinstance(result, MatrixSource)
            assert "." + result.value == PurePosixPath(filename.lower()).suffix
        else:
            assert result is None

    @settings(max_examples=50)
    @given(case=_cased_contact_map_cases())
    def test_should_classify_invariantly_of_stem_and_casing(self, case):
        """Test stem- and casing-invariance of classification.

        Given:
            An HDF5-labelled document whose filename joins an arbitrary
            slash-free stem to a contact-map suffix under per-character
            random casing.
        When:
            matrix_source_kind and needs_materialization are called.
        Then:
            It should classify as the suffix's kind regardless of stem or
            casing, and needs_materialization should be True exactly for
            MCOOL and COOL.
        """
        # Arrange
        filename, expected = case
        doc = _hdf5_doc(filename)

        # Act
        kind = matrix_source_kind(doc)
        materialize = needs_materialization(doc)

        # Assert
        assert kind is expected
        assert materialize is (expected in {MatrixSource.MCOOL, MatrixSource.COOL})


class TestNeedsMaterialization:
    """Only coolers are materialized; .hic is refused with 501."""

    @pytest.mark.parametrize(
        ("filename", "expected"),
        [
            ("sample.mcool", True),
            ("sample.cool", True),
            ("sample.hic", False),
            ("matrix.h5ad", False),
        ],
    )
    def test_should_require_materialization_only_for_coolers(
        self, filename, expected
    ):
        """Test which formats gate on a cached artifact.

        Given:
            An HDF5-labelled document of each supported kind.
        When:
            needs_materialization is called.
        Then:
            Coolers should require it, while ``.hic`` should not — cfdb
            does not serve tiles from ``.hic`` and never materializes an
            artifact for it, so the endpoints refuse it with 501 instead
            of gating on a cached artifact.
        """
        # Act
        result = needs_materialization(_hdf5_doc(filename))

        # Assert
        assert result is expected


class TestImportHygiene:
    """The classifier crosses the wool pickle boundary, so it stays light."""

    def test_should_not_import_the_workflow_or_aws_stack(self):
        """Test that the module pulls in nothing heavy.

        Given:
            ``cfdb.tilesets.formats``, imported by the worker-side
            processor that is cloudpickled into a wool worker.
        When:
            Its module-level imports are inspected.
        Then:
            It should reference only the standard library — no boto3, no
            cfdb.workflows.cache, no clodius. A convenience import of
            ``processors.tools.format_name`` would drag the cache module
            and boto3 across the boundary with it.
        """
        # Arrange
        import ast
        import pathlib

        import cfdb.tilesets.formats as module

        source = pathlib.Path(module.__file__).read_text()
        tree = ast.parse(source)

        # Act
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])

        # Assert
        assert imported <= {"__future__", "enum", "pathlib", "typing"}, imported


class TestIsBbiInteractionSource:
    """bigInteract gets its own minted format name, not an EDAM term."""

    def test_should_recognize_a_biginteract_document(self):
        """Test that an exact format-name match is recognized.

        Given:
            A document whose ``file_format.name`` is exactly
            ``"bigInteract"``.
        When:
            is_bbi_interaction_source is called.
        Then:
            It should return True.
        """
        # Arrange
        doc = {"file_format": {"name": "bigInteract"}}

        # Act
        result = is_bbi_interaction_source(doc)

        # Assert
        assert result is True

    @pytest.mark.parametrize(
        "doc",
        [
            {"file_format": {"name": "bigBed"}},
            {"file_format": {"name": "BIGINTERACT"}},
            {"file_format": {"name": "biginteract"}},
            {},
            {"file_format": None},
            {"file_format": "bigInteract"},
            {"file_format": {}},
        ],
        ids=[
            "plain-bigbed",
            "upper-cased",
            "lower-cased",
            "no-format",
            "null-format",
            "format-not-a-dict",
            "format-missing-name",
        ],
    )
    def test_should_reject_everything_else(self, doc):
        """Test that only an exact, correctly-shaped match is recognized.

        Given:
            A document that is not a bigInteract file — wrong format
            name, wrong casing, or a malformed/missing ``file_format``.
        When:
            is_bbi_interaction_source is called.
        Then:
            It should return False rather than raising. The match is
            deliberately case-sensitive: cfdb mints this name itself
            (``services/ontology_mappings.py``), so there is no upstream
            casing variance to tolerate, unlike ``HDF5_FORMAT_NAME``.
        """
        # Act
        result = is_bbi_interaction_source(doc)

        # Assert
        assert result is False

    @settings(max_examples=50)
    @given(
        doc=st.fixed_dictionaries(
            {},
            optional={
                "file_format": st.one_of(
                    _json_scalars,
                    st.dictionaries(st.text(), _json_scalars, max_size=3),
                    st.just({"name": "bigInteract"}),
                    st.just({"name": "BIGINTERACT"}),
                    st.just({"name": "biginteract"}),
                    st.just({"name": "bigBed"}),
                )
            },
        )
    )
    def test_should_classify_totally_and_soundly(self, doc):
        """Test totality and soundness over arbitrary documents.

        Given:
            An arbitrary document whose ``file_format`` is possibly a
            non-dict, missing, or a dict with a name near the
            bigInteract/bigBed boundary.
        When:
            is_bbi_interaction_source is called.
        Then:
            It should never raise, and return True exactly when
            file_format is a dict with name == "bigInteract" exactly.
        """
        # Act
        result = is_bbi_interaction_source(doc)

        # Assert
        file_format = doc.get("file_format")
        expected = (
            isinstance(file_format, dict) and file_format.get("name") == "bigInteract"
        )
        assert result is expected


class TestSplitBbiPresentation:
    """The ``:links`` uid suffix that selects the 1D presentation."""

    def test_should_default_to_rectangles_with_no_suffix(self):
        """Test the default presentation for a plain uid.

        Given:
            A uid with no ``:links`` suffix.
        When:
            split_bbi_presentation is called.
        Then:
            It should return the uid unchanged, paired with
            "rectangles".
        """
        # Arrange
        uid = "encode/ENCFF000BIG"

        # Act
        base_uid, presentation = split_bbi_presentation(uid)

        # Assert
        assert base_uid == uid
        assert presentation == "rectangles"

    def test_should_select_links_and_strip_the_suffix(self):
        """Test the links presentation for a suffixed uid.

        Given:
            The same uid with a ``:links`` suffix appended.
        When:
            split_bbi_presentation is called.
        Then:
            It should return the base uid with the suffix removed,
            paired with "links".
        """
        # Arrange
        uid = "encode/ENCFF000BIG:links"

        # Act
        base_uid, presentation = split_bbi_presentation(uid)

        # Assert
        assert base_uid == "encode/ENCFF000BIG"
        assert presentation == "links"

    def test_should_split_a_uid_that_is_only_the_suffix(self):
        """Test the degenerate case where the whole uid is the suffix.

        Given:
            A uid equal to exactly ``:links``, with nothing before it.
        When:
            split_bbi_presentation is called.
        Then:
            It should return an empty base uid paired with "links" —
            the suffix still anchors at the end, even with nothing to
            strip it from.
        """
        # Arrange
        uid = ":links"

        # Act
        base_uid, presentation = split_bbi_presentation(uid)

        # Assert
        assert base_uid == ""
        assert presentation == "links"

    def test_should_not_match_the_suffix_in_the_middle_of_a_uid(self):
        """Test that the suffix must anchor at the end, not appear anywhere.

        Given:
            A uid that contains the literal text ``:links`` followed by
            more characters, rather than ending with it.
        When:
            split_bbi_presentation is called.
        Then:
            It should return the uid unchanged, paired with
            "rectangles" — a uid is only detected as the links
            presentation when ``:links`` is its trailing suffix.
        """
        # Arrange
        uid = "encode/ENCFF000BIG:linksextra"

        # Act
        base_uid, presentation = split_bbi_presentation(uid)

        # Assert
        assert base_uid == uid
        assert presentation == "rectangles"

    @settings(max_examples=100)
    @given(uid=st.text())
    def test_should_split_totally_and_invertibly(self, uid):
        """Test totality and the round-trip property over arbitrary strings.

        Given:
            Any string.
        When:
            split_bbi_presentation is called.
        Then:
            It should never raise; presentation should be "links" iff
            the input ends with the exact suffix; and rejoining
            base_uid with the suffix should recover the original
            whenever presentation is "links", while base_uid should
            equal the original string otherwise.
        """
        # Act
        base_uid, presentation = split_bbi_presentation(uid)

        # Assert
        if uid.endswith(LINKS_PRESENTATION_SUFFIX):
            assert presentation == "links"
            assert base_uid + LINKS_PRESENTATION_SUFFIX == uid
        else:
            assert presentation == "rectangles"
            assert base_uid == uid
