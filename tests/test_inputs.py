import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.accessions import normalize_accession
from cfdb.api.gql.inputs import (
    BiosampleInput,
    CollectionInput,
    EnrichedBiosampleInput,
    EnrichedCollectionInput,
    EnrichedEncodeBiosampleInput,
    EnrichedEncodeCollectionInput,
    EnrichedEncodeFileInput,
    EnrichedFileInput,
    FileMetadataInput,
    to_dict,
    to_query,
)

#: Alphabet the DCCs actually issue accessions from.
_ACCESSION_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


def test_to_dict_should_emit_every_declared_field():
    """Test that conversion yields the full field set, not just the set ones.

    Given:
        A FileMetadataInput with every field left at its None default.
    When:
        to_dict converts it.
    Then:
        It should emit a key for every declared field, since to_query is
        written to receive the whole set and drop the None values itself.
    """
    # Arrange
    declared = {
        field.name
        for field in FileMetadataInput.__strawberry_definition__.fields
    }

    # Act
    result = to_dict(FileMetadataInput())

    # Assert
    assert set(result) == declared
    assert all(value is None for value in result.values())


def test_to_dict_should_recurse_into_nested_input_lists():
    """Test that nested Strawberry inputs are converted, not passed through.

    Given:
        A FileMetadataInput whose collections field holds a CollectionInput.
    When:
        to_dict converts it.
    Then:
        It should leave no Strawberry object in the tree, since to_query
        can only walk dicts and lists.
    """
    # Act
    result = to_dict(
        FileMetadataInput(collections=[CollectionInput(accession_id=["X"])])
    )

    # Assert
    assert isinstance(result["collections"], list)
    assert result["collections"][0]["accession_id"] == ["X"]


def test_to_dict_should_pass_non_input_values_through_unchanged():
    """Test the passthrough branch, including that plain dicts are not walked.

    Given:
        A scalar, None, and a plain dict carrying no Strawberry definition.
    When:
        to_dict converts each.
    Then:
        It should return each unchanged -- notably not recursing into the
        plain dict, so to_dict is only safe on Strawberry inputs and lists
        of them.
    """
    # Arrange
    plain = {"accession_id": ["x"]}

    # Act & assert
    assert to_dict("ENCFF525XQX") == "ENCFF525XQX"
    assert to_dict(None) is None
    assert to_dict(plain) is plain


def _clauses(query: dict) -> set:
    """Return a query's $and clauses as an order-insensitive set."""
    return frozenset(tuple(sorted(clause.items())) for clause in query["$and"])


def test_to_dict_and_to_query_should_agree_with_a_hand_written_filter():
    """Test that the Strawberry path and a dict literal produce one query.

    Compared as a set of clauses rather than a list, because to_dict emits
    fields in declaration order while a dict literal preserves the order
    written -- so the two agree on the conjunction but not on its
    sequence, and $and is order-insensitive in MongoDB.

    Given:
        The same filter expressed as a FileMetadataInput and as a dict of
        only its set fields.
    When:
        Each is converted to a MongoDB query.
    Then:
        They should produce the same set of clauses, pinning the
        assumption every dict-literal test in this module rests on.
    """
    # Arrange
    payload = FileMetadataInput(filename=["a.bed"], accession_id=["encff1"])

    # Act
    from_input = to_query(to_dict(payload))
    from_dict = to_query({"filename": ["a.bed"], "accession_id": ["encff1"]})

    # Assert
    assert _clauses(from_input) == _clauses(from_dict)


def test_to_query_should_fold_accession_id_to_upper_case():
    """Test that a lower-case accession filter matches the stored form.

    Given:
        A filter naming accession_id in lower case.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit the upper-case accession, since the stored value
        is folded and the predicate is a bare equality match.
    """
    # Act
    query = to_query({"accession_id": ["4dnfimcjxzkh"]})

    # Assert
    assert query == {"accession_id": "4DNFIMCJXZKH"}


def test_to_query_should_fold_accession_id_nested_under_collections():
    """Test that the collection-level accession folds identically.

    Given:
        A filter naming accession_id inside the collections sub-input.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit the flattened collections.accession_id path with
        the value folded, matching the top-level behavior.
    """
    # Act
    query = to_query({"collections": [{"accession_id": ["encsr918zsj"]}]})

    # Assert
    assert query == {"collections.accession_id": "ENCSR918ZSJ"}


def test_to_query_should_strip_whitespace_around_an_accession():
    """Test that padding in a filter value does not defeat the match.

    Given:
        An accession filter value surrounded by whitespace.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit the accession with the whitespace removed.
    """
    # Act
    query = to_query({"accession_id": ["  encff525xqx  "]})

    # Assert
    assert query == {"accession_id": "ENCFF525XQX"}


def test_to_query_should_leave_other_string_fields_untouched():
    """Test that folding is scoped to accession_id alone.

    Given:
        A filter naming both accession_id and a case-sensitive sibling.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should fold only the accession, leaving filename byte-exact so
        unrelated fields keep their existing matching semantics.
    """
    # Act
    query = to_query(
        {"accession_id": ["encff525xqx"], "filename": ["MixedCase.bigBed"]}
    )

    # Assert
    assert query == {
        "$and": [
            {"accession_id": "ENCFF525XQX"},
            {"filename": "MixedCase.bigBed"},
        ]
    }


def test_to_query_should_not_fold_a_field_merely_containing_the_name():
    """Test that matching is on the whole leaf segment, not a substring.

    Given:
        A filter field whose name contains "accession_id" as a substring.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should leave the value unfolded, so the normalization cannot
        leak onto unrelated fields as new ones are added.
    """
    # Act
    query = to_query({"upstream_accession_id_note": ["lower case"]})

    # Assert
    assert query == {"upstream_accession_id_note": "lower case"}


def test_to_query_should_fold_every_value_of_an_or_clause():
    """Test that folding survives the list-to-OR expansion.

    Given:
        A filter naming several accessions in mixed casing.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit an $or of folded equality clauses, so no single
        branch of the disjunction is left unfolded.
    """
    # Act
    query = to_query({"accession_id": ["4dnf1", "EnCfF2", "encsr3"]})

    # Assert
    assert query == {
        "$or": [
            {"accession_id": "4DNF1"},
            {"accession_id": "ENCFF2"},
            {"accession_id": "ENCSR3"},
        ]
    }


def test_to_query_should_leave_a_non_string_accession_value_unchanged():
    """Test that folding is guarded on the value actually being a string.

    Given:
        A filter whose accession_id value is an integer, which the GraphQL
        layer will not produce but a direct to_query caller can.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit the integer unchanged rather than raising, pinning
        the isinstance guard. Targeting accession_id specifically is what
        makes this test meaningful -- asserted against a field that is not
        folded at all, it would pass with the guard deleted.
    """
    # Act
    query = to_query({"accession_id": [123]})

    # Assert
    assert query == {"accession_id": 123}


def test_to_query_should_leave_a_non_normalized_field_unchanged():
    """Test that an ordinary non-string field passes through.

    Given:
        A filter naming an integer-valued field that is not folded.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit the integer unchanged.
    """
    # Act
    query = to_query({"size_in_bytes": [3221225472]})

    # Assert
    assert query == {"size_in_bytes": 3221225472}


def test_to_query_should_drop_a_blank_accession():
    """Test that a blank accession constrains nothing rather than everything.

    Emitting ``{accession_id: None}`` would match documents whose accession
    is null *or absent* -- all of HuBMAP, every unparsed 4DN file, and the
    whole corpus before the first post-deploy sync. That made a blank value
    the only filter in the schema that widened the result set.

    Given:
        A filter whose only accession value is whitespace.
    When:
        to_query builds the MongoDB query.
    Then:
        It should emit an empty query, so a search box wired straight to
        the variable returns everything unfiltered rather than a page of
        unrelated accession-less files.
    """
    # Act
    query = to_query({"accession_id": ["   "]})

    # Assert
    assert query == {}


def test_to_query_should_keep_the_real_accession_when_one_value_is_blank():
    """Test that a blank value cannot widen a filter that also names one.

    Given:
        A filter carrying a real accession alongside a blank one, as a
        partly-filled multi-value input produces.
    When:
        to_query builds the MongoDB query.
    Then:
        It should emit only the real accession, rather than unioning in
        every document that has none.
    """
    # Act
    query = to_query({"accession_id": ["4DNFIMCJXZKH", "   "]})

    # Assert
    assert query == {"accession_id": "4DNFIMCJXZKH"}


def test_to_query_should_return_an_empty_query_for_a_filter_with_no_constraints():
    """Test that an empty clause list collapses instead of being emitted.

    MongoDB rejects ``{"$and": []}`` and ``{"$or": []}`` with BadValue, so
    a filter whose every field is unset has to collapse to a query that
    matches everything rather than one the server refuses.

    Given:
        A filter object with no fields set.
    When:
        to_query builds the MongoDB query.
    Then:
        It should emit an empty query.
    """
    # Act
    query = to_query([{"filename": None, "accession_id": None}])

    # Assert
    assert query == {}


def test_to_query_should_not_fold_an_accession_nested_under_extra():
    """Test that folding is decided by the whole path, not the leaf name.

    ``extra.<dcc>`` holds values exactly as the DCC published them, so a
    DCC-native accession stored there is not folded on write. Folding it on
    query would make those documents permanently unmatchable with nothing
    raising -- so an unlisted path must fail closed, matching byte-exactly
    like every other field.

    Given:
        A filter naming accession_id under the extra.fourdn namespace.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should leave the value unfolded.
    """
    # Act
    query = to_query({"extra": {"fourdn": {"accession_id": ["4dnfimcjxzkh"]}}})

    # Assert
    assert query == {"extra.fourdn.accession_id": "4dnfimcjxzkh"}


def test_to_query_should_accept_accession_id_from_the_graphql_inputs():
    """Test the wiring from the Strawberry inputs through to the query.

    Given:
        A FileMetadataInput carrying accession_id at both the file level
        and inside a nested CollectionInput.
    When:
        The input is converted with to_dict and then to_query.
    Then:
        It should produce folded predicates on both paths, pinning that
        the field is declared on both input classes.
    """
    # Arrange
    payload = FileMetadataInput(
        accession_id=["4dnfimcjxzkh"],
        collections=[CollectionInput(accession_id=["4dnexnhe6x77"])],
    )

    # Act
    query = to_query(to_dict(payload))

    # Assert
    assert query == {
        "$and": [
            {"collections.accession_id": "4DNEXNHE6X77"},
            {"accession_id": "4DNFIMCJXZKH"},
        ]
    }


#: Every path the ENCODE annotation ingest writes, paired with the filter
#: that must reach it. A mismatch between the two would leave the whole
#: annotation corpus unfilterable while every ingest test still passed, so
#: these are asserted against the paths ``transform_annotation_to_c2m2``
#: actually emits rather than against the schema alone.
_ANNOTATION_FILTER_PATHS = [
    ("annotation_type", "extra.encode.annotation_type"),
    ("organism", "extra.encode.organism"),
    ("assembly", "extra.encode.assembly"),
]


@pytest.mark.parametrize("field, path", _ANNOTATION_FILTER_PATHS)
def test_to_query_should_reach_the_file_level_encode_fields(field, path):
    """Test a file-level ENCODE filter flattens to the stored path.

    Given:
        A FileMetadataInput naming one of the ENCODE file-level fields.
    When:
        The input is converted with to_dict and then to_query.
    Then:
        It should produce a predicate on the dotted path the ingest
        writes.
    """
    # Arrange
    payload = FileMetadataInput(
        extra=[EnrichedFileInput(encode=[EnrichedEncodeFileInput(**{field: ["v"]})])]
    )

    # Act
    query = to_query(to_dict(payload))

    # Assert
    assert query == {path: "v"}


@pytest.mark.parametrize(
    "field, path",
    [
        ("annotation_type", "collections.extra.encode.annotation_type"),
        ("software_used", "collections.extra.encode.software_used"),
        ("encyclopedia_version", "collections.extra.encode.encyclopedia_version"),
    ],
)
def test_to_query_should_reach_the_dataset_level_encode_fields(field, path):
    """Test a dataset-level ENCODE filter flattens to the stored path.

    Given:
        A FileMetadataInput naming one of the ENCODE dataset-level fields
        inside a nested CollectionInput.
    When:
        The input is converted with to_dict and then to_query.
    Then:
        It should produce a predicate on the dotted path the ingest
        writes.
    """
    # Arrange
    payload = FileMetadataInput(
        collections=[
            CollectionInput(
                extra=[
                    EnrichedCollectionInput(
                        encode=[EnrichedEncodeCollectionInput(**{field: ["v"]})]
                    )
                ]
            )
        ]
    )

    # Act
    query = to_query(to_dict(payload))

    # Assert
    assert query == {path: "v"}


@pytest.mark.parametrize("field", ["life_stage", "age", "age_units"])
def test_to_query_should_reach_the_biosample_level_encode_fields(field):
    """Test the deepest ENCODE filter flattens to the stored path.

    Five segments through two array levels -- the shape most likely to be
    got wrong, and the one a client would notice last.

    Given:
        A FileMetadataInput naming a donor-trait field nested under
        collections.biosamples.
    When:
        The input is converted with to_dict and then to_query.
    Then:
        It should produce a predicate on the dotted path the ingest
        writes.
    """
    # Arrange
    payload = FileMetadataInput(
        collections=[
            CollectionInput(
                biosamples=[
                    BiosampleInput(
                        extra=[
                            EnrichedBiosampleInput(
                                encode=[
                                    EnrichedEncodeBiosampleInput(**{field: ["v"]})
                                ]
                            )
                        ]
                    )
                ]
            )
        ]
    )

    # Act
    query = to_query(to_dict(payload))

    # Assert
    assert query == {f"collections.biosamples.extra.encode.{field}": "v"}


@given(
    value=st.text(min_size=1, max_size=40),
    field_and_path=st.sampled_from(_ANNOTATION_FILTER_PATHS),
)
@settings(max_examples=100)
def test_to_query_should_not_fold_an_encode_annotation_filter(value, field_and_path):
    """Test the annotation filters match byte-exactly.

    ENCODE's vocabulary is case- and space-significant -- "candidate
    Cis-Regulatory Elements" is stored exactly as published -- so folding
    any of these the way accessions are folded would make them
    permanently unmatchable.

    Given:
        Any text value on any ENCODE file-level annotation field.
    When:
        to_query builds the predicate.
    Then:
        It should carry the value unaltered under its full dotted path.
    """
    # Arrange
    field, path = field_and_path
    payload = FileMetadataInput(
        extra=[
            EnrichedFileInput(encode=[EnrichedEncodeFileInput(**{field: [value]})])
        ]
    )

    # Act
    query = to_query(to_dict(payload))

    # Assert
    assert query == {path: value}


def test_to_query_should_collapse_a_single_clause():
    """Test that one set field yields a bare predicate, not a wrapper.

    Given:
        A filter dict with exactly one field set.
    When:
        to_query flattens it.
    Then:
        It should return the predicate without an $and wrapper.
    """
    # Act
    query = to_query({"filename": ["a.bed"]})

    # Assert
    assert query == {"filename": "a.bed"}


def test_to_query_should_drop_fields_left_unset():
    """Test that the None fields to_dict always emits are discarded.

    Given:
        A filter dict mixing set fields with explicitly-None ones, which is
        exactly what to_dict produces.
    When:
        to_query flattens it.
    Then:
        It should emit only the set fields, which is what makes to_dict's
        all-fields output usable as a filter.
    """
    # Act
    query = to_query({"filename": ["a.bed"], "md5": None, "sha256": None})

    # Assert
    assert query == {"filename": "a.bed"}


def test_to_query_should_flatten_nested_and_clauses():
    """Test that a nested conjunction is merged upward, not left nested.

    Given:
        A sub-input contributing two fields alongside a top-level field.
    When:
        to_query flattens it.
    Then:
        It should emit one flat three-element $and rather than an $and
        containing another $and.
    """
    # Act
    query = to_query(
        {"filename": ["a.bed"], "collections": {"name": ["c"], "lab": ["l"]}}
    )

    # Assert
    assert query == {
        "$and": [
            {"filename": "a.bed"},
            {"collections.name": "c"},
            {"collections.lab": "l"},
        ]
    }


def test_to_query_should_flatten_nested_or_clauses():
    """Test that a nested disjunction is merged upward.

    Given:
        A list of two sub-inputs, the first of which itself expands to an
        $or over two values.
    When:
        to_query flattens it.
    Then:
        It should emit one flat three-branch $or.
    """
    # Act
    query = to_query({"collections": [{"name": ["a", "b"]}, {"name": ["c"]}]})

    # Assert
    assert query == {
        "$or": [
            {"collections.name": "a"},
            {"collections.name": "b"},
            {"collections.name": "c"},
        ]
    }


def test_to_query_should_build_a_dotted_path_at_depth():
    """Test path construction several levels down.

    Given:
        A filter reaching collections.biosamples.subjects.local_id.
    When:
        to_query flattens it.
    Then:
        It should emit the four-segment dotted path as one predicate, and
        leave the value unfolded -- confirming with the substring test that
        folding is decided by the last segment alone, not by depth.
    """
    # Act
    query = to_query(
        {"collections": {"biosamples": {"subjects": {"local_id": ["Mixed-Case"]}}}}
    )

    # Assert
    assert query == {"collections.biosamples.subjects.local_id": "Mixed-Case"}


def test_to_query_should_return_a_bare_scalar_unchanged():
    """Test the no-prefix branch, which bypasses predicate construction.

    Given:
        A bare scalar passed with no prefix.
    When:
        to_query is called.
    Then:
        It should return the scalar unchanged and unfolded, since there is
        no field name to decide folding by.
    """
    # Act & assert
    assert to_query("4dnfimcjxzkh") == "4dnfimcjxzkh"


@given(
    accession=st.text(alphabet=_ACCESSION_CHARS, min_size=1, max_size=16),
    swap=st.lists(st.booleans(), min_size=16, max_size=16),
    pad=st.text(alphabet=" \t", max_size=3),
)
@settings(max_examples=200)
def test_to_query_should_build_one_predicate_for_any_casing(accession, swap, pad):
    """Test that casing a caller chooses cannot change the predicate.

    Given:
        Any accession over the DCC alphabet, an arbitrary per-character
        re-casing of it, and arbitrary surrounding padding.
    When:
        to_query builds a predicate from each.
    Then:
        Both should equal the canonical stored form -- a stronger claim
        than the two merely agreeing, which would also hold if both were
        folded wrongly in the same way.
    """
    # Arrange
    recased = "".join(
        char.lower() if flip else char for char, flip in zip(accession, swap)
    )
    expected = {"accession_id": normalize_accession(accession)}

    # Act
    from_canonical = to_query({"accession_id": [accession]})
    from_recased = to_query({"accession_id": [f"{pad}{recased}{pad}"]})

    # Assert
    assert from_canonical == expected
    assert from_recased == expected


@given(
    field=st.sampled_from(
        ["filename", "local_id", "md5", "sha256", "persistent_id", "access_url"]
    ),
    value=st.text(min_size=1),
)
@settings(max_examples=100)
def test_to_query_should_leave_every_other_field_byte_identical(field, value):
    """Test that no field other than the accession is ever folded.

    Given:
        Any real model field name other than accession_id, with any value.
    When:
        to_query builds the predicate.
    Then:
        It should emit the value byte-identical, so no future field can be
        folded by accident.
    """
    # Act
    query = to_query({field: [value]})

    # Assert
    assert query == {field: value}
