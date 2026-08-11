from hypothesis import given
from hypothesis import strategies as st

from cfdb.api.gql.inputs import CollectionInput, FileMetadataInput, to_dict, to_query

#: Alphabet the DCCs actually issue accessions from.
_ACCESSION_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


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


def test_to_query_should_leave_non_string_leaves_unchanged():
    """Test that folding never reaches a non-string filter value.

    Given:
        A filter naming an integer-valued field.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit the integer unchanged, since folding is guarded on
        the value being a string.
    """
    # Act
    query = to_query({"size_in_bytes": [3221225472]})

    # Assert
    assert query == {"size_in_bytes": 3221225472}


def test_to_query_should_emit_none_for_a_blank_accession():
    """Test that an explicitly-blank accession selects absent values.

    Given:
        A filter whose accession value is whitespace only.
    When:
        to_query builds the MongoDB predicate.
    Then:
        It should emit None, matching documents with no accession rather
        than an empty string no document stores.
    """
    # Act
    query = to_query({"accession_id": ["   "]})

    # Assert
    assert query == {"accession_id": None}


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


@given(
    accession=st.text(alphabet=_ACCESSION_CHARS, min_size=1, max_size=16),
    swap=st.lists(st.booleans(), min_size=16, max_size=16),
)
def test_to_query_should_build_one_predicate_for_any_casing(accession, swap):
    """Test that casing a caller chooses cannot change the predicate.

    Given:
        Any accession over the DCC alphabet, and an arbitrary per-character
        re-casing of it.
    When:
        to_query builds a predicate from each.
    Then:
        Both should produce the identical predicate, which is the property
        the case-insensitive accession lookup rests on.
    """
    # Arrange
    recased = "".join(
        char.lower() if flip else char for char, flip in zip(accession, swap)
    )

    # Act
    from_canonical = to_query({"accession_id": [accession]})
    from_recased = to_query({"accession_id": [recased]})

    # Assert
    assert from_canonical == from_recased
