from hypothesis import given
from hypothesis import strategies as st

from cfdb.accessions import normalize_accession


def test_normalize_accession_should_upper_case_a_lower_case_accession():
    """Test that a lower-case accession folds to upper case.

    Given:
        A 4DN file accession typed entirely in lower case.
    When:
        normalize_accession is called.
    Then:
        It should return the upper-case form, which is how the accession
        is stored and therefore what an equality match needs.
    """
    # Act
    result = normalize_accession("4dnfimcjxzkh")

    # Assert
    assert result == "4DNFIMCJXZKH"


def test_normalize_accession_should_pass_an_upper_case_accession_through():
    """Test that an already-canonical accession is unchanged.

    Given:
        An ENCODE accession already in the published upper-case form.
    When:
        normalize_accession is called.
    Then:
        It should return the same string.
    """
    # Act
    result = normalize_accession("ENCFF525XQX")

    # Assert
    assert result == "ENCFF525XQX"


def test_normalize_accession_should_strip_surrounding_whitespace():
    """Test that padding a caller pasted in is removed.

    Given:
        An accession surrounded by leading and trailing whitespace.
    When:
        normalize_accession is called.
    Then:
        It should return the accession with the whitespace removed.
    """
    # Act
    result = normalize_accession("  ENCSR918ZSJ\n")

    # Assert
    assert result == "ENCSR918ZSJ"


def test_normalize_accession_should_return_none_when_value_is_none():
    """Test that a missing accession stays missing.

    Given:
        None, as produced when a DCC issues no accession.
    When:
        normalize_accession is called.
    Then:
        It should return None.
    """
    # Act
    result = normalize_accession(None)

    # Assert
    assert result is None


@given(blank=st.text(alphabet=" \t\n\r", max_size=8))
def test_normalize_accession_should_return_none_when_value_is_blank(blank):
    """Test that a blank accession collapses to None rather than "".

    Given:
        Any string made only of whitespace, including the empty string.
    When:
        normalize_accession is called.
    Then:
        It should return None, keeping an absent accession out of the
        index instead of storing an empty string.
    """
    # Act
    result = normalize_accession(blank)

    # Assert
    assert result is None


@given(value=st.text())
def test_normalize_accession_should_be_idempotent(value):
    """Test that folding an already-folded value changes nothing.

    Given:
        Any text at all.
    When:
        normalize_accession is applied twice.
    Then:
        The second application should return the first's result, so a
        value re-stamped by a later sync cannot drift.
    """
    # Act
    once = normalize_accession(value)
    twice = normalize_accession(once)

    # Assert
    assert twice == once


@given(value=st.text())
def test_normalize_accession_should_map_any_casing_to_one_value(value):
    """Test the property the case-insensitive query contract rests on.

    Given:
        Any text, and the same text upper-cased.
    When:
        Both are normalized.
    Then:
        They should produce the same value, so a caller's casing cannot
        change which documents an accession filter matches.
    """
    # Act
    from_value = normalize_accession(value)
    from_upper = normalize_accession(value.upper())

    # Assert
    assert from_value == from_upper
