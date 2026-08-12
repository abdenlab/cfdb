import sys

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfdb.accessions import normalize_accession

#: Every code point ``str.strip()`` actually removes. Enumerated rather
#: than written out as ``" \t\n\r"`` because ``strip()`` removes roughly
#: 25 characters, so a hand-picked four would leave the ones a caller is
#: most likely to paste in (NBSP in particular) unexercised.
_UNICODE_WHITESPACE = "".join(
    c for c in map(chr, range(sys.maxunicode + 1)) if c.isspace()
)

#: The alphabet the DCCs actually issue accessions from.
_ACCESSION_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


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


def test_normalize_accession_should_keep_interior_whitespace():
    """Test that folding repairs padding but not a broken accession.

    Given:
        An accession with whitespace in the middle as well as around it.
    When:
        normalize_accession is called.
    Then:
        It should remove only the surrounding whitespace, so a mangled
        accession keeps a distinct key and fails to match rather than
        being silently repaired into a different file's accession.
    """
    # Act
    result = normalize_accession("  ENC FF525XQX ")

    # Assert
    assert result == "ENC FF525XQX"


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


def test_normalize_accession_should_raise_when_value_is_not_a_string():
    """Test that a non-string accession fails loudly rather than coercing.

    No current caller can reach this: encode reads every accession cell
    through ``_nonempty`` (str or None), and the sync passes regex match
    results. It is a defensive pin on the input contract, not a
    reachable path -- if a future caller does pass a non-string, the
    alternative to raising is storing a value no filter can ever match.

    Given:
        A non-string, non-None value.
    When:
        normalize_accession is called.
    Then:
        It should raise AttributeError rather than coercing.
    """
    # Act & assert
    with pytest.raises(AttributeError):
        normalize_accession(12345)


@given(blank=st.text(alphabet=_UNICODE_WHITESPACE, max_size=8))
@settings(max_examples=100)

def test_normalize_accession_should_return_none_when_value_is_blank(blank):
    """Test that a blank accession collapses to None rather than "".

    Given:
        Any string built from characters str.strip() removes, including
        the empty string.
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
@settings(max_examples=200)

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
@settings(max_examples=200)

def test_normalize_accession_should_not_depend_on_strip_upper_order(value):
    """Test that stripping and upper-casing commute for any input.

    Note this pins operation ordering, NOT case-insensitivity: the
    function's last step is already upper(), so comparing against an
    upper-cased input cannot fail by construction. The genuine
    case-insensitivity contract is pinned over the accession alphabet
    below, because outside it the fold is lossy -- "ẞ" normalizes to
    itself while its lower-case "ß" normalizes to "SS".

    Given:
        Any text, and the same text upper-cased.
    When:
        Both are normalized.
    Then:
        They should agree, so no input exists for which upper-casing
        before stripping would strip differently.
    """
    # Act
    from_value = normalize_accession(value)
    from_upper = normalize_accession(value.upper())

    # Assert
    assert from_value == from_upper


@given(
    accession=st.text(alphabet=_ACCESSION_CHARS, min_size=1, max_size=16),
    flips=st.lists(st.booleans(), min_size=16, max_size=16),
    pad_left=st.text(alphabet=" \t", max_size=3),
    pad_right=st.text(alphabet=" \t", max_size=3),
)
@settings(max_examples=200)

def test_normalize_accession_should_fold_every_casing_to_one_value(
    accession, flips, pad_left, pad_right
):
    """Test the case-insensitivity contract the whole feature rests on.

    Given:
        Any accession over the alphabet the DCCs issue from, plus its
        lower-cased, upper-cased and arbitrarily re-cased forms, each
        with arbitrary surrounding padding.
    When:
        All four are normalized.
    Then:
        They should produce one value, so which casing a caller types
        cannot change which documents an accession filter matches.
    """
    # Arrange
    recased = "".join(
        char.lower() if flip else char for char, flip in zip(accession, flips)
    )
    variants = [
        accession,
        accession.lower(),
        accession.upper(),
        f"{pad_left}{recased}{pad_right}",
    ]

    # Act
    folded = {normalize_accession(v) for v in variants}

    # Assert
    assert folded == {accession.upper()}
