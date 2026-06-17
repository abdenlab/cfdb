"""Tests for 4DN enrichment helpers."""

from __future__ import annotations

from cfdb.services.fourdn import _parse_extra_files


def test__parse_extra_files_normalizes_dict_file_format_to_token():
    """Test that a CV-object file_format is stored as its token.

    Given:
        A raw 4DN extra_files entry whose file_format is an embedded CV
        object carrying the token under display_title.
    When:
        _parse_extra_files processes it.
    Then:
        The stored file_format should be the display_title string.
    """
    # Arrange
    raw = [
        {
            "href": "/files/x.pairs_px2",
            "file_format": {
                "principals_allowed": {"view": ["system.Everyone"]},
                "status": "released",
                "display_title": "pairs_px2",
            },
        }
    ]

    # Act
    result = _parse_extra_files(raw)

    # Assert
    assert result == [{"href": "/files/x.pairs_px2", "file_format": "pairs_px2"}]


def test__parse_extra_files_preserves_string_file_format_and_other_fields():
    """Test that a string file_format and the other fields carry through.

    Given:
        A raw entry with a bare-string file_format plus href, md5sum, and
        file_size.
    When:
        _parse_extra_files processes it.
    Then:
        It should preserve every field unchanged.
    """
    # Arrange
    raw = [
        {
            "href": "/files/x.bai",
            "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
            "file_size": 1024,
            "file_format": "bai",
        }
    ]

    # Act
    result = _parse_extra_files(raw)

    # Assert
    assert result == [
        {
            "href": "/files/x.bai",
            "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
            "file_size": 1024,
            "file_format": "bai",
        }
    ]


def test__parse_extra_files_returns_empty_list_for_empty_input():
    """Test that an empty extra_files list yields an empty list.

    Given:
        An empty list of raw extra_files entries.
    When:
        _parse_extra_files is called.
    Then:
        It should return an empty list.
    """
    # Act
    result = _parse_extra_files([])

    # Assert
    assert result == []


def test__parse_extra_files_drops_entry_yielding_no_fields():
    """Test that an entry with no usable fields is dropped.

    Given:
        A raw extra_files entry whose only key is a dict file_format with
        no display_title (so it normalizes away) alongside an entry with
        usable fields.
    When:
        _parse_extra_files processes the list.
    Then:
        It should drop the empty entry and keep the usable one.
    """
    # Arrange
    raw = [
        {"file_format": {"status": "released"}},
        {"href": "/files/x.bai", "file_format": "bai"},
    ]

    # Act
    result = _parse_extra_files(raw)

    # Assert
    assert result == [{"href": "/files/x.bai", "file_format": "bai"}]
