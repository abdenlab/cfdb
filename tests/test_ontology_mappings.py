"""Tests for ENCODE file format mapping in ontology_mappings."""

from __future__ import annotations

from cfdb.services.ontology_mappings import get_file_format


class TestGetFileFormat:
    def test_get_file_format_with_simple_format(self):
        """
        GIVEN a simple format string that exactly matches a dictionary key
        WHEN get_file_format is called with "fastq"
        THEN the EDAM FASTQ term is returned
        """
        # Act
        result = get_file_format("fastq")

        # Assert
        assert result == {"id": "format:1930", "name": "FASTQ"}

    def test_get_file_format_with_compound_bigbed(self):
        """
        GIVEN a compound format string "bigBed narrowPeak" with no exact match
        WHEN get_file_format is called
        THEN the first token "bigBed" is used as a fallback lookup
        """
        # Act
        result = get_file_format("bigBed narrowPeak")

        # Assert
        assert result == {"id": "format:3004", "name": "bigBed"}

    def test_get_file_format_with_compound_bed(self):
        """
        GIVEN a compound format string "bed bed3+" with no exact match
        WHEN get_file_format is called
        THEN the first token "bed" is used as a fallback lookup
        """
        # Act
        result = get_file_format("bed bed3+")

        # Assert
        assert result == {"id": "format:3003", "name": "BED"}

    def test_get_file_format_with_compound_bigwig(self):
        """
        GIVEN a compound format string "bigWig bed3+" with no exact match
        WHEN get_file_format is called
        THEN the first token "bigWig" is used as a fallback lookup
        """
        # Act
        result = get_file_format("bigWig bed3+")

        # Assert
        assert result == {"id": "format:3006", "name": "bigWig"}

    def test_get_file_format_with_exact_compound_key(self):
        """
        GIVEN a compound format string "bed narrowPeak" that has an exact match
        WHEN get_file_format is called
        THEN the exact match (NarrowPeak) takes precedence over the first-token fallback (BED)
        """
        # Act
        result = get_file_format("bed narrowPeak")

        # Assert
        assert result == {"id": "format:3613", "name": "NarrowPeak"}

    def test_get_file_format_with_starch(self):
        """
        GIVEN the format string "starch" (BEDOPS compressed BED archive)
        WHEN get_file_format is called
        THEN the EDAM BED term is returned
        """
        # Act
        result = get_file_format("starch")

        # Assert
        assert result == {"id": "format:3003", "name": "BED"}

    def test_get_file_format_with_tagalign_case_insensitive(self):
        """
        GIVEN the format string "tagAlign" with mixed case
        WHEN get_file_format is called
        THEN the lookup is case-insensitive and returns the EDAM BED term
        """
        # Act
        result = get_file_format("tagAlign")

        # Assert
        assert result == {"id": "format:3003", "name": "BED"}

    def test_get_file_format_with_biginteract(self):
        """
        GIVEN the format string "bigInteract" (a bigBed variant)
        WHEN get_file_format is called
        THEN the EDAM bigBed term is returned
        """
        # Act
        result = get_file_format("bigInteract")

        # Assert
        assert result == {"id": "format:3004", "name": "bigBed"}

    def test_get_file_format_with_h5ad(self):
        """
        GIVEN the format string "h5ad" (AnnData HDF5 format)
        WHEN get_file_format is called
        THEN the EDAM HDF5 term is returned
        """
        # Act
        result = get_file_format("h5ad")

        # Assert
        assert result == {"id": "format:3590", "name": "HDF5"}

    def test_get_file_format_with_empty_string(self):
        """
        GIVEN an empty format string
        WHEN get_file_format is called
        THEN None is returned
        """
        # Act
        result = get_file_format("")

        # Assert
        assert result is None

    def test_get_file_format_with_unknown_format(self):
        """
        GIVEN an unrecognized format string "xyzzy"
        WHEN get_file_format is called
        THEN None is returned
        """
        # Act
        result = get_file_format("xyzzy")

        # Assert
        assert result is None

    def test_get_file_format_with_unknown_compound(self):
        """
        GIVEN an unrecognized compound format string "xyzzy foo"
        WHEN get_file_format is called
        THEN None is returned (neither the full string nor the first token matches)
        """
        # Act
        result = get_file_format("xyzzy foo")

        # Assert
        assert result is None
