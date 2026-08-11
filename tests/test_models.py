import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from cfdb.models import (
    NUMERIC_PROTOCOL_FIELDS,
    Biosample,
    Collection,
    EnrichedBiosample,
    EnrichedCollection,
    EnrichedFile,
    EnrichedFourdnCollection,
    EnrichedSubject,
    ExtraFile,
    FileMetadataModel,
    Subject,
    coerce_4dn_cv_token,
    coerce_scalar_to_str,
)


class TestCoerce4dnCvToken:
    def test_returns_display_title_for_cv_object(self):
        """Test that a CV object resolves to its display_title token.

        Given:
            A 4DN CV object carrying the token under display_title.
        When:
            coerce_4dn_cv_token is called.
        Then:
            It should return the display_title string.
        """
        # Act
        result = coerce_4dn_cv_token(
            {"status": "released", "display_title": "pairs_px2"}
        )

        # Assert
        assert result == "pairs_px2"

    def test_returns_none_for_cv_object_without_display_title(self):
        """Test that a CV object lacking display_title resolves to None.

        Given:
            A dict with no display_title key.
        When:
            coerce_4dn_cv_token is called.
        Then:
            It should return None.
        """
        # Act
        result = coerce_4dn_cv_token({"status": "released"})

        # Assert
        assert result is None

    def test_passes_string_through_unchanged(self):
        """Test that a bare string is returned unchanged.

        Given:
            A plain string token.
        When:
            coerce_4dn_cv_token is called.
        Then:
            It should return the string unchanged.
        """
        # Act, assert
        assert coerce_4dn_cv_token("bai") == "bai"

    def test_passes_none_through_unchanged(self):
        """Test that None is returned unchanged.

        Given:
            A None value.
        When:
            coerce_4dn_cv_token is called.
        Then:
            It should return None.
        """
        # Act, assert
        assert coerce_4dn_cv_token(None) is None


class TestCoerceScalarToStr:
    def test_stringifies_float_value(self):
        """Test that a float is coerced to its string form.

        Given:
            A float protocol value such as 25.0.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the string "25.0".
        """
        # Act, assert
        assert coerce_scalar_to_str(25.0) == "25.0"

    def test_stringifies_int_value(self):
        """Test that an int is coerced to its string form.

        Given:
            An int protocol value such as 37.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the string "37".
        """
        # Act, assert
        assert coerce_scalar_to_str(37) == "37"

    def test_passes_none_through_unchanged(self):
        """Test that None is returned unchanged.

        Given:
            A None value.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return None.
        """
        # Act, assert
        assert coerce_scalar_to_str(None) is None

    def test_passes_string_through_unchanged(self):
        """Test that a bare string is returned unchanged.

        Given:
            A plain string value.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the string unchanged.
        """
        # Act, assert
        assert coerce_scalar_to_str("25.0") == "25.0"

    def test_stringifies_bool_value(self):
        """Test that a bool is coerced to its string form.

        Given:
            A boolean value True (a numeric subtype that should stringify
            rather than slip through).
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the string "True".
        """
        # Act, assert
        assert coerce_scalar_to_str(True) == "True"

    def test_stringifies_positive_infinity(self):
        """Test that positive infinity is coerced to "inf".

        Given:
            The float value positive infinity.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the string "inf" (never compared by float
            equality).
        """
        # Act, assert
        assert coerce_scalar_to_str(float("inf")) == "inf"

    def test_stringifies_negative_infinity(self):
        """Test that negative infinity is coerced to "-inf".

        Given:
            The float value negative infinity.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the string "-inf".
        """
        # Act, assert
        assert coerce_scalar_to_str(float("-inf")) == "-inf"

    def test_stringifies_nan(self):
        """Test that NaN is coerced to the string "nan".

        Given:
            The float value NaN, which is never equal to itself.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the string "nan" (asserted on the string, since
            float NaN equality always fails).
        """
        # Act, assert
        assert coerce_scalar_to_str(float("nan")) == "nan"

    def test_passes_list_through_unchanged(self):
        """Test that a list is returned unchanged rather than stringified.

        Given:
            A list value on a coerced field.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the list unchanged (not its repr), so pydantic
            later rejects it with a clean string_type error.
        """
        # Arrange
        value = [1, 2, 3]

        # Act, assert
        assert coerce_scalar_to_str(value) is value

    def test_passes_dict_through_unchanged(self):
        """Test that a dict is returned unchanged rather than stringified.

        Given:
            A dict value on a coerced field.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the dict unchanged (not its repr), so pydantic
            later rejects it with a clean string_type error.
        """
        # Arrange
        value = {"value": 25.0}

        # Act, assert
        assert coerce_scalar_to_str(value) is value

    @given(st.integers() | st.floats(allow_nan=False, allow_infinity=False))
    def test_pbt_001_stringifies_any_finite_number_to_str_of_it(self, value):
        """Test that any finite number coerces to its own str().

        Given:
            Any int or finite float value.
        When:
            coerce_scalar_to_str is called.
        Then:
            The result should equal str(value) and be a str instance.
        """
        # Act
        result = coerce_scalar_to_str(value)

        # Assert
        assert result == str(value)
        assert isinstance(result, str)

    @given(st.text())
    def test_pbt_002_string_passthrough_is_idempotent(self, value):
        """Test that an arbitrary string passes through unchanged.

        Given:
            Any string value.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return the same string unchanged (str is its own str).
        """
        # Act, assert
        assert coerce_scalar_to_str(value) == value

    @given(st.none())
    def test_pbt_003_preserves_none(self, value):
        """Test that None is always preserved as None.

        Given:
            The None value.
        When:
            coerce_scalar_to_str is called.
        Then:
            It should return None.
        """
        # Act, assert
        assert coerce_scalar_to_str(value) is None


class TestExtraFile:
    def test_file_format_dict_coerced_to_display_title_token(self):
        """Test that a CV-object file_format is coerced to its token.

        Given:
            An ExtraFile constructed with file_format as a 4DN CV object
            carrying the token under display_title.
        When:
            The model is instantiated.
        Then:
            It should coerce file_format to the display_title string.
        """
        # Act
        result = ExtraFile(
            file_format={
                "principals_allowed": {"view": ["system.Everyone"]},
                "status": "released",
                "display_title": "pairs_px2",
            }
        )

        # Assert
        assert result.file_format == "pairs_px2"

    def test_file_format_string_passes_through(self):
        """Test that a bare string file_format is preserved.

        Given:
            An ExtraFile constructed with file_format as a plain string.
        When:
            The model is instantiated.
        Then:
            It should keep the string unchanged.
        """
        # Act
        result = ExtraFile(file_format="bai")

        # Assert
        assert result.file_format == "bai"

    def test_file_format_dict_without_display_title_coerced_to_none(self):
        """Test that a CV object lacking display_title becomes None.

        Given:
            An ExtraFile constructed with file_format as a dict that has
            no display_title key.
        When:
            The model is instantiated.
        Then:
            It should coerce file_format to None rather than raise.
        """
        # Act
        result = ExtraFile(file_format={"status": "released"})

        # Assert
        assert result.file_format is None

    def test_file_format_defaults_to_none_when_omitted(self):
        """Test that an omitted file_format stays None.

        Given:
            An ExtraFile constructed without a file_format.
        When:
            The model is instantiated.
        Then:
            file_format should be None (the validator passes None through).
        """
        # Act
        result = ExtraFile(href="/files/x.bai")

        # Assert
        assert result.file_format is None


class TestEnrichedFile:
    def test_empty_string_to_none_with_empty_fourdn(self):
        """Test empty string coercion on the fourdn field.

        Given:
            An EnrichedFile constructed with fourdn set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce fourdn to None.
        """
        # Act
        result = EnrichedFile(fourdn="")

        # Assert
        assert result.fourdn is None

    def test_empty_string_to_none_with_empty_encode(self):
        """Test empty string coercion on the encode field.

        Given:
            An EnrichedFile constructed with encode set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce encode to None.
        """
        # Act
        result = EnrichedFile(encode="")

        # Assert
        assert result.encode is None

    def test_empty_string_to_none_with_empty_hubmap(self):
        """Test empty string coercion on the hubmap field.

        Given:
            An EnrichedFile constructed with hubmap set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce hubmap to None.
        """
        # Act
        result = EnrichedFile(hubmap="")

        # Assert
        assert result.hubmap is None

    def test_empty_string_to_none_with_valid_dict(self):
        """Test that valid dicts are not coerced.

        Given:
            An EnrichedFile constructed with fourdn set to a valid dict.
        When:
            The model is instantiated.
        Then:
            It should parse the dict into an EnrichedFourdnFile.
        """
        # Arrange
        data = {"genome_assembly": "GRCh38"}

        # Act
        result = EnrichedFile(fourdn=data)

        # Assert
        assert result.fourdn is not None
        assert result.fourdn.genome_assembly == "GRCh38"


class TestEnrichedSubject:
    def test_empty_string_to_none_with_empty_hubmap(self):
        """Test empty string coercion on the hubmap field.

        Given:
            An EnrichedSubject constructed with hubmap set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce hubmap to None.
        """
        # Act
        result = EnrichedSubject(hubmap="")

        # Assert
        assert result.hubmap is None


class TestEnrichedFourdnCollection:
    def test_coerces_float_protocol_fields_to_strings(self):
        """Test that numeric protocol fields are coerced to strings.

        Given:
            An EnrichedFourdnCollection constructed with the eight protocol
            fields as floats (the shape the 4DN portal API sends and the
            sync persisted).
        When:
            The model is instantiated.
        Then:
            It should deserialize successfully and expose each field as its
            string representation rather than raising a string_type error.
        """
        # Arrange
        data = {
            "crosslinking_temperature": 25.0,
            "crosslinking_time": 10.0,
            "ligation_temperature": 25.0,
            "ligation_volume": 0.12,
            "ligation_time": 360.0,
            "digestion_temperature": 37.0,
            "digestion_time": 960.0,
            "average_fragment_size": 300.0,
        }

        # Act
        result = EnrichedFourdnCollection(**data)

        # Assert
        assert result.crosslinking_temperature == "25.0"
        assert result.crosslinking_time == "10.0"
        assert result.ligation_temperature == "25.0"
        assert result.ligation_volume == "0.12"
        assert result.ligation_time == "360.0"
        assert result.digestion_temperature == "37.0"
        assert result.digestion_time == "960.0"
        assert result.average_fragment_size == "300.0"

    def test_passes_string_protocol_field_through_unchanged(self):
        """Test that an already-string protocol field is preserved.

        Given:
            An EnrichedFourdnCollection constructed with a protocol field as
            a plain string.
        When:
            The model is instantiated.
        Then:
            It should keep the string unchanged.
        """
        # Act
        result = EnrichedFourdnCollection(crosslinking_temperature="25.0")

        # Assert
        assert result.crosslinking_temperature == "25.0"

    def test_leaves_omitted_protocol_field_as_none(self):
        """Test that an omitted protocol field stays None.

        Given:
            An EnrichedFourdnCollection constructed without any protocol
            fields.
        When:
            The model is instantiated.
        Then:
            crosslinking_temperature should be None (the validator passes
            None through).
        """
        # Act
        result = EnrichedFourdnCollection()

        # Assert
        assert result.crosslinking_temperature is None

    def test_coerces_int_protocol_field_to_string(self):
        """Test that an int protocol value is coerced to its string form.

        Given:
            An EnrichedFourdnCollection constructed with digestion_time as a
            bare int (960).
        When:
            The model is instantiated.
        Then:
            It should expose digestion_time as the string "960".
        """
        # Act
        result = EnrichedFourdnCollection(digestion_time=960)

        # Assert
        assert result.digestion_time == "960"

    def test_rejects_float_on_non_coerced_str_field(self):
        """Test that a non-coerced Optional[str] field rejects a float.

        Given:
            An EnrichedFourdnCollection constructed with crosslinking_method
            — an Optional[str] field outside the eight coerced fields — set to
            a float.
        When:
            The model is instantiated.
        Then:
            It should raise a pydantic ValidationError, proving the coercion
            validator is scoped to exactly the eight numeric protocol fields.
        """
        # Act, assert
        with pytest.raises(ValidationError):
            EnrichedFourdnCollection(crosslinking_method=1.5)

    def test_rejects_float_on_digestion_enzyme(self):
        """Test that the digestion_enzyme field rejects a float.

        Given:
            An EnrichedFourdnCollection constructed with digestion_enzyme —
            an Optional[str] field outside the coerced set — set to a float.
        When:
            The model is instantiated.
        Then:
            It should raise a pydantic ValidationError, confirming the
            coercion does not bleed onto sibling string fields.
        """
        # Act, assert
        with pytest.raises(ValidationError):
            EnrichedFourdnCollection(digestion_enzyme=1.5)

    def test_validator_registered_fields_match_canonical_constant(self):
        """Test the coercion validator is registered on exactly the canonical set.

        Given:
            The EnrichedFourdnCollection numeric-coercion validator and the
            canonical NUMERIC_PROTOCOL_FIELDS constant.
        When:
            The validator's registered field set is read from the model's
            pydantic decorators.
        Then:
            It should equal set(NUMERIC_PROTOCOL_FIELDS), so drift between the
            constant and the validator registration fails CI.
        """
        # Arrange
        validators = (
            EnrichedFourdnCollection.__pydantic_decorators__.field_validators
        )
        coercion_validator = validators["_coerce_numeric_protocol_field"]

        # Act, assert
        assert set(coercion_validator.info.fields) == set(NUMERIC_PROTOCOL_FIELDS)

    def test_rejects_list_on_coerced_field(self):
        """Test that a list on a coerced field raises rather than stringifying.

        Given:
            An EnrichedFourdnCollection constructed with digestion_time — a
            coerced numeric field — set to a list.
        When:
            The model is instantiated.
        Then:
            It should raise a pydantic ValidationError, because the hardened
            coercion passes non-scalars through unchanged for pydantic to
            reject (rather than masking them as a Python repr string).
        """
        # Act, assert
        with pytest.raises(ValidationError):
            EnrichedFourdnCollection(digestion_time=[1, 2, 3])

    def test_rejects_dict_on_coerced_field(self):
        """Test that a dict on a coerced field raises rather than stringifying.

        Given:
            An EnrichedFourdnCollection constructed with crosslinking_temperature
            — a coerced numeric field — set to a dict.
        When:
            The model is instantiated.
        Then:
            It should raise a pydantic ValidationError, because the hardened
            coercion passes non-scalars through unchanged for pydantic to
            reject (rather than masking them as a Python repr string).
        """
        # Act, assert
        with pytest.raises(ValidationError):
            EnrichedFourdnCollection(crosslinking_temperature={"value": 25.0})

    def test_passes_fragment_size_range_string_through_unchanged(self):
        """Test that fragment_size_range is left out of the coerced set.

        Given:
            An EnrichedFourdnCollection constructed with fragment_size_range
            as a genuine range string (e.g. "200-400") — a sibling
            Optional[str] field deliberately excluded from the coerced set.
        When:
            The model is instantiated.
        Then:
            It should keep the range string unchanged, locking the boundary of
            the observation-scoped NUMERIC_PROTOCOL_FIELDS set.
        """
        # Act
        result = EnrichedFourdnCollection(fragment_size_range="200-400")

        # Assert
        assert result.fragment_size_range == "200-400"

    @pytest.mark.parametrize("field_name", NUMERIC_PROTOCOL_FIELDS)
    @given(value=st.floats(allow_nan=False, allow_infinity=False))
    def test_pbt_001_each_numeric_field_coerces_finite_float_to_str(
        self, field_name, value
    ):
        """Test that each of the eight fields stringifies a finite float.

        Given:
            Each of the eight numeric protocol fields, and any finite float.
        When:
            An EnrichedFourdnCollection is constructed with that field set to
            the float.
        Then:
            The deserialized field should equal str(value).
        """
        # Act
        result = EnrichedFourdnCollection(**{field_name: value})

        # Assert
        assert getattr(result, field_name) == str(value)


class TestEnrichedCollection:
    def test_empty_string_to_none_with_empty_fourdn(self):
        """Test empty string coercion on the fourdn field.

        Given:
            An EnrichedCollection constructed with fourdn set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce fourdn to None.
        """
        # Act
        result = EnrichedCollection(fourdn="")

        # Assert
        assert result.fourdn is None

    def test_empty_string_to_none_with_empty_hubmap(self):
        """Test empty string coercion on the hubmap field.

        Given:
            An EnrichedCollection constructed with hubmap set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce hubmap to None.
        """
        # Act
        result = EnrichedCollection(hubmap="")

        # Assert
        assert result.hubmap is None

    def test_empty_string_to_none_with_empty_encode(self):
        """Test empty string coercion on the encode field.

        Given:
            An EnrichedCollection constructed with encode set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce encode to None.
        """
        # Act
        result = EnrichedCollection(encode="")

        # Assert
        assert result.encode is None


class TestEnrichedBiosample:
    def test_empty_string_to_none_with_empty_encode(self):
        """Test empty string coercion on the encode field.

        Given:
            An EnrichedBiosample constructed with encode set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce encode to None.
        """
        # Act
        result = EnrichedBiosample(encode="")

        # Assert
        assert result.encode is None


class TestFileMetadataModel:
    def test___init___should_default_accession_id_to_none(self):
        """Test that a file without an accession carries None.

        Given:
            A document omitting accession_id, as every document does before
            a sync populates it and as HuBMAP files do permanently.
        When:
            The model is instantiated.
        Then:
            It should leave accession_id as None rather than failing or
            defaulting to an empty string.
        """
        # Act
        result = FileMetadataModel(**_minimal_file_metadata())

        # Assert
        assert result.accession_id is None

    def test___init___should_round_trip_an_accession_id(self):
        """Test that a populated accession survives model construction.

        Given:
            A materialized document carrying accession_id.
        When:
            The model is instantiated.
        Then:
            It should expose the accession unchanged, since the model is what
            the GraphQL output type is generated from.
        """
        # Arrange
        doc = {**_minimal_file_metadata(), "accession_id": "4DNFIMCJXZKH"}

        # Act
        result = FileMetadataModel(**doc)

        # Assert
        assert result.accession_id == "4DNFIMCJXZKH"

    def test___init___should_preserve_the_uncompressed_sentinel(self):
        """Test that the uncompressed sentinel is not collapsed into None.

        Given:
            One document whose compression_format is the empty string, and
            one that omits the field entirely.
        When:
            Both are instantiated.
        Then:
            The first should keep "" and the second should be None, since ""
            means known-uncompressed while None means undetermined.
        """
        # Arrange
        uncompressed = {**_minimal_file_metadata(), "compression_format": ""}
        undetermined = _minimal_file_metadata()

        # Act
        uncompressed_result = FileMetadataModel(**uncompressed)
        undetermined_result = FileMetadataModel(**undetermined)

        # Assert
        assert uncompressed_result.compression_format == ""
        assert undetermined_result.compression_format is None

    def test___init___should_reject_a_subdocument_compression_format(self):
        """Test that the field stays a scalar rather than an EDAM subdocument.

        Given:
            A document whose compression_format is an id/name dict, the shape
            file_format takes after materialization.
        When:
            The model is instantiated.
        Then:
            It should raise a ValidationError, since the GraphQL type, the
            input filter and the upstream C2M2 column are all scalar strings.
        """
        # Arrange
        data = {
            **_minimal_file_metadata(),
            "compression_format": {"id": "format:3989", "name": "gzip"},
        }

        # Act & assert
        with pytest.raises(ValidationError):
            FileMetadataModel(**data)

    def test_deserializes_with_dict_shaped_extra_file_format(self):
        """Test that a 4DN doc with a CV-object extra_files file_format loads.

        Given:
            A FileMetadataModel document whose
            extra.fourdn.extra_files[0].file_format is a 4DN CV object
            (the shape persisted by the sync that crashed the files query).
        When:
            The model is instantiated.
        Then:
            It should deserialize successfully and expose the file_format
            display_title token as a string.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["extra"] = {
            "fourdn": {
                "extra_files": [
                    {
                        "href": "/files/4DNFITEST001/x.pairs_px2",
                        "file_format": {
                            "status": "released",
                            "display_title": "pairs_px2",
                        },
                    }
                ]
            }
        }

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert result.extra.fourdn.extra_files[0].file_format == "pairs_px2"

    def test_deserializes_with_float_collection_protocol_fields(self):
        """Test that a 4DN doc with float collection protocol fields loads.

        Given:
            A FileMetadataModel document whose
            collections[0].extra.fourdn protocol fields are floats (the
            shape persisted by the sync that crashed the files query).
        When:
            The model is instantiated.
        Then:
            It should deserialize successfully and expose each protocol
            value as its string representation.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["collections"] = [
            {
                "biosamples": [],
                "extra": {
                    "fourdn": {
                        "crosslinking_temperature": 25.0,
                        "digestion_time": 960.0,
                        "ligation_volume": 0.12,
                    }
                },
            }
        ]

        # Act
        result = FileMetadataModel(**data)

        # Assert
        fourdn = result.collections[0].extra.fourdn
        assert fourdn.crosslinking_temperature == "25.0"
        assert fourdn.digestion_time == "960.0"
        assert fourdn.ligation_volume == "0.12"

    def test_model_dump_round_trip_emits_str_typed_protocol_values(self):
        """Test that the resolver round-trip yields str-typed protocol values.

        Given:
            A 4DN file document whose collections[0].extra.fourdn protocol
            fields are floats — the exact shape the resolver loads.
        When:
            FileMetadataModel(**file).model_dump() runs (the precise call the
            GraphQL resolver makes).
        Then:
            The dumped nested protocol values should be str-typed, not floats,
            so the Strawberry conversion does not blow up downstream.
        """
        # Arrange
        file = _minimal_file_metadata()
        file["collections"] = [
            {
                "biosamples": [],
                "extra": {
                    "fourdn": {
                        "crosslinking_temperature": 25.0,
                        "digestion_time": 960.0,
                        "ligation_volume": 0.12,
                    }
                },
            }
        ]

        # Act
        dumped = FileMetadataModel(**file).model_dump()

        # Assert
        fourdn = dumped["collections"][0]["extra"]["fourdn"]
        assert fourdn["crosslinking_temperature"] == "25.0"
        assert fourdn["digestion_time"] == "960.0"
        assert fourdn["ligation_volume"] == "0.12"
        assert isinstance(fourdn["crosslinking_temperature"], str)
        assert isinstance(fourdn["digestion_time"], str)
        assert isinstance(fourdn["ligation_volume"], str)

    def test_deserializes_with_all_eight_float_protocol_fields(self):
        """Test that all eight nested protocol fields coerce when floats.

        Given:
            A 4DN file document whose collections[0].extra.fourdn carries all
            eight numeric protocol fields as floats.
        When:
            The model is instantiated.
        Then:
            Every one of the eight fields should expose its string form.
        """
        # Arrange
        floats = {
            "crosslinking_temperature": 25.0,
            "crosslinking_time": 10.0,
            "ligation_temperature": 16.0,
            "ligation_volume": 0.12,
            "ligation_time": 360.0,
            "digestion_temperature": 37.0,
            "digestion_time": 960.0,
            "average_fragment_size": 300.0,
        }
        data = _minimal_file_metadata()
        data["collections"] = [{"biosamples": [], "extra": {"fourdn": dict(floats)}}]

        # Act
        result = FileMetadataModel(**data)

        # Assert
        fourdn = result.collections[0].extra.fourdn
        for field_name, value in floats.items():
            assert getattr(fourdn, field_name) == str(value)

    def test_deserializes_multi_collection_doc_with_one_float_bearing(self):
        """Test that a multi-collection doc deserializes each collection.

        Given:
            A 4DN file document with two collections — one carrying float
            protocol fields under extra.fourdn and one clean collection.
        When:
            The model is instantiated.
        Then:
            Both collections should deserialize, with the float-bearing one
            stringified and the clean one carrying its plain value.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["collections"] = [
            {
                "biosamples": [],
                "extra": {"fourdn": {"digestion_time": 960.0}},
            },
            {
                "biosamples": [],
                "extra": {"fourdn": {"crosslinking_method": "1% Formaldehyde"}},
            },
        ]

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert len(result.collections) == 2
        assert result.collections[0].extra.fourdn.digestion_time == "960.0"
        assert (
            result.collections[1].extra.fourdn.crosslinking_method
            == "1% Formaldehyde"
        )

    def test_empty_string_to_none_with_empty_file_format(self):
        """Test empty string coercion on the file_format field.

        Given:
            A FileMetadataModel constructed with file_format set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce file_format to None.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["file_format"] = ""

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert result.file_format is None

    def test_empty_string_to_none_with_empty_data_type(self):
        """Test empty string coercion on the data_type field.

        Given:
            A FileMetadataModel constructed with data_type set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce data_type to None.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["data_type"] = ""

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert result.data_type is None

    def test_empty_string_to_none_with_empty_assay_type(self):
        """Test empty string coercion on the assay_type field.

        Given:
            A FileMetadataModel constructed with assay_type set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce assay_type to None.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["assay_type"] = ""

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert result.assay_type is None

    def test_empty_string_to_none_with_empty_project(self):
        """Test empty string coercion on the project field.

        Given:
            A FileMetadataModel constructed with project set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce project to None.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["project"] = ""

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert result.project is None

    def test_empty_string_to_none_with_empty_extra(self):
        """Test empty string coercion on the extra field.

        Given:
            A FileMetadataModel constructed with extra set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce extra to None.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["extra"] = ""

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert result.extra is None

    def test_empty_string_to_none_with_valid_file_format(self):
        """Test that valid file_format dicts are not coerced.

        Given:
            A FileMetadataModel constructed with file_format set to a valid dict.
        When:
            The model is instantiated.
        Then:
            It should parse the dict into a FileFormat.
        """
        # Arrange
        data = _minimal_file_metadata()
        data["file_format"] = {"id": "format:001", "name": "FASTQ"}

        # Act
        result = FileMetadataModel(**data)

        # Assert
        assert result.file_format is not None
        assert result.file_format.name == "FASTQ"


class TestCollection:
    def test___init___should_default_accession_id_to_none(self):
        """Test that a collection without an accession carries None.

        Given:
            A Collection constructed without accession_id, as ENCODE's
            biosample-keyed fallback collection is.
        When:
            The model is instantiated.
        Then:
            It should leave accession_id as None.
        """
        # Act
        result = Collection(biosamples=[])

        # Assert
        assert result.accession_id is None

    def test___init___should_round_trip_an_accession_id(self):
        """Test that a populated experiment accession survives construction.

        Given:
            A Collection carrying a 4DN experiment accession.
        When:
            The model is instantiated.
        Then:
            It should expose the accession unchanged, since the nested GraphQL
            collection type is generated from this model.
        """
        # Act
        result = Collection(biosamples=[], accession_id="4DNEXNHE6X77")

        # Assert
        assert result.accession_id == "4DNEXNHE6X77"

    def test_empty_string_to_none_with_empty_extra(self):
        """Test empty string coercion on the extra field.

        Given:
            A Collection constructed with extra set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce extra to None.
        """
        # Act
        result = Collection(biosamples=[], extra="")

        # Assert
        assert result.extra is None


class TestBiosample:
    def test_empty_string_to_none_with_empty_anatomy(self):
        """Test empty string coercion on the anatomy field.

        Given:
            A Biosample constructed with anatomy set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce anatomy to None.
        """
        # Act
        result = Biosample(anatomy="")

        # Assert
        assert result.anatomy is None

    def test_empty_string_to_none_with_empty_extra(self):
        """Test empty string coercion on the extra field.

        Given:
            A Biosample constructed with extra set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce extra to None.
        """
        # Act
        result = Biosample(extra="")

        # Assert
        assert result.extra is None

    def test_empty_string_to_none_with_valid_anatomy(self):
        """Test that valid anatomy dicts are not coerced.

        Given:
            A Biosample constructed with anatomy set to a valid dict.
        When:
            The model is instantiated.
        Then:
            It should parse the dict into an Anatomy.
        """
        # Arrange
        anatomy_data = {"id": "UBERON:0002107", "name": "liver"}

        # Act
        result = Biosample(anatomy=anatomy_data)

        # Assert
        assert result.anatomy is not None
        assert result.anatomy.name == "liver"


class TestSubject:
    def test_empty_string_to_none_with_empty_age_at_enrollment(self):
        """Test empty string coercion on the age_at_enrollment field.

        Given:
            A Subject constructed with age_at_enrollment set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce age_at_enrollment to None.
        """
        # Act
        result = Subject(age_at_enrollment="")

        # Assert
        assert result.age_at_enrollment is None

    def test_empty_string_to_none_with_empty_taxonomy(self):
        """Test empty string coercion on the taxonomy field.

        Given:
            A Subject constructed with taxonomy set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce taxonomy to None.
        """
        # Act
        result = Subject(taxonomy="")

        # Assert
        assert result.taxonomy is None

    def test_empty_string_to_none_with_empty_extra(self):
        """Test empty string coercion on the extra field.

        Given:
            A Subject constructed with extra set to an empty string.
        When:
            The model is instantiated.
        Then:
            It should coerce extra to None.
        """
        # Act
        result = Subject(extra="")

        # Assert
        assert result.extra is None

    def test_empty_string_to_none_with_valid_taxonomy(self):
        """Test that valid taxonomy dicts are not coerced.

        Given:
            A Subject constructed with taxonomy set to a valid dict.
        When:
            The model is instantiated.
        Then:
            It should parse the dict into an NcbiTaxonomy.
        """
        # Arrange
        taxonomy_data = {"id": "NCBITaxon:9606", "name": "Homo sapiens"}

        # Act
        result = Subject(taxonomy=taxonomy_data)

        # Assert
        assert result.taxonomy is not None
        assert result.taxonomy.name == "Homo sapiens"


def _minimal_file_metadata():
    """Return the minimal required fields for constructing a FileMetadataModel."""
    return {
        "dcc": {"id": "cfde_registry_dcc:hubmap", "dcc_name": "HuBMAP"},
        "collections": [],
    }
