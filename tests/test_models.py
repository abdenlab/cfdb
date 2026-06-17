from cfdb.models import (
    Biosample,
    Collection,
    EnrichedBiosample,
    EnrichedCollection,
    EnrichedFile,
    EnrichedSubject,
    ExtraFile,
    FileMetadataModel,
    Subject,
)


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
