# -*- coding: utf-8 -*-

"""
Tests `marc.marc_writer.py` module
"""
from contextlib import nullcontext as does_not_raise
import logging
import os
import pickle

from pymarc import Field, MARCReader, Record
import pytest

from nightshift import __title__, __version__
from nightshift.datastore import Resource
from nightshift.marc.marc_writer import BibEnhancer


class TestBibEnhancer:
    def test_init(self, caplog, stub_resource):
        with does_not_raise():
            with caplog.at_level(logging.INFO):
                result = BibEnhancer(stub_resource)

        assert result.library == "NYP"
        assert isinstance(result.resource, Resource)
        assert isinstance(result.bib, Record)
        assert "Enhancing NYP Sierra bib # b11111111a." in caplog.text

    def test_missing_full_bib(self, stub_resource):
        stub_resource.fullBib = None
        with pytest.raises(TypeError):
            BibEnhancer(stub_resource)

    @pytest.mark.parametrize(
        "resourceId,libraryId,tag, expectation",
        [
            (1, 1, "091", "eNYPL Book"),
            (2, 1, "091", "eNYPL Audio"),
            (3, 1, "091", "eNYPL Video"),
            (1, 2, "099", "eBOOK"),
            (2, 2, "099", "eAUDIO"),
            (3, 2, "099", "eVIDEO"),
        ],
    )
    def test_add_call_number_supported_resource_categories(
        self, caplog, stub_resource, resourceId, libraryId, tag, expectation
    ):
        stub_resource.resourceCategoryId = resourceId
        stub_resource.libraryId = libraryId
        if libraryId == 1:
            library = "NYP"
        elif libraryId == 2:
            library = "BPL"
        be = BibEnhancer(stub_resource)
        with caplog.at_level(logging.DEBUG):
            assert be._add_call_number() is True
        assert f"Added {expectation} to {library} b11111111a." in caplog.text
        bib = be.bib
        assert str(bib[tag]) == f"={tag}  \\\\$a{expectation}"

    @pytest.mark.parametrize("library,resourceId", [("QPL", 1), ("NYP", 4), ("BPL", 4)])
    def test_add_call_number_unsupported_resources(
        self, caplog, stub_resource, library, resourceId
    ):
        stub_resource.resourceCategoryId = resourceId
        be = BibEnhancer(stub_resource)
        be.library = library

        with caplog.at_level(logging.WARN):
            assert be._add_call_number() is False

        assert f"Unable to create call number for {library} b11111111a." in caplog.text

    @pytest.mark.parametrize(
        "resourceId,suppressed,libraryId,expectation",
        [
            pytest.param(1, False, 1, "*b2=z;", id="nyp-ebook"),
            pytest.param(1, False, 2, "*b2=x;", id="bpl-ebook"),
            pytest.param(1, True, 1, "*b2=z;b3=n;", id="nyp-ebook-supp"),
            pytest.param(1, True, 2, "*b2=x;b3=n;", id="bpl-ebook-supp"),
            pytest.param(2, False, 1, "*b2=n;", id="nyp-eaudio"),
            pytest.param(2, False, 2, "*b2=z;", id="bpl-eaudio"),
            pytest.param(3, True, 1, "*b2=3;b3=n;", id="nyp-evideo-supp"),
            pytest.param(3, True, 2, "*b2=v;b3=n;", id="bpl-evideo-supp"),
            pytest.param(4, False, 1, "*b2=a;", id="nyp-print"),
            pytest.param(4, False, 2, "*b2=a;", id="bpl-print"),
        ],
    )
    def test_add_command_tag(
        self, caplog, resourceId, suppressed, libraryId, expectation, stub_resource
    ):
        stub_resource.resourceCategoryId = resourceId
        stub_resource.suppressed = suppressed
        stub_resource.libraryId = libraryId
        if libraryId == 1:
            library = "NYP"
        elif libraryId == 2:
            library = "BPL"

        be = BibEnhancer(stub_resource)
        with caplog.at_level(logging.DEBUG):
            be._add_command_tag()

        assert (
            f"Added 949 command tag: {expectation} to {library} b11111111a."
            in caplog.text
        )

        bib = be.bib

        assert str(bib["949"]) == f"=949  \\\\$a{expectation}"

    @pytest.mark.parametrize(
        "res_cat_id, tag, indicators, subfields, log_msg",
        [
            pytest.param(
                1,
                None,
                [],
                [],
                "Added ebook genre 655 tag.",
                id="ebook: No previous tags",
            ),
            pytest.param(
                1,
                "650",
                [" ", "0"],
                ["a", "Electronic books."],
                "Added ebook genre 655 tag.",
                id="ebook: 650  0 $Electronic books.",
            ),
            pytest.param(
                2,
                "655",
                [" ", "0"],
                ["a", "Audiobooks."],
                "Added 2 eaudiobook genre 655 tags.",
                id="eaudio: 655  0 $a Audiobooks.",
            ),
            pytest.param(
                3,
                "655",
                [" ", "0"],
                ["a", "Internet videos."],
                "Added evideo genre 655 tag.",
                id="evideo: 650  0 $a Internet videos.",
            ),
            pytest.param(99, None, [], [], "", id="invalid resource category"),
        ],
    )
    def test_add_genre_tags(
        self, caplog, stub_resource, res_cat_id, tag, indicators, subfields, log_msg
    ):
        stub_resource.resourceCategoryId = res_cat_id
        be = BibEnhancer(stub_resource)

        if tag:
            be.bib.add_field(Field(tag=tag, indicators=indicators, subfields=subfields))

        with caplog.at_level(logging.DEBUG):
            be._add_genre_tags()

        assert log_msg in caplog.text

        if res_cat_id == 1:
            assert len(be.bib.get_fields("655")) == 1
            assert str(be.bib["655"]) == "=655  \\0$aElectronic books."
        elif res_cat_id == 2:
            tags = be.bib.get_fields("655")
            assert len(tags) == 2
            assert str(tags[0]) == "=655  \\7$aAudiobooks.$2lcgft"
            assert str(tags[1]) == "=655  \\7$aElectronic audiobooks.$2local"
        elif res_cat_id == 3:
            assert len(be.bib.get_fields("655")) == 1
            assert str(be.bib["655"]) == "=655  \\7$aInternet videos.$2lcgft"
        else:
            assert len(be.bib.get_fields("655")) == 0

    def test_add_local_tags(self, caplog, stub_resource):
        fields = [
            Field(tag="020", indicators=[" ", " "], subfields=["a", "978123456789x"]),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=["a", "123", "b", "Overdrive Inc."],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=["u", "url_here", "2", "opac msg"],
            ),
        ]
        pickled_fields = pickle.dumps(fields)
        stub_resource.srcFieldsToKeep = pickled_fields
        be = BibEnhancer(stub_resource)
        with caplog.at_level(logging.DEBUG):
            be._add_local_tags()
        assert (
            "Added following local fields ['020', '037', '856'] to NYP b11111111a."
            in caplog.text
        )

        bib = be.bib

        assert str(bib["020"]) == "=020  \\\\$a978123456789x"
        assert str(bib["037"]) == "=037  \\\\$a123$bOverdrive Inc."
        assert str(bib["856"]) == "=856  04$uurl_here$2opac msg"

    def test_add_local_tags_missing_tags(self, caplog, stub_resource):
        be = BibEnhancer(stub_resource)
        with caplog.at_level(logging.DEBUG):
            be._add_local_tags()

        assert "No local tags to keep were found for NYP b11111111a." in caplog.text

    @pytest.mark.parametrize(
        "library,tag",
        [
            ("NYP", "901"),
            ("BPL", "947"),
        ],
    )
    def test_add_initials_tag(self, caplog, library, tag, stub_resource):
        be = BibEnhancer(stub_resource)
        be.library = library
        with caplog.at_level(logging.DEBUG):
            be._add_initials_tag()

        assert f"Added initials tag {tag} to {library} b11111111a." in caplog.text
        assert str(be.bib[tag]) == f"={tag}  \\\\$a{__title__}/{__version__}"

    def test_add_initials_tag_invalid_library(self, stub_resource):
        be = BibEnhancer(stub_resource)
        bib_before = str(be.bib)
        be.library = "foo"
        be._add_initials_tag()
        assert str(be.bib) == bib_before

    @pytest.mark.parametrize(
        "library,tag,field_str",
        [
            ("NYP", "945", "=945  \\\\$a.b11111111a"),
            ("BPL", "907", "=907  \\\\$a.b11111111a"),
        ],
    )
    def test_add_sierraId(self, stub_resource, library, tag, field_str):
        be = BibEnhancer(stub_resource)
        be.library = library
        be._add_sierraId()
        assert str(be.bib[tag]) == field_str

    def test_digits_only_in_tag_001(self, stub_resource):
        be = BibEnhancer(stub_resource)
        be._digits_only_in_tag_001()
        assert be.bib["001"].data == "850939580"

    def test_is_acceptable_success(self, stub_resource):
        be = BibEnhancer(stub_resource)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(
                tag="245", indicators=["1", "0"], subfields=["a", "Foo /", "c", "Spam."]
            )
        )
        be.bib.add_field(
            Field(tag="300", indicators=[" ", " "], subfields=["a", "foo"])
        )
        assert be._is_acceptable() is True

    def test_is_acceptable_no_minimum_met(self, stub_resource):
        be = BibEnhancer(stub_resource)
        be.bib.remove_fields("300")

        assert be._is_acceptable() is False

    def test_is_acceptable_unable_to_create_call_number(self, stub_resource):
        stub_resource.resourceCategoryId = 99
        be = BibEnhancer(stub_resource)

        assert be._is_acceptable() is False

    def test_manipulate_failed(self, stub_resource, caplog):
        stub_resource.resourceCategoryId = 99

        with caplog.at_level(logging.INFO):
            be = BibEnhancer(stub_resource)
            be.manipulate()

            assert be.bib is None

        assert (
            "Worldcat record # 850939580 is rejected. Does not meet minimum requirements."
            in caplog.text
        )

    def test_manipulate_success(self, stub_resource, caplog):
        stub_resource.resourceCategoryId = 1
        stub_resource.libraryId = 1
        fields = [
            Field(tag="020", indicators=[" ", " "], subfields=["a", "978123456789x"]),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=["a", "123", "b", "Overdrive Inc."],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=["u", "url_here", "2", "opac msg"],
            ),
        ]
        pickled_fields = pickle.dumps(fields)
        stub_resource.srcFieldsToKeep = pickled_fields

        be = BibEnhancer(stub_resource)
        be.bib.remove_fields("245", "300")
        be.bib.add_field(
            Field(
                tag="245", indicators=["1", "0"], subfields=["a", "Foo /", "c", "Spam."]
            )
        )
        be.bib.add_field(
            Field(tag="300", indicators=[" ", " "], subfields=["a", "foo"])
        )
        with does_not_raise():
            with caplog.at_level(logging.INFO):
                be.manipulate()

                assert be.bib is not None

        assert "Worldcat record # 850939580 is acceptable. Meets minimum requirements."

        assert str(be.bib["020"]) == "=020  \\\\$a978123456789x"
        assert str(be.bib["037"]) == "=037  \\\\$a123$bOverdrive Inc."
        assert str(be.bib["856"]) == "=856  04$uurl_here$2opac msg"
        assert str(be.bib["091"]) == "=091  \\\\$aeNYPL Book"
        assert str(be.bib["901"]) == f"=901  \\\\$a{__title__}/{__version__}"
        assert str(be.bib["945"]) == "=945  \\\\$a.b11111111a"
        assert str(be.bib["949"]) == "=949  \\\\$a*b2=z;"

        # check if fields have been duplicated by accident
        assert len(be.bib.get_fields("001")) == 1
        assert len(be.bib.get_fields("091")) == 1
        assert len(be.bib.get_fields("037")) == 1

    def test_meets_minimum_criteria_success(self, stub_resource, caplog):
        be = BibEnhancer(stub_resource)
        be.bib.remove_fields("245", "300")
        be.bib.add_field(
            Field(
                tag="245", indicators=["1", "0"], subfields=["a", "Foo /", "c", "spam"]
            )
        )
        be.bib.add_field(
            Field(tag="300", indicators=[" ", " "], subfields=["a", "foo"])
        )
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is True

        assert "Worldcat record meets minimum criteria." in caplog.text

    def test_meets_minimum_criteria_upper_case_title(self, stub_resource, caplog):
        be = BibEnhancer(stub_resource)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(
                tag="245", indicators=["1", "0"], subfields=["a", "FOO /", "c", "spam"]
            )
        )
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed uppercase title test." in caplog.text

    def test_meets_minimum_criteria_statement_of_responsibility(
        self, stub_resource, caplog
    ):
        be = BibEnhancer(stub_resource)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(tag="245", indicators=["1", "0"], subfields=["a", "Foo."])
        )
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed statement of resp. test." in caplog.text

    def test_meets_minimum_criteria_physical_desc(self, stub_resource, caplog):
        be = BibEnhancer(stub_resource)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(
                tag="245", indicators=["1", "0"], subfields=["a", "Foo /", "c", "Spam."]
            )
        )
        be.bib.remove_fields("300")
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed physical desc. test." in caplog.text

    def test_purge_tags(self, caplog, stub_resource):
        be = BibEnhancer(stub_resource)
        fields = [
            Field(tag="020", indicators=[" ", " "], subfields=["a", "978123456789x"]),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=["a", "123", "b", "Overdrive Inc."],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=["u", "url_here", "2", "opac msg"],
            ),
        ]
        for field in fields:
            be.bib.add_field(field)

        # make sure added tags are present
        for field in fields:
            assert field.tag in be.bib

        with caplog.at_level(logging.DEBUG):
            be._purge_tags()

        assert (
            "Removed ['020', '029', '037', '090', '263', '856', '910', '938'] from NYP b11111111a."
            in caplog.text
        )

        # test if they were removed
        for field in fields:
            assert field.tag not in be.bib

    def test_purge_tags_non_existent(self, stub_resource):
        be = BibEnhancer(stub_resource)
        with does_not_raise():
            be._purge_tags()

    @pytest.mark.parametrize(
        "vendor",
        ["Overdrive, Inc.", "3M Company", "Recorded Books, Inc", "CloudLibrary"],
    )
    def test_remove_eresource_vendors(self, stub_resource, vendor):
        be = BibEnhancer(stub_resource)
        be.bib.add_field(
            Field(tag="710", indicators=[" ", "0"], subfields=["a", vendor])
        )
        be._remove_eresource_vendors()

        assert len(be.bib.get_fields("710")) == 0

    @pytest.mark.parametrize("arg", ["ocm12345", "ocn12345", "on12345", "12345"])
    def test_remove_oclc_prefix(self, arg, stub_resource):
        be = BibEnhancer(stub_resource)
        assert be._remove_oclc_prefix(arg) == "12345"

    @pytest.mark.parametrize(
        "tag,expectation",
        [
            pytest.param(
                Field(tag="650", indicators=[" ", "0"], subfields=["a", "Foo."]),
                1,
                id="LCSH",
            ),
            pytest.param(
                Field(tag="690", indicators=[" ", "0"], subfields=["a", "Foo."]),
                0,
                id="local SH",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "7"],
                    subfields=["a", "Foo.", "2", "lcsh"],
                ),
                1,
                id="LCSH subfield $2 7",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=["a", "Foo.", "2", "fast"],
                ),
                1,
                id="FAST",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "7"],
                    subfields=["a", "Foo.", "2", "homoit"],
                ),
                1,
                id="HOMOIT",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=["a", "Foo.", "2", "gsafd"],
                ),
                1,
                id="GSAFD",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=["a", "Foo.", "2", "lcgft"],
                ),
                1,
                id="LCGFT",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=["a", "Foo.", "2", "lctgm"],
                ),
                1,
                id="LCTGM",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "7"],
                    subfields=["a", "Foo.", "2", "sears"],
                ),
                0,
                id="Other dict",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "4"],
                    subfields=["a", "Foo.", "2", "lcsh"],
                ),
                0,
                id="2nd ind = 4",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "1"],
                    subfields=["a", "Foo."],
                ),
                0,
                id="Children's LCSH",
            ),
        ],
    )
    def test_remove_unsupported_subject_tags(self, stub_resource, tag, expectation):
        be = BibEnhancer(stub_resource)

        # prep - remove any existing tags for tests
        for f in be.bib.subjects():
            be.bib.remove_field(f)

        assert len(be.bib.subjects()) == 0

        be.bib.add_field(tag)
        be._remove_unsupported_subject_tags()

        assert len(be.bib.subjects()) == expectation

    def test_save2file(self, caplog, stub_resource):
        be = BibEnhancer(stub_resource)
        with caplog.at_level(logging.DEBUG):
            be.save2file()
        assert "Saving to file NYP record b11111111a." in caplog.text

        assert os.path.exists("temp.mrc")
        with open("temp.mrc", "rb") as f:
            reader = MARCReader(f)
            bib = next(reader)
            assert isinstance(bib, Record)

        # cleanup
        os.remove("temp.mrc")

    def test_save2file_os_error(self, caplog, stub_resource, mock_os_error):
        be = BibEnhancer(stub_resource)
        with caplog.at_level(logging.ERROR):
            with pytest.raises(OSError):
                be.save2file()

        assert "Unable to save record to a temp file. Error" in caplog.text

    def test_save2file_when_unable_to_create_call_number(
        self, caplog, stub_resource, tmpdir
    ):
        outfile = tmpdir.join("foo.mrc")
        stub_resource.resourceCategoryId = 99
        be = BibEnhancer(stub_resource)
        with caplog.at_level(logging.WARNING):
            be.manipulate()
            be.save2file(outfile)

        assert "No pymarc object to serialize to MARC21" in caplog.text
