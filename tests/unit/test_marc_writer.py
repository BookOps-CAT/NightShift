# -*- coding: utf-8 -*-

"""
Tests `marc.marc_writer.py` module
"""
from contextlib import nullcontext as does_not_raise
import logging
import os
import pickle

from pymarc import Field, MARCReader, Record, Subfield
import pytest

from nightshift import __title__, __version__
from nightshift.datastore import Resource
from nightshift.marc.marc_writer import BibEnhancer


class TestBibEnhancer:
    def test_init(self, caplog, stub_resource, stub_res_cat_by_id):
        with does_not_raise():
            with caplog.at_level(logging.INFO):
                result = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)

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
        self,
        caplog,
        stub_resource,
        stub_res_cat_by_id,
        resourceId,
        libraryId,
        tag,
        expectation,
    ):
        stub_resource.resourceCategoryId = resourceId
        stub_resource.libraryId = libraryId
        if libraryId == 1:
            library = "NYP"
        elif libraryId == 2:
            library = "BPL"
        be = BibEnhancer(stub_resource, library, stub_res_cat_by_id)
        with caplog.at_level(logging.DEBUG):
            assert be._add_call_number() is True
        assert f"Added {expectation} to {library} b11111111a." in caplog.text
        bib = be.bib
        assert str(bib[tag]) == f"={tag}  \\\\$a{expectation}"

    @pytest.mark.parametrize("library,resourceId", [("QPL", 1), ("NYP", 4), ("BPL", 4)])
    def test_add_call_number_unsupported_resources(
        self, caplog, stub_resource, stub_res_cat_by_id, library, resourceId
    ):
        stub_resource.resourceCategoryId = resourceId
        be = BibEnhancer(stub_resource, library, stub_res_cat_by_id)

        with caplog.at_level(logging.WARN):
            assert be._add_call_number() is False

        assert f"Unable to create call number for {library} b11111111a." in caplog.text

    @pytest.mark.parametrize(
        "resourceId,suppressed,libraryId,expectation",
        [
            pytest.param(1, False, 1, "*b2=z;bn=ia;", id="nyp-ebook"),
            pytest.param(1, False, 2, "*b2=x;bn=elres;", id="bpl-ebook"),
            pytest.param(1, True, 1, "*b2=z;b3=n;bn=ia;", id="nyp-ebook-supp"),
            pytest.param(1, True, 2, "*b2=x;b3=n;bn=elres;", id="bpl-ebook-supp"),
            pytest.param(2, False, 1, "*b2=n;bn=ia;", id="nyp-eaudio"),
            pytest.param(2, False, 2, "*b2=z;bn=elres;", id="bpl-eaudio"),
            pytest.param(3, True, 1, "*b2=3;b3=n;bn=ia;", id="nyp-evideo-supp"),
            pytest.param(3, True, 2, "*b2=v;b3=n;bn=elres;", id="bpl-evideo-supp"),
            pytest.param(4, False, 1, "*b2=a;bn=ia;", id="nyp-print"),
            pytest.param(4, False, 2, "*b2=a;bn=elres;", id="bpl-print"),
        ],
    )
    def test_add_command_tag(
        self,
        caplog,
        stub_resource,
        stub_res_cat_by_id,
        resourceId,
        suppressed,
        libraryId,
        expectation,
    ):
        stub_resource.resourceCategoryId = resourceId
        stub_resource.suppressed = suppressed
        stub_resource.libraryId = libraryId
        if libraryId == 1:
            library = "NYP"
        elif libraryId == 2:
            library = "BPL"

        be = BibEnhancer(stub_resource, library, stub_res_cat_by_id)
        with caplog.at_level(logging.DEBUG):
            be._add_command_tag()

        assert (
            f"Added 949 command tag: {expectation} to {library} b11111111a."
            in caplog.text
        )

        bib = be.bib

        assert str(bib["949"]) == f"=949  \\\\$a{expectation}"

    @pytest.mark.parametrize(
        "res_cat_id, tag, indicators, subfields, log_msgs",
        [
            pytest.param(
                1,
                None,
                [],
                [],
                [],
                id="ebook: No previous tags",
            ),
            pytest.param(
                2,
                None,
                [],
                [],
                [
                    "Added 'Audiobooks' LCGFT genre to 655 tag.",
                ],
                id="eaudio: No previous tags",
            ),
            pytest.param(
                3,
                None,
                [],
                [],
                ["Added 'Internet videos' LCGFT genre to 655 tag."],
                id="evideo: No previous tags",
            ),
            pytest.param(
                99,
                None,
                [],
                [],
                [],
                id="illegal resource category",
            ),
        ],
    )
    def test_clean_up_genre_tags_when_missing(
        self,
        caplog,
        stub_resource,
        stub_res_cat_by_id,
        res_cat_id,
        tag,
        indicators,
        subfields,
        log_msgs,
    ):
        stub_resource.resourceCategoryId = res_cat_id
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)

        if tag:
            be.bib.add_field(Field(tag=tag, indicators=indicators, subfields=subfields))

        with caplog.at_level(logging.DEBUG):
            be._clean_up_genre_tags()

        for log_msg in log_msgs:
            assert log_msg in caplog.text

        if res_cat_id == 1:
            assert len(be.bib.get_fields("655")) == 0
        elif res_cat_id == 2:
            tags = be.bib.get_fields("655")
            assert len(tags) == 1
            assert str(tags[0]) == "=655  \\7$aAudiobooks.$2lcgft"
        elif res_cat_id == 3:
            assert len(be.bib.get_fields("655")) == 1
            assert str(be.bib["655"]) == "=655  \\7$aInternet videos.$2lcgft"
        else:
            assert len(be.bib.get_fields("655")) == 0

    @pytest.mark.parametrize(
        "res_cat_id,tag,indicators,subfields",
        [
            pytest.param(
                1,
                "655",
                [" ", "0"],
                [Subfield("a", "Electronic books.")],
                id="ebook: Electronic books - lcsh",
            ),
            pytest.param(
                1,
                "655",
                [" ", "7"],
                [Subfield("a", "Electronic books."), Subfield("2", "lcgft")],
                id="ebook: Electronic books - lcgft",
            ),
            pytest.param(
                1,
                "650",
                [" ", "0"],
                [Subfield("a", "Electronic books.")],
                id="ebook: Electronic books as invalid LCSH",
            ),
            pytest.param(
                1,
                "655",
                [" ", "0"],
                [Subfield("a", "Children's electronic books.")],
                id="ebook: Children's electronic books.",
            ),
            pytest.param(
                2,
                "655",
                [" ", "7"],
                [Subfield("a", "Audiobooks."), Subfield("2", "lcgft")],
                id="eaudio: Audiobooks - lcgft",
            ),
            pytest.param(
                2,
                "655",
                [" ", "7"],
                [Subfield("a", "Chidlren's Audiobooks."), Subfield("2", "lcgft")],
                id="eaudio: Children's audiobooks - lcgft",
            ),
            pytest.param(
                2,
                "655",
                [" ", "7"],
                [Subfield("a", "Electronic audiobooks."), Subfield("2", "local")],
                id="eaudio: Electronic audiobooks - local",
            ),
            pytest.param(
                2,
                "650",
                [" ", "0"],
                [Subfield("a", "Electronic audiobooks.")],
                id="eaudio: Electronic audiobooks as invalid LCSH",
            ),
            pytest.param(
                3,
                "655",
                [" ", "7"],
                [Subfield("a", "Internet videos."), Subfield("2", "lcgft")],
                id="evideo: Internet videos - lcgft.",
            ),
        ],
    )
    def test_clean_up_genre_tags_when_present(
        self, stub_resource, stub_res_cat_by_id, res_cat_id, tag, indicators, subfields
    ):
        stub_resource.resourceCategoryId = res_cat_id
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)

        # make sure no other subjects interfere
        be.bib.remove_fields("650", "655")
        be.bib.add_field(Field(tag=tag, indicators=indicators, subfields=subfields))

        be._clean_up_genre_tags()

        if (
            res_cat_id == 1
        ):  # remove, OCLC no longer allows (Sept. 2022) use of 'electronic books' genre terms - so this should technically not occur any more
            assert len(be.bib.subjects) == 0
        elif res_cat_id == 2:
            assert len(be.bib.subjects) == 1
            assert "audiobooks. lcgft" in be.bib["655"].value().lower()
        elif res_cat_id == 3:
            assert len(be.bib.subjects) == 1
            assert str(be.bib["655"]) == "=655  \\7$aInternet videos.$2lcgft"

    def test_add_local_tags(self, caplog, stub_resource, stub_res_cat_by_id):
        fields = [
            Field(
                tag="020",
                indicators=[" ", " "],
                subfields=[Subfield("a", "978123456789x")],
            ),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=[Subfield("a", "123"), Subfield("b", "Overdrive Inc.")],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=[Subfield("u", "url_here"), Subfield("2", "opac msg")],
            ),
        ]
        pickled_fields = pickle.dumps(fields)
        stub_resource.srcFieldsToKeep = pickled_fields
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
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

    def test_add_local_tags_missing_tags(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
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
    def test_add_initials_tag(
        self,
        caplog,
        stub_resource,
        stub_res_cat_by_id,
        library,
        tag,
    ):
        be = BibEnhancer(stub_resource, library, stub_res_cat_by_id)
        with caplog.at_level(logging.DEBUG):
            be._add_initials_tag()

        assert f"Added initials tag {tag} to {library} b11111111a." in caplog.text
        assert str(be.bib[tag]) == f"={tag}  \\\\$a{__title__}/{__version__}"

    def test_add_initials_tag_invalid_library(self, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "foo", stub_res_cat_by_id)
        bib_before = str(be.bib)
        be._add_initials_tag()
        assert str(be.bib) == bib_before

    @pytest.mark.parametrize(
        "library,tag,field_str",
        [
            ("NYP", "945", "=945  \\\\$a.b11111111a"),
            ("BPL", "907", "=907  \\\\$a.b11111111a"),
        ],
    )
    def test_add_sierraId(
        self, stub_resource, stub_res_cat_by_id, library, tag, field_str
    ):
        be = BibEnhancer(stub_resource, library, stub_res_cat_by_id)
        be._add_sierraId()
        assert str(be.bib[tag]) == field_str

    def test_digits_only_in_tag_001(self, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be._digits_only_in_tag_001()
        assert be.bib["001"].data == "850939580"

    def test_is_acceptable_success(self, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(
                tag="245",
                indicators=["1", "0"],
                subfields=[Subfield("a", "Foo /"), Subfield("c", "Spam.")],
            )
        )
        be.bib.add_field(
            Field(tag="300", indicators=[" ", " "], subfields=[Subfield("a", "foo")])
        )
        assert be._is_acceptable() is True

    def test_is_acceptable_no_minimum_met(self, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "BPL", stub_res_cat_by_id)
        be.bib.remove_fields("300")

        assert be._is_acceptable() is False

    def test_is_acceptable_unable_to_create_call_number(
        self, stub_resource, stub_res_cat_by_id
    ):
        stub_resource.resourceCategoryId = 99
        be = BibEnhancer(stub_resource, "BPL", stub_res_cat_by_id)

        assert be._is_acceptable() is False

    def test_manipulate_failed(self, caplog, stub_resource, stub_res_cat_by_id):
        stub_resource.resourceCategoryId = 99

        with caplog.at_level(logging.INFO):
            be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
            be.manipulate()

            assert be.bib is None

        assert (
            "Worldcat record # 850939580 is rejected. Does not meet minimum requirements."
            in caplog.text
        )

    def test_manipulate_success(self, caplog, stub_resource, stub_res_cat_by_id):
        stub_resource.resourceCategoryId = 1
        stub_resource.libraryId = 1
        fields = [
            Field(
                tag="020",
                indicators=[" ", " "],
                subfields=[Subfield("a", "978123456789x")],
            ),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=[Subfield("a", "123"), Subfield("b", "Overdrive Inc.")],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=[Subfield("u", "url_here"), Subfield("2", "opac msg")],
            ),
        ]
        pickled_fields = pickle.dumps(fields)
        stub_resource.srcFieldsToKeep = pickled_fields

        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("245", "300")
        be.bib.add_field(
            Field(
                tag="245",
                indicators=["1", "0"],
                subfields=[Subfield("a", "Foo /"), Subfield("c", "Spam.")],
            )
        )
        be.bib.add_field(
            Field(tag="300", indicators=[" ", " "], subfields=[Subfield("a", "foo")])
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
        assert str(be.bib["949"]) == "=949  \\\\$a*b2=z;bn=ia;"

        # check if fields have been duplicated by accident
        assert len(be.bib.get_fields("001")) == 1
        assert len(be.bib.get_fields("091")) == 1
        assert len(be.bib.get_fields("037")) == 1

    def test_meets_minimum_criteria_success(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("245", "300")
        be.bib.add_field(
            Field(
                tag="245",
                indicators=["1", "0"],
                subfields=[Subfield("a", "Foo /"), Subfield("c", "spam")],
            )
        )
        be.bib.add_field(
            Field(tag="300", indicators=[" ", " "], subfields=[Subfield("a", "foo")])
        )
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is True

        assert "Worldcat record meets minimum criteria." in caplog.text

    def test_meets_minimum_criteria_upper_case_title(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(
                tag="245",
                indicators=["1", "0"],
                subfields=[Subfield("a", "FOO /"), Subfield("c", "spam")],
            )
        )
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed uppercase title test." in caplog.text

    def test_meets_minimum_criteria_statement_of_responsibility(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(tag="245", indicators=["1", "0"], subfields=[Subfield("a", "Foo.")])
        )
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed statement of resp. test." in caplog.text

    def test_meets_minimum_criteria_physical_desc(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(
                tag="245",
                indicators=["1", "0"],
                subfields=[Subfield("a", "Foo /"), Subfield("c", "Spam.")],
            )
        )
        be.bib.remove_fields("300")
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed physical desc. test." in caplog.text

    @pytest.mark.parametrize(
        "tag,value,expectation, msg",
        [
            pytest.param(
                "100",
                "spam",
                True,
                "Worldcat record meets minimum criteria.",
                id="meets in 100",
            ),
            pytest.param(
                "245",
                "spam",
                True,
                "Worldcat record meets minimum criteria.",
                id="meets in 245",
            ),
            pytest.param(
                "100",
                None,
                True,
                "Worldcat record meets minimum criteria.",
                id="no author field",
            ),
            pytest.param(
                "100",
                "℗",
                False,
                "Worldcat record failed characters encoding test.",
                id="prod symbol in 100",
            ),
            pytest.param(
                "245",
                "℗",
                False,
                "Worldcat record failed characters encoding test.",
                id="prod symbol in 245",
            ),
            pytest.param(
                "100",
                "©",
                False,
                "Worldcat record failed characters encoding test.",
                id="copyright symbol in 100",
            ),
            pytest.param(
                "245",
                "©",
                False,
                "Worldcat record failed characters encoding test.",
                id="copyright symbol in 245",
            ),
        ],
    )
    def test_meets_minimum_criteria_diacritics_copyright_symbol(
        self, caplog, stub_resource, stub_res_cat_by_id, tag, value, expectation, msg
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields(tag)
        if value:
            be.bib.add_field(
                Field(
                    tag=tag,
                    subfields=[
                        Subfield("a", "Foo "),
                        Subfield("b", value),
                        Subfield("c", "bar"),
                    ],
                )
            )
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() == expectation

        assert msg in caplog.text

    def test_meets_minimum_criteria_no_subject_tags(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("650")
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed subjects test." in caplog.text

    def test_purge_tags(self, caplog, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        fields = [
            Field(
                tag="020",
                indicators=[" ", " "],
                subfields=[Subfield("a", "978123456789x")],
            ),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=[Subfield("a", "123"), Subfield("b", "Overdrive Inc.")],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=[Subfield("u", "url_here"), Subfield("2", "opac msg")],
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

    def test_purge_tags_non_existent(self, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "BPL", stub_res_cat_by_id)
        with does_not_raise():
            be._purge_tags()

    @pytest.mark.parametrize(
        "vendor",
        ["Overdrive, Inc.", "3M Company", "Recorded Books, Inc", "CloudLibrary"],
    )
    def test_remove_eresource_vendors(self, stub_resource, stub_res_cat_by_id, vendor):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.add_field(
            Field(tag="710", indicators=[" ", "0"], subfields=[Subfield("a", vendor)])
        )
        be._remove_eresource_vendors()

        assert len(be.bib.get_fields("710")) == 0

    @pytest.mark.parametrize("arg", ["ocm12345", "ocn12345", "on12345", "12345"])
    def test_remove_oclc_prefix(self, stub_resource, stub_res_cat_by_id, arg):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        assert be._remove_oclc_prefix(arg) == "12345"

    def test_remove_unsupported_local_genre_tag_electronic_books(
        self, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)

        # prep - remove any existing tags for tests
        for f in be.bib.subjects:
            be.bib.remove_field(f)

        assert len(be.bib.subjects) == 0

        be.bib.add_field(
            Field(
                tag="655",
                indicators=[" ", "4"],
                subfields=[Subfield("a", "Electronic books.")],
            )
        )
        # must add additional 650 to pass minimium requirments
        be.bib.add_field(
            Field(tag="650", indicators=[" ", "0"], subfields=[Subfield("a", "Foo.")])
        )

        assert len(be.bib.subjects) == 2
        be.manipulate()
        assert len(be.bib.subjects) == 1
        assert str(be.bib.subjects[0]) == "=650  \\0$aFoo."

    @pytest.mark.parametrize(
        "tag",
        [
            pytest.param(
                Field(
                    tag="650", indicators=[" ", "0"], subfields=[Subfield("a", "Foo.")]
                ),
                id="LCSH",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcsh")],
                ),
                id="LCSH subfield $2 7",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "fast")],
                ),
                id="FAST",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "homoit")],
                ),
                id="HOMOIT",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gsafd")],
                ),
                id="GSAFD",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcgft")],
                ),
                id="LCGFT",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lctgm")],
                ),
                id="LCTGM",
            ),
        ],
    )
    def test_remove_unsupported_subject_tags_good_terms(
        self, stub_resource, stub_res_cat_by_id, tag
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)

        # prep - remove any existing tags for tests
        for f in be.bib.subjects:
            be.bib.remove_field(f)

        assert len(be.bib.subjects) == 0

        be.bib.add_field(tag)

        be.manipulate()
        assert len(be.bib.subjects) == 1
        assert str(be.bib.subjects[0]) == str(tag)

    @pytest.mark.parametrize(
        "tag",
        [
            pytest.param(
                Field(
                    tag="690", indicators=[" ", "0"], subfields=[Subfield("a", "Foo.")]
                ),
                id="local SH",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gmgpc")],
                ),
                id="GMGPC",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "7"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "sears")],
                ),
                id="Other dict: sears",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "4"],
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcsh")],
                ),
                id="2nd ind = 4",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=[" ", "1"],
                    subfields=[Subfield("a", "Foo.")],
                ),
                id="Children's LCSH",
            ),
            pytest.param(
                Field(
                    tag="650", indicators=[" ", "7"], subfields=[Subfield("a", "Foo.")]
                ),
                id="Incomplete field for other dict",
            ),
        ],
    )
    def test_remove_unsupported_subject_tags_unwanted_terms(
        self, stub_resource, stub_res_cat_by_id, tag
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)

        # prep - remove any existing tags for tests
        for f in be.bib.subjects:
            be.bib.remove_field(f)

        assert len(be.bib.subjects) == 0

        be.bib.add_field(tag)
        be.bib.add_field(
            Field(tag="650", indicators=[" ", "0"], subfields=[Subfield("a", "Spam.")])
        )

        be.manipulate()
        assert len(be.bib.subjects) == 1
        assert str(be.bib.subjects[0]) == "=650  \\0$aSpam."

    def test_save2file(self, caplog, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
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

    def test_save2file_os_error(
        self, caplog, stub_resource, stub_res_cat_by_id, mock_os_error
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        with caplog.at_level(logging.ERROR):
            with pytest.raises(OSError):
                be.save2file()

        assert "Unable to save record to a temp file. Error" in caplog.text

    def test_save2file_when_unable_to_create_call_number(
        self, caplog, tmpdir, stub_resource, stub_res_cat_by_id
    ):
        outfile = tmpdir.join("foo.mrc")
        stub_resource.resourceCategoryId = 99
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        with caplog.at_level(logging.WARNING):
            be.manipulate()
            be.save2file(outfile)

        assert "No pymarc object to serialize to MARC21" in caplog.text
