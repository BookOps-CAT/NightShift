"""
Tests `marc.marc_writer.py` module
"""

import copy
import logging
import os
import pickle

import pytest
from pymarc import Field, Indicators, MARCReader, Record, Subfield

from nightshift.marc.marc_writer import BibEnhancer


@pytest.fixture
def res_to_enhance(resourceId, library, suppressed, stub_resource):
    resource = copy.copy(stub_resource)
    if library == "NYP":
        resource.libraryId = 1
    else:
        resource.libraryId = 2
    resource.resourceCategoryId = resourceId
    resource.suppressed = suppressed
    return resource


class TestBibEnhancer:
    @pytest.mark.parametrize(
        "resourceId,library,call_no,command,suppressed",
        [
            pytest.param(1, "NYP", "eNYPL Book", "*b2=z;bn=ia;", False, id="nyp-ebook"),
            pytest.param(1, "BPL", "eBOOK", "*b2=x;bn=elres;", False, id="bpl-ebook"),
            pytest.param(
                1, "NYP", "eNYPL Book", "*b2=z;b3=n;bn=ia;", True, id="nyp-ebook-supp"
            ),
            pytest.param(
                1, "BPL", "eBOOK", "*b2=x;b3=n;bn=elres;", True, id="bpl-ebook-supp"
            ),
        ],
    )
    def test_manipulate_res_cat_1(
        self, caplog, res_to_enhance, stub_res_cat_by_id, library, call_no, command
    ):
        be = BibEnhancer(res_to_enhance, library, stub_res_cat_by_id)
        assert be.bib["001"].value() == "ocn850939580"
        be.manipulate()
        call_tag = be.tags["call_tag"]
        initials_tag = be.tags["initials_tag"]
        log_msgs = [i.msg for i in caplog.records]
        assert len(log_msgs) == 7
        assert log_msgs[0] == "Converting Worldcat response to bookops-marc Bib object."
        assert (
            log_msgs[1]
            == f"Removed ['020', '029', '037', '090', '263', '856', '910', '938'] from {library} b11111111a."
        )
        assert log_msgs[2] == f"Added {call_no} to {library} b11111111a."
        assert (
            log_msgs[3]
            == "Worldcat record # 850939580 is acceptable. Meets minimum requirements."
        )

        assert (
            log_msgs[4] == f"No local tags to keep were found for {library} b11111111a."
        )
        assert (
            log_msgs[5] == f"Added 949 command tag: {command} to {library} b11111111a."
        )
        assert (
            log_msgs[6] == f"Added initials tag {initials_tag} to {library} b11111111a."
        )
        assert str(be.bib[call_tag]) == f"={call_tag}  \\\\$a{call_no}"

    @pytest.mark.parametrize(
        "field,count",
        [
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Electronic books.")],
                ),
                0,
                id="ebook: Electronic books - lcsh",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[
                        Subfield("a", "Electronic books."),
                        Subfield("2", "lcgft"),
                    ],
                ),
                0,
                id="ebook: Electronic books - lcgft",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Electronic books.")],
                ),
                0,
                id="ebook: Electronic books as invalid LCSH",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Children's electronic books.")],
                ),
                0,
                id="ebook: Children's electronic books.",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                1,
                id="LCSH",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcsh")],
                ),
                1,
                id="LCSH subfield $2 7",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "fast")],
                ),
                1,
                id="FAST",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "homoit")],
                ),
                1,
                id="HOMOIT",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gsafd")],
                ),
                1,
                id="GSAFD",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcgft")],
                ),
                1,
                id="LCGFT",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lctgm")],
                ),
                1,
                id="LCTGM",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "library,resourceId,suppressed", [("NYP", 1, False), ("BPL", 1, False)]
    )
    def test_manipulate_res_cat_1_genre_tags(
        self, res_to_enhance, library, stub_res_cat_by_id, field, count
    ):
        be = BibEnhancer(res_to_enhance, library, stub_res_cat_by_id)
        be.bib.remove_fields("650", "655")
        be.bib.add_field(field)
        assert len(be.bib.subjects) == 1
        be.manipulate()
        assert len(be.bib.subjects) == count

    @pytest.mark.parametrize(
        "resourceId,library,call_no,command,subj,suppressed",
        [
            pytest.param(
                2,
                "NYP",
                "eNYPL Audio",
                "*b2=n;bn=ia;",
                "Audiobooks",
                False,
                id="nyp-eaudio",
            ),
            pytest.param(
                2,
                "BPL",
                "eAUDIO",
                "*b2=z;bn=elres;",
                "Audiobooks",
                False,
                id="bpl-eaudio",
            ),
            pytest.param(
                2,
                "NYP",
                "eNYPL Audio",
                "*b2=n;b3=n;bn=ia;",
                "Audiobooks",
                True,
                id="nyp-eaudio-supp",
            ),
            pytest.param(
                2,
                "BPL",
                "eAUDIO",
                "*b2=z;b3=n;bn=elres;",
                "Audiobooks",
                True,
                id="bpl-eaudio0-supp",
            ),
            pytest.param(
                3,
                "NYP",
                "eNYPL Video",
                "*b2=3;bn=ia;",
                "Internet videos",
                False,
                id="nyp-evideo",
            ),
            pytest.param(
                3,
                "BPL",
                "eVIDEO",
                "*b2=v;bn=elres;",
                "Internet videos",
                False,
                id="bpl-video",
            ),
            pytest.param(
                3,
                "NYP",
                "eNYPL Video",
                "*b2=3;b3=n;bn=ia;",
                "Internet videos",
                True,
                id="nyp-video-supp",
            ),
            pytest.param(
                3,
                "BPL",
                "eVIDEO",
                "*b2=v;b3=n;bn=elres;",
                "Internet videos",
                True,
                id="bpl-video-supp",
            ),
        ],
    )
    def test_manipulate_res_cat_2_3(
        self,
        caplog,
        res_to_enhance,
        stub_res_cat_by_id,
        library,
        call_no,
        command,
        subj,
    ):
        be = BibEnhancer(res_to_enhance, library, stub_res_cat_by_id)
        be.manipulate()
        call_tag = be.tags["call_tag"]
        initials_tag = be.tags["initials_tag"]
        log_msgs = [i.msg for i in caplog.records]
        assert len(log_msgs) == 8
        assert log_msgs[0] == "Converting Worldcat response to bookops-marc Bib object."
        assert (
            log_msgs[1]
            == f"Removed ['020', '029', '037', '090', '263', '856', '910', '938'] from {library} b11111111a."
        )
        assert log_msgs[2] == f"Added {call_no} to {library} b11111111a."
        assert (
            log_msgs[3]
            == "Worldcat record # 850939580 is acceptable. Meets minimum requirements."
        )
        assert log_msgs[4] == f"Added '{subj}' LCGFT genre to 655 tag."
        assert (
            log_msgs[5] == f"No local tags to keep were found for {library} b11111111a."
        )
        assert (
            log_msgs[6] == f"Added 949 command tag: {command} to {library} b11111111a."
        )
        assert (
            log_msgs[7] == f"Added initials tag {initials_tag} to {library} b11111111a."
        )
        assert str(be.bib[call_tag]) == f"={call_tag}  \\\\$a{call_no}"

    @pytest.mark.parametrize(
        "field,suppressed,resourceId,subj",
        [
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Audiobooks."), Subfield("2", "lcgft")],
                ),
                False,
                2,
                "Audiobooks. lcgft",
                id="eaudio: Audiobooks - lcgft",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[
                        Subfield("a", "Children's Audiobooks."),
                        Subfield("2", "lcgft"),
                    ],
                ),
                False,
                2,
                "Children's Audiobooks. lcgft",
                id="eaudio: Children's audiobooks - lcgft",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[
                        Subfield("a", "Electronic audiobooks."),
                        Subfield("2", "local"),
                    ],
                ),
                False,
                2,
                "Audiobooks. lcgft",
                id="eaudio: Electronic audiobooks - local",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Electronic audiobooks.")],
                ),
                False,
                2,
                "Audiobooks. lcgft",
                id="eaudio: Electronic audiobooks as invalid LCSH",
            ),
            pytest.param(
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[
                        Subfield("a", "Internet videos."),
                        Subfield("2", "lcgft"),
                    ],
                ),
                False,
                3,
                "Internet videos. lcgft",
                id="evideo: Internet videos - lcgft.",
            ),
        ],
    )
    @pytest.mark.parametrize("library", ["NYP", "BPL"])
    def test_manipulate_res_cat_2_3_genre_tags(
        self, res_to_enhance, library, stub_res_cat_by_id, field, subj
    ):
        be = BibEnhancer(res_to_enhance, library, stub_res_cat_by_id)
        assert len(be.bib.subjects) == 1
        assert be.bib.subjects[0].value() == "Test."
        be.bib.remove_fields("650", "655")
        be.bib.add_field(field)
        be.manipulate()
        assert len(be.bib.subjects) == 1
        assert be.bib.subjects[0].value() == subj

    @pytest.mark.parametrize(
        "resourceId,library",
        [
            pytest.param(4, "NYP", id="nyp-print-eng-adult-fic"),
            pytest.param(4, "BPL", id="bpl-print-eng-adult-fic"),
            pytest.param(5, "NYP", id="nyp-print-eng-adult-bio"),
            pytest.param(5, "BPL", id="bpl-print-eng-adult-bio"),
            pytest.param(6, "NYP", id="nyp-print-eng-adult-nonfic"),
            pytest.param(6, "BPL", id="bpl-print-eng-adult-nonfic"),
            pytest.param(7, "NYP", id="nyp-print-eng-adult-mystery"),
            pytest.param(7, "BPL", id="bpl-print-eng-adult-mystery"),
            pytest.param(8, "NYP", id="nyp-print-eng-adult-scifi"),
            pytest.param(8, "BPL", id="bpl-print-eng-adult-scifi"),
            pytest.param(9, "NYP", id="nyp-print-eng-juv-fic"),
            pytest.param(9, "BPL", id="bpl-print-eng-juv-fic"),
            pytest.param(10, "NYP", id="nyp-print-eng-juv-bio"),
            pytest.param(10, "BPL", id="bpl-print-eng-juv-bio"),
            pytest.param(11, "NYP", id="nyp-print-eng-juv-nonfic"),
            pytest.param(11, "BPL", id="bpl-print-eng-juv-nonfic"),
        ],
    )
    @pytest.mark.parametrize("suppressed", [False])
    def test_manipulate_res_cat_4_11(
        self, caplog, res_to_enhance, stub_res_cat_by_id, library
    ):
        be = BibEnhancer(res_to_enhance, library, stub_res_cat_by_id)
        be.manipulate()
        log_msgs = [i.msg for i in caplog.records]
        assert len(log_msgs) == 4
        assert log_msgs[0] == "Converting Worldcat response to bookops-marc Bib object."
        assert (
            log_msgs[1]
            == f"Removed ['029', '090', '263', '936', '938'] from {library} b11111111a."
        )
        assert log_msgs[2] == f"Unable to create call number for {library} b11111111a."
        assert (
            log_msgs[3]
            == "Worldcat record # 850939580 is rejected. Does not meet minimum requirements."
        )

    @pytest.mark.parametrize(
        "resourceId,library",
        [
            pytest.param(99, "NYP", id="nyp-invalid-res-cat"),
            pytest.param(99, "BPL", id="bpl-invalid-res-cat"),
        ],
    )
    @pytest.mark.parametrize("suppressed", [False, True])
    def test_manipulate_res_cat_invalid(
        self, caplog, res_to_enhance, stub_res_cat_by_id, library
    ):
        be = BibEnhancer(res_to_enhance, library, stub_res_cat_by_id)
        be.manipulate()
        log_msgs = [i.msg for i in caplog.records]
        assert len(log_msgs) == 4
        assert log_msgs[0] == "Converting Worldcat response to bookops-marc Bib object."
        assert log_msgs[1] == "Encountered unsupported resource category."
        assert log_msgs[2] == f"Unable to create call number for {library} b11111111a."
        assert (
            log_msgs[3]
            == "Worldcat record # 850939580 is rejected. Does not meet minimum requirements."
        )

    def test_add_local_tags(self, caplog, stub_resource, stub_res_cat_by_id):
        fields = [
            Field(
                tag="020",
                indicators=Indicators(" ", " "),
                subfields=[Subfield("a", "978123456789x")],
            ),
            Field(
                tag="037",
                indicators=Indicators(" ", " "),
                subfields=[Subfield("a", "123"), Subfield("b", "Overdrive Inc.")],
            ),
            Field(
                tag="856",
                indicators=Indicators("0", "4"),
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

    def test_meets_minimum_criteria_upper_case_title(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("245")
        be.bib.add_field(
            Field(
                tag="245",
                indicators=Indicators("1", "0"),
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
            Field(
                tag="245",
                indicators=Indicators("1", "0"),
                subfields=[Subfield("a", "Foo.")],
            )
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
                indicators=Indicators("1", "0"),
                subfields=[Subfield("a", "Foo /"), Subfield("c", "Spam.")],
            )
        )
        be.bib.remove_fields("300")
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed physical desc. test." in caplog.text

    @pytest.mark.parametrize(
        "tag,value,expectation,msg",
        [
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
            assert be.is_acceptable() == expectation

        assert msg in caplog.text

    def test_meets_minimum_criteria_no_subject_tags(
        self, caplog, stub_resource, stub_res_cat_by_id
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.remove_fields("650")
        with caplog.at_level(logging.DEBUG):
            assert be._meets_minimum_criteria() is False

        assert "Worldcat record failed subjects test." in caplog.text

    @pytest.mark.parametrize(
        "vendor",
        ["Overdrive, Inc.", "3M Company", "Recorded Books, Inc", "CloudLibrary"],
    )
    def test_remove_eresource_vendors(self, stub_resource, stub_res_cat_by_id, vendor):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.bib.add_field(
            Field(
                tag="710",
                indicators=Indicators(" ", "0"),
                subfields=[Subfield("a", vendor)],
            )
        )
        be._remove_eresource_vendors()

        assert len(be.bib.get_fields("710")) == 0

    @pytest.mark.parametrize(
        "tag",
        [
            pytest.param(
                Field(
                    tag="690",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                id="local SH",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gmgpc")],
                ),
                id="GMGPC",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "sears")],
                ),
                id="Other dict: sears",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "4"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcsh")],
                ),
                id="2nd ind = 4",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "1"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                id="Children's LCSH",
            ),
            pytest.param(
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo.")],
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
            Field(
                tag="650",
                indicators=Indicators(" ", "0"),
                subfields=[Subfield("a", "Spam.")],
            )
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
