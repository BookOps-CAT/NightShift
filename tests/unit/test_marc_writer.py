"""
Tests `marc.marc_writer.py` module
"""

import os
import pickle

import pytest
from pymarc import Field, Indicators, MARCReader, Record, Subfield

from nightshift import __title__, __version__
from nightshift.datastore import Resource
from nightshift.marc.marc_writer import BibEnhancer


class TestBibEnhancer:
    def test_init(self, stub_resource, stub_res_cat_by_id):
        result = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)

        assert result.library == "NYP"
        assert isinstance(result.resource, Resource)
        assert isinstance(result.bib, Record)

    def test_init_invalid_bib_data(self, stub_resource, stub_res_cat_by_id, caplog):
        stub_resource.fullBib = None
        with pytest.raises(TypeError):
            BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        assert (
            caplog.records[1].msg
            == "Invalid MARC data format: NoneType. Not able to convert to bookops-marc Bib object."
        )

    def test_init_invalid_library(self, stub_resource, stub_res_cat_by_id):
        result = BibEnhancer(stub_resource, "QPL", stub_res_cat_by_id)

        assert result.tags == {}
        assert result.values == {}

    def test_is_acceptable(self, stub_resource, stub_res_cat_by_id):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        assert be.is_acceptable() is True

    @pytest.mark.parametrize(
        "tag,indicators,subfield,msg",
        [
            ("245", ("1", "0"), "FOO", "uppercase title"),
            ("245", ("1", "0"), "Foo", "statement of resp."),
            ("300", None, None, "physical desc."),
            ("100", ("1", " "), "©", "characters encoding"),
            ("100", ("1", " "), "℗", "characters encoding"),
            ("600", None, None, "subjects"),
        ],
    )
    def test_is_acceptable_false(
        self, stub_resource, stub_res_cat_by_id, tag, indicators, subfield, msg, caplog
    ):
        be = BibEnhancer(stub_resource, "BPL", stub_res_cat_by_id)
        be.bib.remove_fields(tag)
        if indicators and subfield:
            be.bib.add_field(
                Field(
                    tag=tag,
                    indicators=indicators,
                    subfields=[Subfield(code="a", value=subfield)],
                )
            )
        assert be.is_acceptable() is False
        assert f"Worldcat record failed {msg} test." in caplog.text

    @pytest.mark.parametrize(
        "library,nid,bib_id,call_no,initials,cmd,suppressed",
        [
            ("NYP", "1", "945", "091", "901", "$a*b2=z;bn=ia;", False),
            ("NYP", "1", "945", "091", "901", "$a*b2=z;b3=n;bn=ia;", True),
            ("BPL", "2", "907", "099", "947", "$a*b2=x;bn=elres;", False),
            ("BPL", "2", "907", "099", "947", "$a*b2=x;b3=n;bn=elres;", True),
        ],
    )
    def test_manipulate(
        self,
        caplog,
        stub_resource,
        stub_res_cat_by_id,
        library,
        nid,
        bib_id,
        call_no,
        initials,
        cmd,
        suppressed,
    ):
        stub_resource.resourceCategoryId = 1
        stub_resource.libraryId = nid
        stub_resource.suppressed = suppressed
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
        be = BibEnhancer(stub_resource, library, stub_res_cat_by_id)
        be.manipulate()
        assert be.bib is not None
        assert (
            "Worldcat record # 850939580 is acceptable. Meets minimum requirements."
            in caplog.text
        )
        assert str(be.bib[call_no]) is not None
        assert str(be.bib[initials]) == f"={initials}  \\\\$a{__title__}/{__version__}"
        assert str(be.bib[bib_id]) == f"={bib_id}  \\\\$a.b11111111a"
        assert str(be.bib["949"]) == f"=949  \\\\{cmd}"

        # check if fields have been duplicated by accident
        assert len(be.bib.get_fields("001")) == 1
        assert len(be.bib.get_fields(call_no)) == 1
        assert len(be.bib.get_fields("037")) == 1

    def test_manipulate_failed(self, caplog, stub_resource, stub_res_cat_by_id):
        stub_resource.resourceCategoryId = 99
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.manipulate()
        assert be.is_acceptable() is False
        assert "Unable to create call number for NYP b11111111a." in caplog.text
        assert (
            "Worldcat record # 850939580 is rejected. Does not meet minimum requirements."
            in caplog.text
        )

    @pytest.mark.parametrize(
        "res_cat_id,ind2,subfields,subj_count",
        [
            pytest.param(
                1,
                "0",
                [Subfield("a", "Electronic books.")],
                1,
                id="ebook: Electronic books - lcsh",
            ),
            pytest.param(
                1,
                "7",
                [Subfield("a", "Electronic books."), Subfield("2", "lcgft")],
                1,
                id="ebook: Electronic books - lcgft",
            ),
            pytest.param(
                2,
                "7",
                [Subfield("a", "Audiobooks."), Subfield("2", "lcgft")],
                2,
                id="eaudio: Audiobooks - lcgft",
            ),
            pytest.param(
                2,
                "7",
                [Subfield("a", "Children's Audiobooks."), Subfield("2", "lcgft")],
                2,
                id="eaudio: Children's audiobooks - lcgft exists",
            ),
            pytest.param(
                2,
                "7",
                [Subfield("a", "Electronic audiobooks."), Subfield("2", "lcgft")],
                2,
                id="eaudio: Electronic audiobooks - lcgft remove",
            ),
            pytest.param(
                3,
                "7",
                [Subfield("a", "Internet videos."), Subfield("2", "lcgft")],
                2,
                id="evideo: Internet videos - lcgft exists",
            ),
            pytest.param(3, None, None, 2, id="evideo: Internet videos - lcgft add"),
        ],
    )
    def test_manipulate_subjects_and_local_fields(
        self,
        caplog,
        stub_resource,
        stub_res_cat_by_id,
        res_cat_id,
        ind2,
        subfields,
        subj_count,
    ):
        stub_resource.resourceCategoryId = res_cat_id
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        if ind2 and subfields:
            be.bib.add_field(
                Field(tag="655", indicators=Indicators(" ", ind2), subfields=subfields)
            )
        original_020 = be.bib.get_fields("020")
        be.manipulate()
        assert (
            "Worldcat record # 850939580 is acceptable. Meets minimum requirements."
            in caplog.text
        )
        assert len(original_020) == 1
        assert len(be.bib.subjects) == subj_count
        assert len(be.bib.get_fields("020")) == 0

    def test_save2file(self, caplog, stub_resource, stub_res_cat_by_id, tmpdir):
        outfile = tmpdir.join("temp.mrc")
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        be.save2file(outfile)
        assert "Saving to file NYP record b11111111a." in caplog.text
        assert os.path.exists(outfile)
        with open(outfile, "rb") as f:
            reader = MARCReader(f)
            bib = next(reader)
            assert isinstance(bib, Record)

    def test_save2file_os_error(
        self, caplog, stub_resource, stub_res_cat_by_id, mock_os_error
    ):
        be = BibEnhancer(stub_resource, "NYP", stub_res_cat_by_id)
        with pytest.raises(OSError):
            be.save2file()

        assert "Unable to save record to a temp file. Error" in caplog.text

    def test_save2file_unacceptable(
        self, caplog, tmpdir, stub_resource, stub_res_cat_by_id
    ):
        outfile = tmpdir.join("foo.mrc")
        stub_resource.resourceCategoryId = 99
        be = BibEnhancer(stub_resource, "BPL", stub_res_cat_by_id)
        be.save2file(outfile)

        assert "No pymarc object to serialize to MARC21" in caplog.text
