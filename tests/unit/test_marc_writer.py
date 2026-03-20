"""
Tests `marc.marc_writer.py` module
"""

import copy
import datetime
import logging
import os
import pickle

import pytest
from pymarc import Field, Indicators, MARCReader, Record, Subfield

from nightshift.datastore import Resource, WorldcatQuery
from nightshift.marc.marc_writer import BibEnhancer


@pytest.fixture
def test_resource(library, resource_id):
    today = datetime.datetime.now(datetime.timezone.utc).date()
    library_id = 1 if library == "NYP" else 2
    return Resource(
        sierraId=11111111,
        libraryId=library_id,
        resourceCategoryId=resource_id,
        sourceId=1,
        bibDate=today - datetime.timedelta(days=31),
        title="TITLE 1",
        status="bot_enhanced",
        fullBib=b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<entry xmlns="http://www.w3.org/2005/Atom">\n<content type="application/xml">\n<response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n<record xmlns="http://www.loc.gov/MARC21/slim">\n<leader>00000cam a2200000Ia 4500</leader>\n<controlfield tag="001">ocn850939580</controlfield>\n<controlfield tag="003">OCoLC</controlfield>\n<controlfield tag="005">20190426152409.0</controlfield>\n<controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n<datafield tag="040" ind1=" " ind2=" ">\n<subfield code="a">OCPSB</subfield>\n<subfield code="b">eng</subfield>\n<subfield code="c">OCPSB</subfield>\n<subfield code="d">NYP</subfield>\n</datafield>\n<datafield tag="035" ind1=" " ind2=" ">\n<subfield code="a">(OCoLC)850939580</subfield>\n</datafield>\n<datafield tag="020" ind1=" " ind2=" ">\n<subfield code="a">some isbn</subfield>\n</datafield>\n<datafield tag="100" ind1="0" ind2=" ">\n<subfield code="a">OCLC RecordBuilder.</subfield>\n</datafield>\n<datafield tag="245" ind1="1" ind2="0">\n<subfield code="a">Record Builder Added This Test Record</subfield>\n<subfield code="c">spam.</subfield>\n</datafield>\n<datafield tag="300" ind1=" " ind2=" ">\n<subfield code="a">1 online resource</subfield>\n</datafield>\n<datafield tag="336" ind1=" " ind2=" ">\n<subfield code="a">text</subfield>\n<subfield code="b">txt</subfield>\n<subfield code="2">rdacontent</subfield>\n</datafield>\n<datafield tag="337" ind1=" " ind2=" ">\n<subfield code="a">unmediated</subfield>\n<subfield code="b">n</subfield>\n<subfield code="2">rdamedia</subfield>\n</datafield>\n<datafield tag="650" ind1=" " ind2="0">\n<subfield code="a">Test.</subfield>\n</datafield>\n</record>\n</response>\n</content>\n<id>http://worldcat.org/oclc/850939580</id>\n<link href="http://worldcat.org/oclc/850939580"/>\n</entry>',
        oclcMatchNumber="850939580",
        enhanceTimestamp=today - datetime.timedelta(days=15),
        queries=[WorldcatQuery(match=True)],
        outputId=1,
    )


@pytest.fixture
def suppressed_test_resource(test_resource):
    res = copy.copy(test_resource)
    res.suppressed = True
    field = [
        Field(
            tag="020",
            indicators=Indicators(" ", " "),
            subfields=[Subfield("a", "978123456789x")],
        )
    ]
    pickled_field = pickle.dumps(field)
    res.srcFieldsToKeep = pickled_field
    return res


class TestBibEnhancer:
    @pytest.mark.parametrize(
        "resource_id,library,call_no,command",
        [
            (1, "NYP", "eNYPL Book", "*b2=z;bn=ia;"),
            (1, "BPL", "eBOOK", "*b2=x;bn=elres;"),
        ],
    )
    def test_manipulate_res_cat_1(
        self, caplog, test_resource, stub_res_cat_by_id, library, call_no, command
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
        assert be.bib["001"].value() == "ocn850939580"
        be.manipulate()
        call_tag = be.tags["call_tag"]
        initials_tag = be.tags["initials_tag"]
        log_msgs = [i.msg for i in caplog.records]
        assert str(be.bib[call_tag]) == f"={call_tag}  \\\\$a{call_no}"
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

    @pytest.mark.parametrize(
        "resource_id,library,command",
        [(1, "NYP", "*b2=z;b3=n;bn=ia;"), (1, "BPL", "*b2=x;b3=n;bn=elres;")],
    )
    def test_manipulate_res_cat_1_suppressed(
        self, caplog, suppressed_test_resource, stub_res_cat_by_id, library, command
    ):
        be = BibEnhancer(suppressed_test_resource, library, stub_res_cat_by_id)
        assert be.bib["001"].value() == "ocn850939580"
        be.manipulate()
        log_msgs = [i.msg for i in caplog.records]
        assert len(log_msgs) == 7
        assert (
            log_msgs[4]
            == f"Added following local fields ['020'] to {library} b11111111a."
        )
        assert (
            log_msgs[5] == f"Added 949 command tag: {command} to {library} b11111111a."
        )

    @pytest.mark.parametrize(
        "resource_id,library,call_no,command,subj",
        [
            (2, "NYP", "eNYPL Audio", "*b2=n;bn=ia;", "Audiobooks"),
            (2, "BPL", "eAUDIO", "*b2=z;bn=elres;", "Audiobooks"),
            (3, "NYP", "eNYPL Video", "*b2=3;bn=ia;", "Internet videos"),
            (3, "BPL", "eVIDEO", "*b2=v;bn=elres;", "Internet videos"),
        ],
    )
    def test_manipulate_res_cat_2_3(
        self, caplog, test_resource, stub_res_cat_by_id, library, call_no, command, subj
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
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
        "resource_id,library,command",
        [
            (2, "NYP", "*b2=n;b3=n;bn=ia;"),
            (2, "BPL", "*b2=z;b3=n;bn=elres;"),
            (3, "NYP", "*b2=3;b3=n;bn=ia;"),
            (3, "BPL", "*b2=v;b3=n;bn=elres;"),
        ],
    )
    def test_manipulate_res_cat_2_3_suppressed(
        self, caplog, suppressed_test_resource, stub_res_cat_by_id, library, command
    ):
        be = BibEnhancer(suppressed_test_resource, library, stub_res_cat_by_id)
        be.manipulate()
        log_msgs = [i.msg for i in caplog.records]
        assert len(log_msgs) == 8
        assert (
            log_msgs[5]
            == f"Added following local fields ['020'] to {library} b11111111a."
        )
        assert (
            log_msgs[6] == f"Added 949 command tag: {command} to {library} b11111111a."
        )

    @pytest.mark.parametrize("resource_id", [4, 5, 6, 7, 8, 9, 10, 11])
    @pytest.mark.parametrize("library", ["NYP", "BPL"])
    def test_manipulate_res_cat_4_11(
        self, caplog, test_resource, stub_res_cat_by_id, library
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
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

    @pytest.mark.parametrize("resource_id,library", [(99, "NYP"), (99, "BPL")])
    def test_manipulate_res_cat_99(
        self, caplog, test_resource, stub_res_cat_by_id, library
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
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

    @pytest.mark.parametrize(
        "resource_id,field,count,subjs",
        [
            (
                1,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Electronic books.")],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[
                        Subfield("a", "Electronic books."),
                        Subfield("2", "lcgft"),
                    ],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Electronic books.")],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Children's electronic books.")],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                2,
                ["Spam.", "Foo."],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcsh")],
                ),
                2,
                ["Spam.", "Foo. lcsh"],
            ),
            (
                1,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "fast")],
                ),
                2,
                ["Spam.", "Foo. fast"],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "homoit")],
                ),
                2,
                ["Spam.", "Foo. homoit"],
            ),
            (
                1,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gsafd")],
                ),
                2,
                ["Spam.", "Foo. gsafd"],
            ),
            (
                1,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcgft")],
                ),
                2,
                ["Spam.", "Foo. lcgft"],
            ),
            (
                1,
                Field(
                    tag="690",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lctgm")],
                ),
                2,
                ["Spam.", "Foo. lctgm"],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "1"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "sears")],
                ),
                1,
                ["Spam."],
            ),
            (
                1,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gmgpc")],
                ),
                1,
                ["Spam."],
            ),
            (
                2,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Audiobooks."), Subfield("2", "lcgft")],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[
                        Subfield("a", "Children's Audiobooks."),
                        Subfield("2", "lcgft"),
                    ],
                ),
                2,
                ["Spam.", "Children's Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[
                        Subfield("a", "Electronic audiobooks."),
                        Subfield("2", "local"),
                    ],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Electronic audiobooks.")],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                3,
                ["Spam.", "Foo.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcsh")],
                ),
                3,
                ["Spam.", "Foo. lcsh", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "fast")],
                ),
                3,
                ["Spam.", "Foo. fast", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "homoit")],
                ),
                3,
                ["Spam.", "Foo. homoit", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gsafd")],
                ),
                3,
                ["Spam.", "Foo. gsafd", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcgft")],
                ),
                3,
                ["Spam.", "Foo. lcgft", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lctgm")],
                ),
                3,
                ["Spam.", "Foo. lctgm", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="690",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "1"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "sears")],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                2,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gmgpc")],
                ),
                2,
                ["Spam.", "Audiobooks. lcgft"],
            ),
            (
                3,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[
                        Subfield("a", "Internet videos."),
                        Subfield("2", "lcgft"),
                    ],
                ),
                2,
                ["Spam.", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                3,
                ["Spam.", "Foo.", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcsh")],
                ),
                3,
                ["Spam.", "Foo. lcsh", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "fast")],
                ),
                3,
                ["Spam.", "Foo. fast", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "homoit")],
                ),
                3,
                ["Spam.", "Foo. homoit", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gsafd")],
                ),
                3,
                ["Spam.", "Foo. gsafd", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lcgft")],
                ),
                3,
                ["Spam.", "Foo. lcgft", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="655",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "lctgm")],
                ),
                3,
                ["Spam.", "Foo. lctgm", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="690",
                    indicators=Indicators(" ", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                2,
                ["Spam.", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                2,
                ["Spam.", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "1"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                2,
                ["Spam.", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "gmgpc")],
                ),
                2,
                ["Spam.", "Internet videos. lcgft"],
            ),
            (
                3,
                Field(
                    tag="650",
                    indicators=Indicators(" ", "7"),
                    subfields=[Subfield("a", "Foo."), Subfield("2", "sears")],
                ),
                2,
                ["Spam.", "Internet videos. lcgft"],
            ),
        ],
    )
    @pytest.mark.parametrize("library", ["NYP", "BPL"])
    def test_manipulate_remove_unwanted_6xx(
        self, test_resource, library, stub_res_cat_by_id, field, count, subjs
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
        assert len(be.bib.subjects) == 1
        assert be.bib.subjects[0].value() == "Test."
        be.bib.remove_fields("650", "655")
        be.bib.add_field(
            Field(
                tag="600",
                indicators=Indicators("0", "0"),
                subfields=[Subfield("a", "Spam.")],
            )
        )
        be.bib.add_field(field)
        be.manipulate()
        assert len(be.bib.subjects) == count
        assert [i.value() for i in be.bib.subjects] == subjs

    @pytest.mark.parametrize(
        "field",
        [
            Field(
                tag="710",
                indicators=Indicators(" ", "0"),
                subfields=[Subfield("a", "Overdrive, Inc.")],
            ),
            Field(
                tag="710",
                indicators=Indicators(" ", "0"),
                subfields=[Subfield("a", "3M Company")],
            ),
            Field(
                tag="710",
                indicators=Indicators(" ", "0"),
                subfields=[Subfield("a", "Recorded Books, Inc")],
            ),
            Field(
                tag="710",
                indicators=Indicators(" ", "0"),
                subfields=[Subfield("a", "CloudLibrary")],
            ),
        ],
    )
    @pytest.mark.parametrize("library", ["NYP", "BPL"])
    @pytest.mark.parametrize("resource_id", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    def test_manipulate_remove_eresource_vendors(
        self, test_resource, stub_res_cat_by_id, library, field, caplog
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
        assert len(be.bib.get_fields("710")) == 0
        be.bib.add_field(field)
        be.bib.add_field(
            Field(
                tag="710",
                indicators=Indicators(" ", "0"),
                subfields=[Subfield("a", "Foo")],
            )
        )
        assert len(be.bib.get_fields("710")) == 2
        be.manipulate()
        assert len(be.bib.get_fields("710")) == 1
        assert be.bib.get_fields("710")[0].value() == "Foo"

    @pytest.mark.parametrize(
        "field,delete_tag,msg",
        [
            (
                Field(
                    tag="245",
                    indicators=Indicators("1", "0"),
                    subfields=[Subfield("a", "FOO /"), Subfield("c", "spam")],
                ),
                "",
                "Worldcat record failed uppercase title test.",
            ),
            (
                Field(
                    tag="245",
                    indicators=Indicators("1", "0"),
                    subfields=[Subfield("a", "Foo.")],
                ),
                "",
                "Worldcat record failed statement of resp. test.",
            ),
            (
                Field(
                    tag="245",
                    indicators=Indicators("1", "0"),
                    subfields=[Subfield("a", "Foo."), Subfield("c", "spam")],
                ),
                "300",
                "Worldcat record failed physical desc. test.",
            ),
            (
                Field(
                    tag="245",
                    indicators=Indicators("1", "0"),
                    subfields=[Subfield("a", "Foo."), Subfield("c", "spam")],
                ),
                "650",
                "Worldcat record failed subjects test.",
            ),
            (
                Field(
                    tag="245",
                    indicators=Indicators("1", "0"),
                    subfields=[Subfield("a", "©."), Subfield("c", "spam")],
                ),
                "",
                "Worldcat record failed characters encoding test.",
            ),
            (
                Field(
                    tag="245",
                    indicators=Indicators("1", "0"),
                    subfields=[Subfield("a", "℗."), Subfield("c", "spam")],
                ),
                "",
                "Worldcat record failed characters encoding test.",
            ),
        ],
    )
    @pytest.mark.parametrize("library", ["NYP", "BPL"])
    @pytest.mark.parametrize("resource_id", [1, 2, 3])
    def test_manipulate_does_not_meet_min_criteria(
        self, caplog, test_resource, stub_res_cat_by_id, field, delete_tag, library, msg
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
        be.bib.remove_fields("245")
        be.bib.remove_fields(delete_tag)
        be.bib.add_field(field)
        be.manipulate()
        log_msgs = [i.msg for i in caplog.records]
        assert len(log_msgs) == 4
        assert msg in caplog.text

    @pytest.mark.parametrize("library", ["NYP", "BPL"])
    @pytest.mark.parametrize("resource_id", [1, 2, 3])
    def test_save2file(self, caplog, test_resource, stub_res_cat_by_id, library):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
        with caplog.at_level(logging.DEBUG):
            be.save2file()
        assert f"Saving to file {library} record b11111111a." in caplog.text

        assert os.path.exists("temp.mrc")
        with open("temp.mrc", "rb") as f:
            reader = MARCReader(f)
            bib = next(reader)
            assert isinstance(bib, Record)

        # cleanup
        os.remove("temp.mrc")

    @pytest.mark.parametrize("library", ["NYP", "BPL"])
    @pytest.mark.parametrize("resource_id", [1, 2, 3])
    def test_save2file_os_error(
        self, caplog, test_resource, stub_res_cat_by_id, mock_os_error, library
    ):
        be = BibEnhancer(test_resource, library, stub_res_cat_by_id)
        with caplog.at_level(logging.ERROR):
            with pytest.raises(OSError):
                be.save2file()

        assert "Unable to save record to a temp file. Error" in caplog.text
