# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
from datetime import date
from io import BytesIO
import pickle

from pymarc import Field, Record
import pytest

from nightshift.datastore import Resource
from nightshift.marc.marc_parser import BibReader, worldcat_response_to_pymarc


def test_worldcat_response_to_pymarc():
    data = b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<entry xmlns="http://www.w3.org/2005/Atom">\n  <content type="application/xml">\n    <response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n      <record xmlns="http://www.loc.gov/MARC21/slim">\n        <leader>00000cam a2200000Ia 4500</leader>\n        <controlfield tag="001">ocn850939580</controlfield>\n        <controlfield tag="003">OCoLC</controlfield>\n        <controlfield tag="005">20190426152409.0</controlfield>\n        <controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n        <datafield tag="040" ind1=" " ind2=" ">\n          <subfield code="a">OCPSB</subfield>\n          <subfield code="b">eng</subfield>\n          <subfield code="c">OCPSB</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">NYP</subfield>\n    </datafield>\n        <datafield tag="035" ind1=" " ind2=" ">\n          <subfield code="a">(OCoLC)850939580</subfield>\n    </datafield>\n        <datafield tag="049" ind1=" " ind2=" ">\n          <subfield code="a">NYPP</subfield>\n    </datafield>\n        <datafield tag="100" ind1="0" ind2=" ">\n          <subfield code="a">OCLC RecordBuilder.</subfield>\n    </datafield>\n        <datafield tag="245" ind1="1" ind2="0">\n          <subfield code="a">Record Builder Added This Test Record On 06/26/2013 13:06:26.</subfield>\n    </datafield>\n        <datafield tag="336" ind1=" " ind2=" ">\n          <subfield code="a">text</subfield>\n          <subfield code="b">txt</subfield>\n          <subfield code="2">rdacontent</subfield>\n    </datafield>\n        <datafield tag="337" ind1=" " ind2=" ">\n          <subfield code="a">unmediated</subfield>\n          <subfield code="b">n</subfield>\n          <subfield code="2">rdamedia</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">TEST RECORD -- DO NOT USE.</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">Added Field by MarcEdit.</subfield>\n    </datafield>\n  </record>\n    </response>\n  </content>\n  <id>http://worldcat.org/oclc/850939580</id>\n  <link href="http://worldcat.org/oclc/850939580"/>\n</entry>'
    record = worldcat_response_to_pymarc(data)
    assert isinstance(record, Record)
    assert record["001"].data == "ocn850939580"


def test_wordcat_response_to_pymarc_invalid_data_type():
    with pytest.raises(TypeError):
        worldcat_response_to_pymarc("some_str_data")


@pytest.mark.parametrize("arg", ["nyp", "bpl"])
def test_BibReader_library_arg(arg):
    with does_not_raise():
        BibReader(marc_target=BytesIO(b"some records"), library=arg)


def test_BibReader_invalid_library():
    with pytest.raises(ValueError):
        BibReader(BytesIO(b"some records"), "qpl")


def test_BibReader_invalid_marc_target():
    with pytest.raises(ValueError):
        BibReader(123, "nyp")


def test_BibReader_iterator():
    reader = BibReader("tests/nyp-ebook-sample.mrc", "nyp")
    with does_not_raise():
        for bib in reader:
            continue
    assert reader.marc_target.closed


@pytest.mark.parametrize(
    "arg1,arg2,expectation",
    [
        ("a", "ODN12345", "ebook"),
        ("i", "ODN12345", "eaudio"),
        ("g", "ODN12345", "evideo"),
        ("c", "ODN12345", None),
        ("a", "BT12345", None),
    ],
)
def test_BibReader_determine_resource_category(
    arg1, arg2, expectation, stub_marc, fake_BibReader
):
    stub_marc.leader = f"00000n{arg1}m a2200385Ka 4500"
    stub_marc.add_field(Field(tag="001", data=arg2))
    assert fake_BibReader._determine_resource_category(stub_marc) == expectation


def test_BibReader_pickle_obj(fake_BibReader):
    assert isinstance(fake_BibReader._pickle_obj(["foo"]), bytes)


@pytest.mark.parametrize(
    "arg1, arg2", [("a", "ebook"), ("i", "eaudio"), ("g", "evideo")]
)
def test_BibReader_fields2keep_eresource(arg1, arg2, stub_marc, fake_BibReader):
    tags = [
        Field(tag="001", data="ODN12345"),
        Field(tag="020", indicators=[" ", " "], subfields=["a", "978111111111x"]),
        Field(
            tag="037", indicators=[" ", " "], subfields=["a", "12345", "b", "Ovedrive"]
        ),
        Field(tag="856", indicators=["4", "0"], subfields=["u", "example.com"]),
        Field(
            tag="856",
            indicators=["4", " "],
            subfields=["3", "Image", "u", "example.com"],
        ),
    ]
    stub_marc.leader = f"00000n{arg1}m a2200385Ka 4500"
    for tag in tags:
        stub_marc.add_field(tag)

    pickled = fake_BibReader._fields2keep(bib=stub_marc, resource_category=arg2)
    result = pickle.loads(pickled)
    assert len(result) == 5
    assert result[0].tag == "001"
    assert result[1].tag == "020"
    assert result[2].tag == "037"
    assert result[3].tag == "856"
    assert result[4].tag == "856"


@pytest.mark.parametrize(
    "arg1, arg2, expectation",
    [("a", "ebook", 1), ("i", "eaudio", 2), ("g", "evideo", 3)],
)
def test_BibReader_map_data_eresource(
    arg1, arg2, expectation, stub_marc, fake_BibReader
):
    bib = stub_marc
    bib.leader = f"00000n{arg1}m a2200385Ka 4500"
    tags = [
        Field(tag="001", data="ODN12345"),
        Field(tag="010", subfields=["a", "12345"]),
        Field(tag="020", indicators=[" ", " "], subfields=["a", "978111111111x"]),
        Field(
            tag="037",
            indicators=[" ", " "],
            subfields=["a", "1234567", "b", "Ovedrive"],
        ),
        Field(tag="856", indicators=["4", "0"], subfields=["u", "example.com"]),
        Field(
            tag="856",
            indicators=["4", " "],
            subfields=["3", "Image", "u", "example.com"],
        ),
        Field(
            tag="907",
            subfields=["a", ".b225094204", "b", "07-03-21", "c", "07-03-2021 19:07"],
        ),
    ]
    for tag in tags:
        bib.add_field(tag)

    res = fake_BibReader._map_data(bib=bib, resource_category=arg2)
    assert isinstance(res, Resource)
    assert res.sierraId == "22509420"
    assert res.libraryId == 1
    assert res.resourceCategoryId == expectation
    assert res.bibDate == date(2021, 7, 3)
    assert res.author == "Adams, John, author."
    assert res.title == "The foo /"
    assert res.pubDate == "2021"
    assert res.congressNumber == "12345"
    assert res.controlNumber == "ODN12345"
    assert res.distributorNumber == "1234567"
    assert res.otherNumber is None
    assert isinstance(res.srcFieldsToKeep, bytes)
    assert res.standardNumber == "978111111111x"
    assert res.status == "open"
