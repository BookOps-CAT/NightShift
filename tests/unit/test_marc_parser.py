import pickle
from collections.abc import Iterable
from datetime import date
from io import BytesIO

import pytest
from bookops_marc import Bib
from pymarc import Field, Indicators, Subfield

from nightshift.datastore import Resource
from nightshift.marc.marc_parser import BibReader


@pytest.fixture
def stub_marc():
    bib = Bib()
    bib.leader = "02866pam  2200517 i 4500"
    bib.add_field(Field(tag="008", data="190306s2017    ht a   j      000 1 hat d"))
    bib.add_field(
        Field(
            tag="100",
            indicators=Indicators("1", " "),
            subfields=[Subfield("a", "Adams, John,"), Subfield("e", "author.")],
        )
    )
    bib.add_field(
        Field(
            tag="245",
            indicators=Indicators("1", "4"),
            subfields=[Subfield("a", "The foo /"), Subfield("c", "by John Adams.")],
        )
    )
    bib.add_field(
        Field(
            tag="264",
            indicators=Indicators(" ", "1"),
            subfields=[
                Subfield("a", "Bar :"),
                Subfield("b", "New York,"),
                Subfield("c", "2021"),
            ],
        )
    )
    bib.add_field(
        Field(
            tag="907",
            indicators=Indicators(" ", " "),
            subfields=[
                Subfield("a", ".b22222222x"),
                Subfield("b", "07-01-21"),
                Subfield("c", "07-01-2021 19:07"),
            ],
        )
    )

    return bib


def test_BibReader(stub_res_cat_by_name):
    reader = BibReader(
        marc_target="tests/nyp-ebook-sample.mrc",
        library="NYP",
        libraryId=1,
        resource_categories=stub_res_cat_by_name,
    )
    assert isinstance(reader, Iterable)


def test_BibReader_invalid_marc_target(caplog, stub_res_cat_by_name):
    with pytest.raises(TypeError):
        BibReader(123, "NYP", 1, stub_res_cat_by_name)

    assert "Invalid 'marc_target' argument: '123' (int)" in caplog.text


def test_BibReader_invalid_resource_categories_arg(caplog):
    with pytest.raises(TypeError):
        BibReader(
            marc_target=BytesIO(b"foo"),
            library="NYP",
            libraryId=1,
            resource_categories=[],
        )
    assert "Invalid 'resource_categories' argument" in caplog.text


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
    arg1, arg2, expectation, stub_marc, stub_res_cat_by_name
):
    stub_marc.leader = f"00000n{arg1}m a2200385Ka 4500"
    stub_marc.add_field(Field(tag="001", data=arg2))
    reader = BibReader(BytesIO(b"some records"), "NYP", 1, stub_res_cat_by_name)
    assert reader._determine_resource_category(stub_marc) == expectation


def test_BibReader_determine_resource_category_bib_none(stub_res_cat_by_name):
    reader = BibReader(BytesIO(b"some records"), "NYP", 1, stub_res_cat_by_name)
    assert reader._determine_resource_category(None) is None


def test_BibReader_unsupported_resource_type_log_warning(
    caplog, stub_marc, stub_res_cat_by_name
):
    stub_marc.leader = "00000nem a2200385Ka 4500"  # cartographic mat rec type
    reader = BibReader(BytesIO(b"some records"), "NYP", 1, stub_res_cat_by_name)
    reader._determine_resource_category(stub_marc)

    assert "Unsupported bib type. Unable to ingest NYP bib # b22222222x." in caplog.text


def test_BibReader_pickle_obj(stub_res_cat_by_name):
    reader = BibReader(BytesIO(b"some records"), "NYP", 1, stub_res_cat_by_name)
    assert isinstance(reader._pickle_obj(["foo"]), bytes)


@pytest.mark.parametrize(
    "arg1, arg2", [("a", "ebook"), ("i", "eaudio"), ("g", "evideo")]
)
def test_BibReader_fields2keep_eresource(arg1, arg2, stub_marc, stub_res_cat_by_name):
    tags = [
        Field(tag="001", data="ODN12345"),
        Field(
            tag="020",
            indicators=Indicators(" ", " "),
            subfields=[Subfield("a", "978111111111x")],
        ),
        Field(
            tag="037",
            indicators=Indicators(" ", " "),
            subfields=[Subfield("a", "12345"), Subfield("b", "Ovedrive")],
        ),
        Field(
            tag="856",
            indicators=Indicators("4", "0"),
            subfields=[Subfield("u", "example.com")],
        ),
        Field(
            tag="856",
            indicators=Indicators("4", " "),
            subfields=[Subfield("3", "Image"), Subfield("u", "example.com")],
        ),
    ]
    stub_marc.leader = f"00000n{arg1}m a2200385Ka 4500"
    for tag in tags:
        stub_marc.add_field(tag)
    reader = BibReader(BytesIO(b"some records"), "NYP", 1, stub_res_cat_by_name)
    pickled = reader._fields2keep(bib=stub_marc, resource_category=arg2)
    result = pickle.loads(pickled)
    assert len(result) == 4
    assert result[0].tag == "020"
    assert result[1].tag == "037"
    assert result[2].tag == "856"
    assert result[3].tag == "856"


@pytest.mark.parametrize(
    "arg1, arg2, expectation",
    [("a", "ebook", 1), ("i", "eaudio", 2), ("g", "evideo", 3)],
)
def test_BibReader_map_data_eresource(
    arg1, arg2, expectation, stub_marc, stub_res_cat_by_name
):
    bib = stub_marc
    bib.leader = f"00000n{arg1}m a2200385Ka 4500"
    tags = [
        Field(tag="001", data="ODN12345"),
        Field(tag="010", subfields=[Subfield("a", "12345")]),
        Field(
            tag="020",
            indicators=Indicators(" ", " "),
            subfields=[Subfield("a", "978111111111x")],
        ),
        Field(
            tag="037",
            indicators=Indicators(" ", " "),
            subfields=[Subfield("a", "1234567"), Subfield("b", "OverDrive, Inc.")],
        ),
        Field(
            tag="856",
            indicators=Indicators("4", "0"),
            subfields=[Subfield("u", "example.com")],
        ),
        Field(
            tag="856",
            indicators=Indicators("4", " "),
            subfields=[Subfield("3", "Image"), Subfield("u", "example.com")],
        ),
        Field(
            tag="907",
            subfields=[
                Subfield("a", ".b22222222x"),
                Subfield("b", "07-01-21"),
                Subfield("c", "07-01-2021 19:07"),
            ],
        ),
        Field(
            tag="998", indicators=Indicators(" ", " "), subfields=[Subfield("e", "n")]
        ),
    ]
    for tag in tags:
        bib.add_field(tag)
    reader = BibReader(BytesIO(b"some records"), "NYP", 1, stub_res_cat_by_name)
    res = reader._map_data(bib=bib, resource_category=arg2)
    assert isinstance(res, Resource)
    assert res.sierraId == 22222222
    assert res.libraryId == 1
    assert res.resourceCategoryId == expectation
    assert res.bibDate == date(2021, 7, 1)
    assert res.author == "Adams, John, author."
    assert res.title == "The foo /"
    assert res.pubDate == "2021"
    assert res.congressNumber == "12345"
    assert res.controlNumber == "ODN12345"
    assert res.distributorNumber == "1234567"
    assert res.suppressed is True
    assert res.otherNumber is None
    assert isinstance(res.srcFieldsToKeep, bytes)
    assert res.standardNumber == "978111111111x"
    assert res.status == "open"
