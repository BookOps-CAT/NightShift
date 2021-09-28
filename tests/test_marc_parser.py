# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
from datetime import date
import pickle

from pymarc import Field
import pytest

from nightshift.datastore import Resource
from nightshift.marc_parser import BibReader


@pytest.mark.parametrize("arg", ["nyp", "bpl"])
def test_BibReader_library_arg(arg):
    with does_not_raise():
        BibReader(marc_fh="foo.mrc", library=arg)


def test_BibReader_invalid_library():
    with pytest.raises(ValueError):
        BibReader("foo.mrc", "qpl")


def test_BibReader_iterator():
    reader = BibReader("tests/nyp-ebook-sample.mrc", "nyp")
    with does_not_raise():
        for bib in reader:
            pass


@pytest.mark.parametrize(
    "arg,expectation", [("ODN12345", "eresource"), ("BT12345", None)]
)
def test_BibReader_determine_resource_category(arg, expectation, stub_marc):
    stub_marc.add_field(Field(tag="001", data=arg))
    reader = BibReader("foo.mrc", "nyp")
    assert reader._determine_resource_category(stub_marc) == expectation


def test_BibReader_pickle_obj():
    reader = BibReader("foo.mrc", "nyp")
    assert isinstance(reader._pickle_obj(["foo"]), bytes)


def test_BibReader_fields2keep_eresource(stub_marc):
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
    for tag in tags:
        stub_marc.add_field(tag)

    reader = BibReader("foo.mrc", "nyp")
    pickled = reader._fields2keep(bib=stub_marc, resource_category="eresource")
    result = pickle.loads(pickled)
    assert len(result) == 5
    assert result[0].tag == "001"
    assert result[1].tag == "020"
    assert result[2].tag == "037"
    assert result[3].tag == "856"
    assert result[4].tag == "856"


def test_BibReader_map_data_eresource(stub_marc):
    bib = stub_marc
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

    reader = BibReader("foo.mrc", "nyp")
    res = reader._map_data(bib=bib, resource_category="eresource")
    assert isinstance(res, Resource)
    assert res.sierraId == "22509420"
    assert res.libraryId == 1
    assert res.resourceCategoryId == 1
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
