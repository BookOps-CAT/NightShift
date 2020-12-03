"""
Tests bibs module
"""
from io import BytesIO

import pymarc


from nightshift.bibs import (
    _parse_xml_record,
    response2pymarc,
    remove_unwanted_tags,
)


def test_parse_xml_record(fake_xml_response_content):
    record = _parse_xml_record(BytesIO(fake_xml_response_content))
    assert type(record) == pymarc.record.Record


def test_marcxml2pymarc(fake_xml_response_content):
    record = response2pymarc(fake_xml_response_content)
    assert type(record) == pymarc.record.Record
    assert record.title() == "Zendegi /"


def test_remove_unwanted_tags(stub_marc_bib):
    # before
    record = stub_marc_bib
    assert "020" in record
    assert "037" in record
    assert "856" in record
    # process
    remove_unwanted_tags(record)

    # after
    assert "019" not in record
    assert "020" not in record
    assert "024" not in record
    assert "037" not in record
    assert "084" not in record
    assert "091" not in record
    assert "099" not in record
    assert "856" not in record
    assert "938" not in record
    assert "949" not in record


def test_remove_unwanted_tags_no_tag_found(stub_marc_bib):
    record = stub_marc_bib
    assert "019" in record
    record.remove_fields("019", "024", "084")
    assert "019" not in record
    remove_unwanted_tags(record)
    assert "020" not in record
    assert "037" not in record
    assert "856" not in record
