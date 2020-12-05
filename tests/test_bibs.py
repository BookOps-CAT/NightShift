"""
Tests bibs module
"""
from io import BytesIO

import pymarc
import pytest


from nightshift.bibs import (
    construct_callnumber_tag,
    construct_content_url_tag,
    construct_generic_url_tags,
    construct_isbn_tags,
    construct_overdrive_access_point_tag,
    construct_overdrive_reserve_id_tag,
    construct_upc_tags,
    determine_url_label,
    has_overdrive_access_point_tag,
    parse_xml_record,
    response2pymarc,
    remove_unwanted_tags,
)
from nightshift.datastore import Resource
from nightshift.errors import NightShiftError


def test_parse_xml_record(fake_xml_response_content):
    record = parse_xml_record(BytesIO(fake_xml_response_content))
    assert type(record) == pymarc.record.Record


def test_marcxml2pymarc(fake_xml_response_content):
    record = response2pymarc(fake_xml_response_content)
    assert type(record) == pymarc.record.Record
    assert record.title() == "Zendegi /"


def test_remove_unwanted_tags_eresources(stub_marc_bib):
    # before
    record = stub_marc_bib
    assert "020" in record
    assert "037" in record
    assert "856" in record
    assert "263" in record
    # process
    remove_unwanted_tags(record, "eresources")

    # after
    assert "019" not in record
    assert "020" not in record
    assert "024" not in record
    assert "037" not in record
    assert "084" not in record
    assert "091" not in record
    assert "099" not in record
    assert "263" not in record
    assert "856" not in record
    assert "938" not in record
    assert "949" not in record


def test_remove_unwanted_tags_eresources_no_tag_found(stub_marc_bib):
    record = stub_marc_bib
    assert "019" in record
    record.remove_fields("019", "024", "084")
    assert "019" not in record
    remove_unwanted_tags(record, "eresources")
    assert "020" not in record
    assert "037" not in record
    assert "856" not in record


def test_remove_unwanted_tags_print(stub_marc_bib):
    # before
    record = stub_marc_bib
    assert "263" in record
    remove_unwanted_tags(record, "print")

    # after
    assert "019" not in record
    assert "020" in record
    assert "024" in record
    assert "037" in record
    assert "084" not in record
    assert "091" not in record
    assert "099" not in record
    assert "263" not in record
    assert "856" in record
    assert "938" not in record
    assert "949" not in record


def test_remove_unwanted_tags_for_unidentified_material_type(stub_marc_bib):
    err_msg = "Unable to manipulate MARC record for unknown material type."
    with pytest.raises(NightShiftError) as exc:
        remove_unwanted_tags(stub_marc_bib, "unknown")

    assert err_msg in str(exc.value)


def test_construct_isbn_tags_no_isbns():
    assert construct_isbn_tags(None) == []


def test_construct_isbn_tags_one_isbn():
    assert type(construct_isbn_tags("1111")) is list
    assert len(construct_isbn_tags("1111")) == 1
    assert type(construct_isbn_tags("1111")[0]) == pymarc.field.Field
    assert str(construct_isbn_tags("1111")[0]) == "=020  \\\\$a1111$q(electronic bk.)"


def test_construct_isbn_tags_multi_isbns():
    output = construct_isbn_tags("1111,2222")
    assert type(output) is list
    assert len(output) == 2
    for o in output:
        assert type(o) == pymarc.field.Field
    assert str(output[0]) == "=020  \\\\$a1111$q(electronic bk.)"
    assert str(output[1]) == "=020  \\\\$a2222$q(electronic bk.)"


def test_construct_upc_tags_no_upc():
    assert construct_upc_tags(None) == []


def test_construct_upc_tags():
    output = construct_upc_tags("1111,2222")
    assert type(output) == list
    assert len(output) == 2
    for o in output:
        assert type(o) == pymarc.field.Field

    assert str(output[0])


def test_construct_overdrive_reserve_id_tag_when_none():
    assert construct_overdrive_reserve_id_tag(None) is None


def test_construct_overdrive_reserve_id_tag():
    tag = construct_overdrive_reserve_id_tag("3333")
    assert type(tag) == pymarc.field.Field
    assert str(tag) == "=037  \\\\$a3333$bOverDrive, Inc.$nhttp://www.overdrive.com"


@pytest.mark.parametrize(
    "arg,expectation", [(2, "Excerpt"), (3, "Image"), (4, "Thumbnail")]
)
def test_determine_url_label(arg, expectation):
    assert determine_url_label(arg) == expectation


def test_construct_generic_url_tags(mixed_dataset):
    resource = mixed_dataset.query(Resource).filter_by(sbid=3, librarySystemId=1).one()
    output = construct_generic_url_tags(resource.urls)
    assert type(output) is list
    for tag in output:
        assert type(tag) == pymarc.field.Field

    assert str(output[0]) == "=856  4\\$uexcerpt_url$3Excerpt"
    assert str(output[1]) == "=856  4\\$uimage_url$3Image"
    assert str(output[2]) == "=856  4\\$uthumbnail_url$3Thumbnail"


@pytest.mark.parametrize(
    "arg1,arg2,expectation",
    [
        ("content_url1", 1, "=856  40$ucontent_url1$yAccess eNYPL"),
        (
            "content_url2",
            2,
            "=856  40$ucontent_url2$zAn electronic book accessible online",
        ),
    ],
)
def test_construct_content_url_tag(arg1, arg2, expectation):
    output = construct_content_url_tag(arg1, arg2)
    assert type(output) == pymarc.field.Field
    assert str(output) == expectation


@pytest.mark.parametrize(
    "arg1,arg2,expectation",
    [
        (1, 1, "=091  \\\\$aERROR UNKNOWN"),
        (2, 1, "=091  \\\\$aeNYPL Book"),
        (3, 1, "=091  \\\\$aeNYPL Audio"),
        (4, 1, "=091  \\\\$aeNYPL Video"),
        (1, 2, "=099  \\\\$aERROR UNKNOWN"),
        (2, 2, "=099  \\\\$aeBOOK"),
        (3, 2, "=099  \\\\$aeAUDIO"),
        (4, 2, "=099  \\\\$aeVIDEO"),
    ],
)
def test_construct_callnumber_tag(arg1, arg2, expectation):
    output = construct_callnumber_tag(arg1, arg2)
    assert type(output) == pymarc.field.Field
    assert str(output) == expectation


@pytest.mark.parametrize("arg1,arg2", [(5, 1), (5, 2)])
def test_construct_callnumber_tag_print_exception(arg1, arg2):
    err_msg = "Processing of print materials not implemented yet."
    with pytest.raises(NightShiftError) as exc:
        construct_callnumber_tag(arg1, arg2)
    assert err_msg in str(exc.value)


def test_has_overdrive_access_point_tag_false(stub_marc_bib):
    assert has_overdrive_access_point_tag(stub_marc_bib) is False


def test_has_overdrive_access_point_tag_true(stub_marc_bib):
    stub_marc_bib.add_field(
        pymarc.field.Field(
            tag="710",
            indicators=["2", " "],
            subfields=["a", "OverDrive, Inc."],
        )
    )
    assert has_overdrive_access_point_tag(stub_marc_bib) is True


def test_construct_overdrive_access_point_tag():
    outcome = construct_overdrive_access_point_tag()
    assert type(outcome) == pymarc.field.Field
    assert str(outcome) == "=710  2\\$aOverDrive, Inc."
