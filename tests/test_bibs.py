"""
Tests bibs module
"""
from io import BytesIO

import pymarc
import pytest


from nightshift.bibs import (
    add_oclc_prefix,
    add_zeros_to_oclc_number,
    construct_callnumber_tag,
    construct_content_url_tag,
    construct_generic_url_tags,
    construct_isbn_tags,
    construct_oclc_control_number_tag,
    construct_overdrive_access_point_tag,
    construct_overdrive_control_number_tag,
    construct_overdrive_reserve_id_tag,
    construct_sierra_command_tag,
    construct_upc_tags,
    determine_material_type,
    determine_url_label,
    filter_subject_headings,
    has_overdrive_access_point_tag,
    is_approved_vacabulary,
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
    assert "001" in record
    assert "020" in record
    assert "037" in record
    assert "856" in record
    assert "263" in record
    # process
    remove_unwanted_tags(record, "eresources")

    # after
    assert "001" not in record
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
    assert "001" in record
    assert "019" in record
    record.remove_fields("019", "024", "084")
    assert "019" not in record
    remove_unwanted_tags(record, "eresources")
    assert "001" not in record
    assert "020" not in record
    assert "037" not in record
    assert "856" not in record


def test_remove_unwanted_tags_print(stub_marc_bib):
    # before
    record = stub_marc_bib
    assert "001" in record
    assert "263" in record
    remove_unwanted_tags(record, "print")

    # after
    assert "001" not in record
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


def test_construct_overdrive_control_number_tag():
    outcome = construct_overdrive_control_number_tag("ODN123")
    assert type(outcome) == pymarc.field.Field
    assert str(outcome) == "=019  \\\\$aODN123"


@pytest.mark.parametrize(
    "arg,expectation",
    [
        ("1", "00000001"),
        ("12345", "00012345"),
        ("12345678", "12345678"),
        ("123456789", "123456789"),
    ],
)
def test_add_zeros_to_oclc_number(arg, expectation):
    assert add_zeros_to_oclc_number(arg) == expectation


@pytest.mark.parametrize(
    "arg,expectation",
    [
        ("1", "ocm00000001"),
        ("12", "ocm00000012"),
        ("123", "ocm00000123"),
        ("1234", "ocm00001234"),
        ("12345", "ocm00012345"),
        ("123456", "ocm00123456"),
        ("1234567", "ocm01234567"),
        ("12345678", "ocm12345678"),
        ("123456789", "ocn123456789"),
        ("1234567890", "on1234567890"),
        ("12345678901", "on12345678901"),
    ],
)
def test_add_oclc_prefix(arg, expectation):
    assert add_oclc_prefix(arg) == expectation


@pytest.mark.parametrize(
    "arg1,arg2,expectation",
    [
        ("1", 1, "00000001"),
        ("1234567", 1, "01234567"),
        ("123456789", 1, "123456789"),
        ("1", 2, "ocm00000001"),
        ("123456789", 2, "ocn123456789"),
        ("1234567890", 2, "on1234567890"),
    ],
)
def test_construct_oclc_control_number_tag(arg1, arg2, expectation):
    output = construct_oclc_control_number_tag(arg1, arg2)
    assert type(output) == pymarc.field.Field
    assert str(output) == f"=001  {expectation}"


@pytest.mark.parametrize(
    "arg,expectation",
    [
        (1, "unknown"),
        (2, "eresources"),
        (3, "eresources"),
        (4, "eresources"),
        (5, "print"),
    ],
)
def test_determine_material_type(arg, expectation):
    assert determine_material_type(arg) == expectation


@pytest.mark.parametrize(
    "arg1,arg2,expectation",
    [
        ("gsafd", 1, True),
        ("LCGFT", 1, True),
        ("FAST", 1, True),
        ("gsafd", 2, True),
        ("LCGFT", 2, True),
        ("FAST", 2, True),
        ("local", 1, False),
        ("local", 2, False),
    ],
)
def test_is_approved_vocabulary(arg1, arg2, expectation):
    assert is_approved_vacabulary(arg1, arg2) == expectation


def test_filter_subject_headings_nyp_lcsh(stub_marc_bib):
    record = stub_marc_bib
    assert "650" not in record
    record.add_field(
        pymarc.field.Field(
            tag="650",
            indicators=[" ", "0"],
            subfields=["a", "LCSH heading"],
        )
    )
    assert "650" in record
    output = filter_subject_headings(record, 1)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=650  \\0$aLCSH heading"


def test_filter_subject_headings_nyp_children_lcsh(stub_marc_bib):
    record = stub_marc_bib
    assert "650" not in record
    record.add_field(
        pymarc.field.Field(
            tag="650",
            indicators=[" ", "1"],
            subfields=["a", "Children's LCSH heading"],
        )
    )
    assert "650" in record
    output = filter_subject_headings(record, 1)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=650  \\1$aChildren's LCSH heading"


def test_filter_subject_headings_nyp_lcgft(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "LC heading", "2", "lcgft"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 1)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=655  \\7$aLC heading$2lcgft"


def test_filter_subject_headings_nyp_gsafd(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "GSAFD heading", "2", "gsafd"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 1)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=655  \\7$aGSAFD heading$2gsafd"


def test_filter_subject_headings_nyp_bisac(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "BISAC heading", "2", "bisacsh"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 1)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=655  \\7$aBISAC heading$2bisacsh"


def test_filter_subject_headings_nyp_fast(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "FAST heading", "2", "fast"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 1)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=655  \\7$aFAST heading$2fast"


def test_filter_subject_headings_nyp_other(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "LOCAL heading", "2", "local"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 1)
    assert type(output) is list
    assert output == []


def test_filter_subject_headings_bpl_lcsh(stub_marc_bib):
    record = stub_marc_bib
    assert "650" not in record
    record.add_field(
        pymarc.field.Field(
            tag="650",
            indicators=[" ", "0"],
            subfields=["a", "LCSH heading"],
        )
    )
    assert "650" in record
    output = filter_subject_headings(record, 2)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=650  \\0$aLCSH heading"


def test_filter_subject_headings_bpl_children_lcsh(stub_marc_bib):
    record = stub_marc_bib
    assert "650" not in record
    record.add_field(
        pymarc.field.Field(
            tag="650",
            indicators=[" ", "1"],
            subfields=["a", "Children LCSH heading"],
        )
    )
    assert "650" in record
    output = filter_subject_headings(record, 2)
    assert type(output) is list
    assert output == []


def test_filter_subject_headings_bpl_fast(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "FAST heading", "2", "fast"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 2)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=655  \\7$aFAST heading$2fast"


def test_filter_subject_headings_bpl_gsafd(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "GSAFD heading", "2", "gsafd"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 2)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=655  \\7$aGSAFD heading$2gsafd"


def test_filter_subject_headings_bpl_lcgft(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "LCGFT heading", "2", "lcgft"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 2)
    assert type(output) is list
    assert type(output[0]) == pymarc.field.Field
    assert str(output[0]) == "=655  \\7$aLCGFT heading$2lcgft"


def test_filter_subject_headings_bpl_other(stub_marc_bib):
    record = stub_marc_bib
    assert "655" not in record
    record.add_field(
        pymarc.field.Field(
            tag="655",
            indicators=[" ", "7"],
            subfields=["a", "LOCAL heading", "2", "local"],
        )
    )
    assert "655" in record
    output = filter_subject_headings(record, 2)
    assert type(output) is list
    assert output == []


@pytest.mark.parametrize(
    "arg1,arg2,arg3,expectation",
    [
        (999, 1, 1, "=949  \\\\$a*b3=a;ov=.b999a;"),
        (999, 2, 1, "=949  \\\\$a*b2=z;bn=ia;b3=a;ov=.b999a;"),
        (999, 3, 1, "=949  \\\\$a*b2=n;bn=ia;b3=a;ov=.b999a;"),
        (999, 4, 1, "=949  \\\\$a*b2=3;bn=ia;b3=a;ov=.b999a;"),
        (999, 5, 1, "=949  \\\\$a*b2=a;b3=a;ov=.b999a;"),
        (999, 1, 2, "=949  \\\\$a*ov=.b999a;"),
        (999, 2, 2, "=949  \\\\$a*b2=x;ov=.b999a;"),
        (999, 3, 2, "=949  \\\\$a*b2=z;ov=.b999a;"),
        (999, 4, 2, "=949  \\\\$a*b2=v;ov=.b999a;"),
        (999, 5, 2, "=949  \\\\$a*b2=a;ov=.b999a;"),
    ],
)
def test_construct_sierra_command_tag(arg1, arg2, arg3, expectation):
    output = construct_sierra_command_tag(arg1, arg2, arg3)
    assert type(output) == pymarc.field.Field
    assert str(output) == expectation
