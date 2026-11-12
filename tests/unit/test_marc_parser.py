from io import BytesIO

import pytest
from bookops_marc import Bib
from pymarc import Field, Indicators, Subfield

from nightshift.marc.marc_parser import BibReader


@pytest.fixture
def stub_marc():
    bib = Bib()
    bib.leader = "00000nam a2200385 i 4500"
    bib.add_field(Field(tag="001", data="ODN12345"))
    bib.add_field(
        Field(
            tag="245",
            indicators=Indicators("1", "4"),
            subfields=[Subfield("a", "The foo /"), Subfield("c", "by John Adams.")],
        )
    )
    bib.add_field(
        Field(
            tag="907",
            indicators=Indicators(" ", " "),
            subfields=[Subfield("a", ".b22222222x")],
        )
    )
    return bib


def test_bib_reader(stub_res_cat_by_name, mocker, stub_marc):
    mock_read = mocker.mock_open(read_data=stub_marc.as_marc())
    mocker.patch("nightshift.marc.marc_parser.open", mock_read)
    reader = BibReader("temp.mrc", "NYP", 1, stub_res_cat_by_name)
    records = list(reader)
    assert len(records) > 0
    assert isinstance(records[0].srcFieldsToKeep, bytes)


@pytest.mark.parametrize("record_type,res_cat_id", [("a", 1), ("i", 2), ("g", 3)])
def test_bib_reader_bytes_io(record_type, stub_marc, stub_res_cat_by_name, res_cat_id):
    stub_marc.leader = f"00000n{record_type}m a2200385 i 4500"
    reader = BibReader(BytesIO(stub_marc.as_marc()), "NYP", 1, stub_res_cat_by_name)
    record = next(iter(reader))
    assert record.resourceCategoryId == res_cat_id


@pytest.mark.parametrize(
    "record_type,control_number,last_log",
    [
        ("c", "ODN12345", "Initiating BibReader."),
        (
            "a",
            "BT12345",
            "Unsupported bib type. Unable to ingest NYP bib # b22222222x.",
        ),
    ],
)
def test_bib_reader_stop_iteration(
    stub_marc, stub_res_cat_by_name, caplog, record_type, control_number, last_log
):
    stub_marc.leader = f"00000n{record_type}m a2200385 i 4500"
    stub_marc.remove_fields("001")
    stub_marc.add_field(Field(tag="001", data=control_number))
    reader = BibReader(BytesIO(stub_marc.as_marc()), "NYP", 1, stub_res_cat_by_name)
    with pytest.raises(StopIteration):
        next(iter(reader))
    assert caplog.records[-1].msg == last_log


@pytest.mark.parametrize(
    "target,res_cat,output",
    [
        (123, {}, "Invalid 'marc_target' argument: '123' (int)"),
        (BytesIO(b"foo"), [], "Invalid 'resource_categories' argument"),
    ],
)
def test_bib_reader_errors(caplog, target, res_cat, output):
    with pytest.raises(TypeError):
        BibReader(target, "NYP", 1, res_cat)

    assert output in caplog.text
