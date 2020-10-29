from nightshift.datastore import Resource, ExportFile
from nightshift.workers import import_sierra_data, record_export_file_data


def test_record_export_file_data(init_dataset):
    session = init_dataset
    fh = "tests/files/bpl-ebk-export-sample.txt"
    rec = record_export_file_data(fh, session)
    assert rec.efid == 1
    assert rec.handle == "bpl-ebk-export-sample.txt"


def test_import_sierra_data(init_dataset):
    session = init_dataset
    import_sierra_data("tests/files/bpl-ebk-export-sample.txt", session)

    assert len(session.query(Resource).all()) == 4
    assert len(session.query(ExportFile).all()) == 1
