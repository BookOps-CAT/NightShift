# -*- coding: utf-8 -*-

"""
worker module tests
"""

import pytest

from nightshift.datastore import Resource, ExportFile
from nightshift.workers import (
    import_sierra_data,
    record_export_file_data,
    retrieve_bibnos_for_enhancement,
)


def test_record_export_file_data(init_dataset):
    session = init_dataset
    fh = "tests/files/bpl-ere-export-sample.txt"
    rec = record_export_file_data(fh, session)
    assert rec.efid == 1
    assert rec.handle == "bpl-ere-export-sample.txt"


def test_import_sierra_data(init_dataset):
    session = init_dataset
    import_sierra_data("tests/files/bpl-ere-export-sample.txt", session)

    assert len(session.query(Resource).all()) == 4
    assert len(session.query(ExportFile).all()) == 1


@pytest.mark.parametrize(
    "lib_sys,bib_cat,expectation",
    [
        ("nyp", "ere", [12345678, 12345679]),
        ("nyp", "pre", [12345670, 12345671]),
        ("bpl", "ere", [22345678, 22345679]),
        ("bpl", "pre", [22345670, 22345671]),
    ],
)
def test_retrieve_bibnos_for_enhancement(
    lib_sys, bib_cat, expectation, brief_bib_dataset
):
    session = brief_bib_dataset
    assert retrieve_bibnos_for_enhancement(lib_sys, bib_cat, session) == expectation
