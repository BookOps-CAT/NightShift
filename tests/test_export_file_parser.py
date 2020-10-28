# -*- coding: utf-8 -*-

"""
Tests parsing of Sierra exports to file
"""
from datetime import date

import pytest

from nightshift.datastore_values import LIB_SYS, BIB_CAT
from nightshift.export_file_parser import ResourceMeta, SierraExportReader
from nightshift.errors import SierraExportReaderError


class TestSierraExportReader:
    """
    Test SierraExportReader
    """

    @pytest.mark.parametrize(
        "handle,expectation",
        [
            ("nyp-ebk-201001.txt", LIB_SYS["nyp"]["lsid"]),
            ("bpl-ebk-201001.txt", LIB_SYS["bpl"]["lsid"]),
        ],
    )
    def test_determine_library_system_id(self, handle, expectation):
        reader = SierraExportReader(handle)

        assert reader._determine_library_system_id(handle) == expectation

    @pytest.mark.parametrize(
        "handle, err_msg",
        [
            (None, "No file handle was passed to Sierra export reader."),
            (1234, "No file handle was passed to Sierra export reader."),
            ([], "No file handle was passed to Sierra export reader."),
            ("ebk-201001.txt", "Sierra export file handle has invalid format"),
        ],
    )
    def test_determine_library_system_id_exceptions(self, handle, err_msg):
        with pytest.raises(SierraExportReaderError) as exc:
            SierraExportReader(handle)
            assert err_msg in str(exc.value)

    @pytest.mark.parametrize(
        "handle,expectation",
        [
            ("nyp-ebk-201001.txt", BIB_CAT["ebk"]["bcid"]),
            ("bpl-pre-201001.txt", BIB_CAT["pre"]["bcid"]),
        ],
    )
    def test_determine_bib_category_id(self, handle, expectation):
        reader = SierraExportReader(handle)
        assert reader._determine_bib_category_id(handle) == expectation

    @pytest.mark.parametrize("handle", ["nyp-201001.txt", "bpl-xxx-201001.txt"])
    def test_determine_bib_category_id_exceptions(self, handle):
        err_msg = "Sierra export file handle has invalid bib category."
        with pytest.raises(SierraExportReaderError) as exc:
            SierraExportReader(handle)
            assert err_msg in str(exc.value)

    def test_determine_bib_created_date(self):
        reader = SierraExportReader("nyp-ebk-201001.txt")
        assert str(reader._determine_bib_created_date("09-30-2020")) == "2020-09-30"

    @pytest.mark.parametrize("arg", ["2020-09-30", "", None])
    def test_determine_bib_created_date_exceptions(self, arg):
        with pytest.raises(SierraExportReaderError):
            reader = SierraExportReader("nyp-ebk-201001.txt")
            reader._determine_bib_created_date(arg)

    def test_prep_sierra_bibno(self):
        reader = SierraExportReader("nyp-ebk-201001.txt")
        assert reader._prep_sierra_bibno("b123456789") == 12345678

    @pytest.mark.parametrize("arg", [None, ""])
    def test_prep_sierra_bibno_exceptions(self, arg):
        err_msg = "Invalid Sierra number passed."
        reader = SierraExportReader("nyp-ebk-201001.txt")
        with pytest.raises(SierraExportReaderError) as exc:
            reader._prep_sierra_bibno(arg)
            assert err_msg in str(exc.value)

    @pytest.mark.parametrize(
        "fh,row,expectation",
        [
            (
                "nyp-ebk-201001.txt",
                ["b123530271", "09-30-2020", "ODN0004408595"],
                ResourceMeta(12353027, 1, 1, "ODN0004408595", date(2020, 9, 30)),
            ),
            (
                "bpl-pre-201001.txt",
                ["b123530283", "09-30-2020", "ODN0005151593"],
                ResourceMeta(12353028, 2, 2, "ODN0005151593", date(2020, 9, 30)),
            ),
        ],
    )
    def test_map_data(self, fh, row, expectation):
        reader = SierraExportReader(fh)
        assert reader._map_data(row) == expectation

    def test_generator(self):
        reader = SierraExportReader("tests/files/bpl-ebk-export-sample.txt")
        records = []
        n = 0
        for record in reader:
            records.append(record)
            print(record)
            assert record.bibDate == date(2020, 9, 30)
            assert record.lsid == 2
            assert record.bcid == 1

            if n == 0:
                assert record.sbid == 12353027
                assert record.cno == "ODN0004408595"
            elif n == 1:
                assert record.sbid == 12353028
                assert record.cno == "ODN0005151593"
            elif n == 2:
                assert record.sbid == 12353058
                assert record.cno == "ODN0005254906"
            elif n == 3:
                assert record.sbid == 12353059
                assert record.cno == "ODN0005124775"
            n += 1
        assert len(records) == 4
