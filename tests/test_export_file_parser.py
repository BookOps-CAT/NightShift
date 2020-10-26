# -*- coding: utf-8 -*-

"""
Tests parsing of Sierra exports to file
"""
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
