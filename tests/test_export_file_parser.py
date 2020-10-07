"""
Tests parsing of Sierra exports to file
"""
import pytest

from nightshift.export_file_parser import ResourceMeta, SierraExportReader
from nightshift.errors import SierraExportReaderError


class TestSierraExportReader:
    """
    Test SierraExportReader
    """

    @pytest.mark.parametrize(
        "handle,expectation",
        [
            ("nyp-ebk-201001.txt", 1),
            ("bpl-ebk-201001.txt", 2),
        ],
    )
    def test_determine_library_system_id(self, handle, expectation):
        print(handle, expectation)
        reader = SierraExportReader(handle)
        assert reader.lsid == expectation

    @pytest.mark.parametrize(
        "handle, err_msg",
        [
            (None, "No file handle was passed to Sierra export reader."),
            (1234, "No file handle was passed to Sierra export reader."),
            ([], "No file handle was passed to Sierra export reader."),
        ],
    )
    def test_determine_library_system_id_exceptions(self, handle, err_msg):
        with pytest.raises(SierraExportReaderError) as exc:
            SierraExportReader(handle)
            assert err_msg in str(exc.value)
