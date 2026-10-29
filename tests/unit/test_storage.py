import io

import pytest
from paramiko import SFTPClient

from nightshift.comms.storage import Drive, get_credentials
from nightshift.ns_exceptions import DriveError


class TestDriveMocked:
    @pytest.mark.parametrize(
        "port,expectation", [("22", "sftp.fakehost.com:22"), ("", "sftp.fakehost.com")]
    )
    def test_drive(self, mock_drive, monkeypatch, port, expectation, caplog):
        monkeypatch.setenv("SFTP_PORT", port)
        with Drive(*get_credentials()) as drive:
            assert isinstance(drive.sftp, SFTPClient)
            assert drive.src_dir == "sierra_dumps_dir"
            assert drive.dst_dir == "load_dir"
        assert caplog.records[0].msg == f"Opening a secure channel to {expectation}."
        assert caplog.records[1].msg == "Successfully connected to the SFTP."

    def test_drive_connection_error(self, caplog, mock_ssh_exception):
        creds = get_credentials()
        with pytest.raises(DriveError):
            Drive(*creds)
        assert (
            "Unable to establish a secure channel and open SFTP session." in caplog.text
        )

    def test_drive_no_client(self, caplog, mock_empty_sftp_client):
        creds = get_credentials()
        with pytest.raises(DriveError):
            Drive(*creds)
        assert "Unable to create SFTP Client from Transport." in caplog.text

    @pytest.mark.parametrize("arg,expectation", [("foo.mrc", True), ("bar.mrc", False)])
    def test_check_file_exists(self, mock_drive, arg, expectation):
        with Drive(*get_credentials()) as drive:
            assert drive.check_file_exists(arg) == expectation

    def test_fetch_file(self, mock_drive):
        with Drive(*get_credentials()) as drive:
            result = drive.fetch_file("foo.mrc")
            assert isinstance(result, io.BytesIO)

    def test_fetch_file_io_error(self, caplog, mock_drive, mock_io_error):
        with Drive(*get_credentials()) as drive:
            with pytest.raises(DriveError):
                drive.fetch_file("foo.mrc")
        assert (
            "Unable to fetch file sierra_dumps_dir/foo.mrc from the SFTP."
            in caplog.text
        )

    def test_list_src_directory(self, mock_drive):
        with Drive(*get_credentials()) as drive:
            result = drive.list_src_directory()
            assert result == ["NYP-bar-pout"]

    def test_list_src_directory_io_error(self, caplog, mock_drive, mock_io_error):
        with Drive(*get_credentials()) as drive:
            with pytest.raises(DriveError):
                drive.list_src_directory()

        assert "Unable to reach sierra_dumps_dir on the SFTP." in caplog.text

    def test_output_file(self, mock_drive):
        with Drive(*get_credentials()) as drive:
            out_file = drive.output_file("temp.mrc", "foo")
        assert out_file == "foo-02.mrc"

    def test_output_file_io_error(self, caplog, mock_drive, mock_io_error):
        with Drive(*get_credentials()) as drive:
            with pytest.raises(DriveError):
                drive.output_file("temp.mrc", "NYPebook210715")
        assert (
            "IOError. Unable to output temp.mrc to load_dir/NYPebook210715-01.mrc on the SFTP."
            in caplog.text
        )
