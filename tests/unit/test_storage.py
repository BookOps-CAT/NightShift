from io import BytesIO
import logging
import pytest

import paramiko

from nightshift.comms.storage import get_credentials, Drive
from nightshift.ns_exceptions import DriveError


def test_get_credentials(mock_sftp_env, sftpserver):
    assert get_credentials() == (
        sftpserver.host,
        "nightshift",
        "sftp_password",
        "sierra_dumps_dir",
        "load_dir",
        str(sftpserver.port),
    )


class TestDriveMocked:
    """
    note, sftpserver fixture below has session scope by default and is called when
    mock_sftp_env fixture is initated
    """

    def test_drive_initiation(self, sftpserver, mock_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert isinstance(drive.sftp, paramiko.sftp_client.SFTPClient)
            assert drive.src_dir == "sierra_dumps_dir"
            assert drive.dst_dir == "load_dir"

    @pytest.mark.parametrize("arg,expectation", [(dict(a=123), True), (dict(), False)])
    def test_check_file_exists(self, sftpserver, mock_drive, arg, expectation):
        with sftpserver.serve_content(arg):
            assert mock_drive.check_file_exists("/a") == expectation

    def test_check_file_on_closed_sftp_client(self, caplog, mock_drive):
        mock_drive.sftp = None
        with pytest.raises(DriveError):
            with caplog.at_level(logging.ERROR):
                mock_drive.check_file_exists("foo.mrc")

        assert "Attempted operation on a closed SFTP session."

    def test_fetch_file(self, sftpserver, mock_drive):
        with sftpserver.serve_content({"sierra_dumps_dir": {"foo.mrc": b"shrubbery"}}):
            result = mock_drive.fetch_file("foo.mrc")
            assert isinstance(result, BytesIO)
            assert result.read() == b"shrubbery"

    def test_fetch_file_on_closed_sftp_client(self, caplog, mock_drive):
        mock_drive.sftp = None
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DriveError):
                mock_drive.fetch_file("foo.mrc")
        assert "Attempted operation on a closed SFTP session." in caplog.text

    def test_fetch_file_io_error(self, caplog, mock_drive):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DriveError):
                mock_drive.fetch_file("foo.mrc")
        assert (
            "Unable to fetch file sierra_dumps_dir/foo.mrc from the SFTP."
            in caplog.text
        )

    def test_list_src_directory(self, sftpserver, mock_drive):
        with sftpserver.serve_content(
            {"sierra_dumps_dir": {"foo1.mrc": "foo", "foo2.mrc": "foo"}}
        ):
            result = mock_drive.list_src_directory()
            assert result == ["foo1.mrc", "foo2.mrc"]

    def test_list_src_directory_io_error(self, caplog, mock_drive, mock_io_error):
        with caplog.at_level(logging.ERROR):
            mock_drive.src_dir = "FOO/SPAM"
            with pytest.raises(DriveError):
                mock_drive.list_src_directory()

        assert "Unable to reach FOO/SPAM on the SFTP." in caplog.text

    def test_list_src_directory_on_closed_sftp_client(self, caplog, mock_drive):
        mock_drive.sftp = None
        with caplog.at_level(logging.ERROR):
            mock_drive.list_src_directory()

        assert "Attempted an operation on a closed SFTP session." in caplog.text

    def test_output_file(self, sftpserver, mock_drive, tmpdir):
        tmpfile = tmpdir.join("temp.mrc")
        content = "spam"
        tmpfile.write(content)
        with sftpserver.serve_content({"load_dir": {"NYPebook210715-0.mrc": "spam"}}):
            mock_drive.output_file(str(tmpfile), "NYPebook210715-0.mrc")
            assert mock_drive.sftp.listdir("load_dir") == ["NYPebook210715-0.mrc"]

    def test_output_file_io_error(self, caplog, mock_drive, mock_io_error):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DriveError):
                mock_drive.output_file("temp.mrc", "NYPebook210715-0.mrc")
        assert (
            "IOError. Unable to output temp.mrc to load_dir/NYPebook210715-0.mrc on the SFTP."
            in caplog.text
        )

    def test_output_file_on_closed_sftp_client(self, caplog, mock_drive):
        mock_drive.sftp = None
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DriveError):
                mock_drive.output_file("temp.mrc", "NYPebook210715-0.mrc")

        assert (
            "SFTP session closed. Unable to output temp.mrc to load_dir/NYPebook210715-0.mrc on the drive."
            in caplog.text
        )

    def test_construct_dst_file_path(self, mock_drive):
        assert (
            mock_drive._construct_dst_file_path("/test/foo.mrc") == "load_dir/foo.mrc"
        )

    def test_construct_src_file_path(self, mock_drive):
        assert (
            mock_drive._construct_src_file_path("foo.mrc") == "sierra_dumps_dir/foo.mrc"
        )

    def test_sftp_ssh_exception(self, caplog, mock_sftp_env, mock_ssh_exception):
        creds = get_credentials()
        with caplog.at_level(logging.CRITICAL):
            with pytest.raises(DriveError):
                Drive(*creds)
        assert (
            "Unable to establish a secure channel and open SFTP session." in caplog.text
        )

    @pytest.mark.parametrize(
        "host,port,expectation",
        [
            ("foo", "22", "foo:22"),
            ("foo", None, "foo"),
        ],
    )
    def test_sock(self, host, port, expectation, mock_drive):
        assert mock_drive._sock(host, port) == expectation
