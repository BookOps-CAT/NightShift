from contextlib import nullcontext as does_not_raise
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


@pytest.mark.localtest
class TestDriveLive:
    def test_list_src_directory(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert sorted(drive.list_src_directory()) == [
                "BPLeres210701.pout",
                "NYPeres210701.pout",
            ]

    def test_fetch_file(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            with does_not_raise():
                file = drive.fetch_file("NYPeres210701.pout")
            assert isinstance(file, BytesIO)

    def test_construct_dst_file_path(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert (
                drive._construct_dst_file_path("/test/foo.mrc")
                == "/NSDROP/TEST/load/foo.mrc"
            )

    def test_construct_src_file_path(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert (
                drive._construct_src_file_path("foo.mrc")
                == "/NSDROP/TEST/sierra_dumps/nightshift/foo.mrc"
            )

    def test_list_src_directory_io_error(self, caplog, live_sftp_env, mock_io_error):
        """
        this may happen when sierra_dumps/nightshift directory is accidentally deleted
        """
        creds = get_credentials()
        with caplog.at_level(logging.ERROR):
            with Drive(*creds) as drive:
                drive.src_dir = "/NSDROP/TEST/FOO"  # non-existing dir
                with pytest.raises(DriveError):
                    drive.list_src_directory()

        assert "Unable to reach /NSDROP/TEST/FOO on the SFTP" in caplog.text

    def test_output_file_success(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            local_fh = "tests/nyp-ebook-sample.mrc"
            drive.output_file(local_fh)

            assert drive.sftp.listdir(drive.dst_dir) == ["nyp-ebook-sample.mrc"]

            # cleanup
            drive.sftp.remove(drive.dst_dir + "/nyp-ebook-sample.mrc")

    def test_output_file_io_error(self, caplog, live_sftp_env, mock_io_error):
        creds = get_credentials()
        with Drive(*creds) as drive:
            with caplog.at_level(logging.ERROR):
                with pytest.raises(DriveError):
                    drive.output_file("foo.mrc")
        assert "IOError. Unable to create /NSDROP/TEST/load/foo.mrc on the SFTP."


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
        assert "Attempted an operation on a closed SFTP session." in caplog.text

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

    def test_list_src_directory_no_sftp_session(self, caplog, mock_drive):
        mock_drive.sftp = None
        with caplog.at_level(logging.ERROR):
            mock_drive.list_src_directory()

        assert "Attempted an operation on a closed SFTP session." in caplog.text

    def test_output_file(self, sftpserver, mock_drive, tmpdir):
        tmpfile = tmpdir.join("foo.mrc")
        content = "spam"
        tmpfile.write(content)
        with sftpserver.serve_content({"load_dir": {"foo.mrc": "spam"}}):
            mock_drive.output_file(str(tmpfile))
            assert mock_drive.sftp.listdir("load_dir") == ["foo.mrc"]

    def test_output_file_io_error(self, caplog, mock_drive, mock_io_error):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DriveError):
                mock_drive.output_file("foo.mrc")
        assert (
            "IOError. Unable to output foo.mrc to load_dir/foo.mrc on the SFTP."
            in caplog.text
        )

    def test_output_file_closed_sftp_session(self, caplog, mock_drive):
        mock_drive.sftp = None
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DriveError):
                mock_drive.output_file("TEMP/foo.mrc")

        assert (
            "SFTP session closed. Unable to output TEMP/foo.mrc to load_dir/foo.mrc on the drive."
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
