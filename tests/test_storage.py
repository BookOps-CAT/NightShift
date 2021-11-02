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

    def test_determine_dst_file_handle(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert (
                drive._determine_drive_file_handle("/test/foo.mrc")
                == "/NSDROP/TEST/load/foo.mrc"
            )

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
        with sftpserver.serve_content({"sierra_dumps_dir": {}}):
            with Drive(*creds) as drive:
                assert isinstance(drive.sftp, paramiko.sftp_client.SFTPClient)
                assert drive.src_dir == "sierra_dumps_dir"
                assert drive.dst_dir == "load_dir"

    @pytest.mark.parametrize(
        "host,port,expectation",
        [
            ("foo", "22", "foo:22"),
            ("foo", None, "foo"),
        ],
    )
    def test_sock(self, host, port, expectation, mock_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert drive._sock(host, port) == expectation

    def test_list_src_directory(self, sftpserver, mock_sftp_env):
        creds = get_credentials()
        with sftpserver.serve_content(
            {"sierra_dumps_dir": {"foo1.mrc": "foo", "foo2.mrc": "foo"}}
        ):
            with Drive(*creds) as drive:
                result = drive.list_src_directory()
            assert result == ["foo1.mrc", "foo2.mrc"]

    def test_determine_dst_file_handle(self, mock_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert (
                drive._determine_drive_file_handle("/test/foo.mrc")
                == "load_dir/foo.mrc"
            )

    def test_output_file(self, sftpserver, mock_sftp_env, tmpdir):
        tmpfile = tmpdir.join("foo.mrc")
        content = "spam"
        tmpfile.write(content)
        creds = get_credentials()
        with sftpserver.serve_content({"load_dir": {"foo.mrc": "spam"}}):
            with Drive(*creds) as drive:
                drive.output_file(str(tmpfile))
                assert drive.sftp.listdir("load_dir") == ["foo.mrc"]

    def test_output_file_io_error(self, caplog, mock_sftp_env, mock_io_error):
        creds = get_credentials()
        with Drive(*creds) as drive:
            with caplog.at_level(logging.ERROR):
                with pytest.raises(DriveError):
                    drive.output_file("foo.mrc")
        assert "IOError. Unable to create /load/foo.mrc on the SFTP."

    def test_fetch_file(self, sftpserver, mock_sftp_env, tmpdir):
        creds = get_credentials()
        with sftpserver.serve_content({"/sierra_dump_dir": {"foo.mrc": b"shrubbery"}}):
            with Drive(*creds) as drive:
                result = drive.fetch_file("foo.mrc")
                assert isinstance(result, BytesIO)
                assert result.read() == b"shrubbery"
