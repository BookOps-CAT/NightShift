from contextlib import nullcontext as does_not_raise
from io import BytesIO
import logging
import pytest

from nightshift.comms.storage import get_credentials, Drive


def test_get_credentials(mock_sftp_env, sftpserver):
    assert get_credentials() == (
        sftpserver.host,
        str(sftpserver.port),
        "nightshift",
        "sftp_password",
        "sierra_dumps_dir",
        "load_dir",
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
            path = drive.src_dir + "/NYPeres210701.pout"
            with does_not_raise():
                file = drive.fetch_file(path)
            assert isinstance(file, BytesIO)

    def test_output_bib_to_file(self, live_sftp_env):
        pass

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
                with pytest.raises(IOError):
                    drive.output_file("foo.mrc")
        assert "IOError. Unable to create /NSDROP/TEST/load/foo.mrc on the SFTP."


# class TestDriveMocked:
#     def test_sftp_home_directory_on_init(self, sftpserver, mock_sftp_env):
#         with sftpserver.serve_content({"sierra_dumps": ["file1.mrc", "file2.mrc"]}):
#             creds = get_credentials()
#             print(creds)
# with Drive(*creds) as drive:
#     print(drive.list_directory())
# assert drive.sftp.getcwd() == "/NSDROP/sierra_dumps/nightshift"
