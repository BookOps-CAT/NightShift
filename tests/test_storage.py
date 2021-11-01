from contextlib import nullcontext as does_not_raise
from io import BytesIO
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

    def test_fetch_src_file(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            path = (
                drive.src_dir + "/NYPeres210701.pout"
            )  # how to do it with os.path.join?
            with does_not_raise():
                file = drive.fetch_src_file(path)
            assert isinstance(file, BytesIO)

    def test_output_bib_to_file(self, live_sftp_env, stub_marc):
        creds = get_credentials()
        with Drive(*creds) as drive:
            path = drive.dst_dir + "/test.mrc"
            data = stub_marc.as_marc()
            drive.output_bib_to_file(path, data)


# class TestDriveMocked:
#     def test_sftp_home_directory_on_init(self, sftpserver, mock_sftp_env):
#         with sftpserver.serve_content({"sierra_dumps": ["file1.mrc", "file2.mrc"]}):
#             creds = get_credentials()
#             print(creds)
# with Drive(*creds) as drive:
#     print(drive.list_directory())
# assert drive.sftp.getcwd() == "/NSDROP/sierra_dumps/nightshift"
