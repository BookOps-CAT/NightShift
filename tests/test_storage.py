import os

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


class TestDriveLive:
    def test_init(self, live_sftp_env):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert drive.list_directory() == ["test1.txt"]


# class TestDriveMocked:
#     def test_sftp_home_directory_on_init(self, sftpserver, mock_sftp_env):
#         with sftpserver.serve_content({"sierra_dumps": ["file1.mrc", "file2.mrc"]}):
#             creds = get_credentials()
#             print(creds)
# with Drive(*creds) as drive:
#     print(drive.list_directory())
# assert drive.sftp.getcwd() == "/NSDROP/sierra_dumps/nightshift"
