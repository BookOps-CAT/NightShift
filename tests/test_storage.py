import os

from nightshift.comms.storage import get_credentials, Drive


def test_get_credentials(mock_sftp_env):
    assert get_credentials() == (
        "sftp_host",
        "sftp_user",
        "sftp_password",
        "nightshift_home_dir",
    )


# class TestLiveDrive:
#     def test_init(self, live_sftp_env):
#         creds = get_credentials()
#         with Drive(*creds) as sftp:
#             print(type(sftp))
#             # sftp = Drive.sftp()
#             print(sftp.list_directory())
