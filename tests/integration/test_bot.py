"""
Top level tests.
"""

import logging

import pytest


from nightshift.bot import run
from nightshift.comms.storage import Drive, get_credentials


@pytest.mark.local
def test_logger_on_run(env_var, caplog, test_data):

    with caplog.at_level(logging.INFO):
        run(env="prod")

    assert "Launching prod NightShift..." in caplog.text
    assert "Processing resources completed." in caplog.text
    assert "Database maintenance completed." in caplog.text

    # clean up
    drive_creds = get_credentials()
    with Drive(*drive_creds) as drive:
        temp_files = drive.sftp.listdir(path=drive.dst_dir)
        for file_handle in temp_files:
            drive.sftp.remove(f"{drive.dst_dir}/{file_handle}")
