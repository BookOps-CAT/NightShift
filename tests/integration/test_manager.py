from contextlib import nullcontext as does_not_raise

# from datetime import datetime

import pytest

from nightshift.comms.storage import get_credentials, Drive
from nightshift.manager import process_resources


@pytest.mark.firewalled
@pytest.mark.local
def test_process_resources(env_var, test_data):
    with does_not_raise():
        process_resources()

    # cleanup
    drive_creds = get_credentials()
    with Drive(*drive_creds) as drive:
        temp_files = drive.sftp.listdir(path=drive.dst_dir)
        for file_handle in temp_files:
            drive.sftp.remove(f"{drive.dst_dir}/{file_handle}")


def test_process_resources_mocked():
    pass
