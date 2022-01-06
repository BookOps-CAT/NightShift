from contextlib import nullcontext as does_not_raise

# from datetime import datetime

import pytest

from nightshift.comms.storage import get_credentials, Drive
from nightshift.manager import process_resources


@pytest.mark.firewalled
@pytest.mark.local
def test_process_resources_live(env_var, test_data):
    """
    This tests runs using real data and live services
    """
    with does_not_raise():
        process_resources()

    # cleanup
    drive_creds = get_credentials()
    with Drive(*drive_creds) as drive:
        temp_files = drive.sftp.listdir(path=drive.dst_dir)
        for file_handle in temp_files:
            drive.sftp.remove(f"{drive.dst_dir}/{file_handle}")


class TestProcessResourcesMocked:
    """
    These test runs using mocked SFTP, Worldcat, NYPL Platform and BPL Solr,
    and local Postgres db
    """

    def test_blank_state_db(
        self,
        env_var,
        test_data_core,
        mock_sftp_env,
        mock_drive_unprocessed_files,
        mock_drive_fetch_file,
        mock_worldcat_brief_bib_matches,
        mock_check_resources_sierra_status,
        mock_get_worldcat_full_bibs,
    ):
        with does_not_raise():
            process_resources()
