from contextlib import nullcontext as does_not_raise
from datetime import datetime
import os

import pytest

from nightshift.comms.storage import get_credentials, Drive
from nightshift.datastore import Resource
from nightshift.manager import process_resources


@pytest.mark.firewalled
@pytest.mark.local
def test_process_resources_live(env_var, test_data, test_session):
    """
    This tests runs using real data and live services
    """
    with does_not_raise():
        process_resources()

    # assert here changes in the db
    # and correct manipulations to bibs
    # retrieve manipulated file in SFTP and examine

    # db
    results = test_session.query(Resource).all()
    assert len(results) == 3
    for res in results:
        assert (
            len(res.queries) == 1
        )  # initial stub resource indicates it was already successfully searched
        assert res.oclcMatchNumber is not None
        assert res.fullBib is not None
        assert res.outputId is not None
        assert res.status == "upgraded_bot"
        assert res.upgradeTimestamp is not None

    # SFTP
    drive_creds = get_credentials()
    with Drive(*drive_creds) as drive:

        temp_files = drive.sftp.listdir(path=drive.dst_dir)

        today = datetime.utcnow().date()
        assert temp_files == [
            f"{today:%y%m%d}-NYP-ebook-01.mrc",
            f"{today:%y%m%d}-BPL-ebook-01.mrc",
        ]

        for file_handle in temp_files:
            with drive.sftp.file(f"{drive.dst_dir}/{file_handle}", mode="r") as file:
                file_size = file.stat().st_size
                assert file_size > 0

        # cleanup (fix needed, make sure it always happens)
        for file_handle in temp_files:
            drive.sftp.remove(f"{drive.dst_dir}/{file_handle}")


class TestProcessResourcesMocked:
    """
    These test runs using higher level patching: mocked SFTP, Worldcat, NYPL Platform
    and BPL Solr. It still uses local Postgres db
    """

    def test_blank_state_db(
        self,
        env_var,
        test_session,
        test_data_core,
        mock_sftp_env,
        mock_drive_unprocessed_files,
        mock_drive_fetch_file,
        mock_worldcat_brief_bib_matches,
        mock_check_resources_sierra_state_open,
        mock_get_worldcat_full_bibs,
        mock_transfer_to_drive,
    ):
        with does_not_raise():
            process_resources()

        results = test_session.query(Resource).all()
        assert len(results) == 2
        for res in results:
            assert len(res.queries) == 1
            assert res.oclcMatchNumber is not None
            assert res.fullBib is not None
            assert res.outputId is not None
            assert res.status == "upgraded_bot"
            assert res.upgradeTimestamp is not None

    def test_older_resources(self, env_var, test_data, mock_sftp_env):
        pass
