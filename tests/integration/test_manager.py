from contextlib import nullcontext as does_not_raise
from datetime import datetime

import pytest

from nightshift.comms.storage import get_credentials, Drive
from nightshift.datastore import Resource
from nightshift.manager import process_resources


@pytest.mark.firewalled
def test_process_resources_live(env_var, test_data_rich, test_session):
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
        assert res.status == "bot_enhanced"
        assert res.enhanceTimestamp is not None

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
