from contextlib import nullcontext as does_not_raise
from datetime import datetime, timedelta
import logging
import os

import pytest

from nightshift.comms.storage import get_credentials, Drive
from nightshift.constants import RESOURCE_CATEGORIES
from nightshift.datastore import Resource, WorldcatQuery
from nightshift.manager import process_resources, perform_db_maintenance


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

    def test_new_resources(
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

    def test_older_resources(
        self,
        env_var,
        test_session,
        test_data,
        mock_sftp_env,
        stub_resource,
        mock_drive_unprocessed_files_empty,
        mock_check_resources_sierra_state_open,
        mock_worldcat_brief_bib_matches,
        mock_get_worldcat_full_bibs,
        mock_transfer_to_drive,
    ):
        resource = stub_resource
        resource.status = "open"
        resource.fullBib = None
        resource.oclcMatchNumber = None
        resource.upgradeTimestamp = None
        resource.queries = [
            WorldcatQuery(
                match=False,
                timestamp=datetime.utcnow().date() - timedelta(days=31),
            )
        ]
        resource.outputId = None

        test_session.add(resource)
        test_session.commit()

        with does_not_raise():
            process_resources()

        results = test_session.query(Resource).all()
        assert len(results) == 1

        res = results[0]

        assert len(res.queries) == 2
        assert res.queries[0].match is False
        assert res.queries[1].match is True
        assert res.oclcMatchNumber is not None
        assert res.fullBib is not None
        assert res.outputId is not None
        assert res.status == "upgraded_bot"
        assert res.upgradeTimestamp is not None


@pytest.mark.parametrize(
    "age,status,tally", [(91, "open", 0), (179, "open", 0), (181, "expired", 1)]
)
def test_perform_db_maintenance_set_expired(
    caplog, env_var, test_session, test_data, age, status, tally
):
    # expired resource
    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.utcnow().date() - timedelta(days=age),
            queries=[WorldcatQuery(match=False)],
        )
    )
    test_session.commit()

    with caplog.at_level(logging.INFO):
        perform_db_maintenance()

    assert f"Changed {tally} ebook resource(s) status to 'expired'." in caplog.text

    resource = test_session.query(Resource).filter_by(sierraId=22222222).one()
    assert resource.status == status


@pytest.mark.parametrize(
    "age,tally,expectation",
    [(91, 0, Resource), (191, 0, Resource), (300, 1, type(None))],
)
def test_perform_db_maintenance_delete(
    caplog, env_var, test_session, test_data, age, tally, expectation
):
    expiration_age = RESOURCE_CATEGORIES["ebook"]["query_days"][-1][1]

    # expired resource
    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.utcnow().date() - timedelta(days=age),
            queries=[WorldcatQuery(match=False)],
        )
    )
    test_session.commit()

    with caplog.at_level(logging.INFO):
        perform_db_maintenance()

    assert (
        f"Deleted {tally} ebook resource(s) older than {expiration_age + 90} days from the database."
        in caplog.text
    )
    resource = test_session.query(Resource).filter_by(sierraId=22222222).one_or_none()
    assert isinstance(resource, expectation)
