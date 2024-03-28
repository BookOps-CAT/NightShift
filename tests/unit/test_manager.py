from contextlib import nullcontext as does_not_raise
from datetime import datetime, timedelta, timezone
import logging

import pytest

from nightshift.comms.storage import get_credentials, Drive
from nightshift.constants import RESOURCE_CATEGORIES
from nightshift.datastore import Event, Resource, WorldcatQuery
from nightshift.manager import process_resources, perform_db_maintenance


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
            assert res.status == "bot_enhanced"
            assert res.enhanceTimestamp is not None

    def test_older_resources(
        self,
        env_var,
        test_session,
        test_data_rich,
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
        resource.enhanceTimestamp = None
        resource.queries = [
            WorldcatQuery(
                match=False,
                timestamp=datetime.now(timezone.utc).date() - timedelta(days=31),
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
        assert res.status == "bot_enhanced"
        assert res.enhanceTimestamp is not None


@pytest.mark.parametrize(
    "age,status,tally", [(91, "open", 0), (179, "open", 0), (181, "expired", 1)]
)
def test_perform_db_maintenance_set_expired(
    caplog, env_var, test_session, test_data_rich, age, status, tally
):
    # expired resource
    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.now(timezone.utc).date() - timedelta(days=age),
            queries=[WorldcatQuery(match=False)],
        )
    )
    test_session.commit()

    with caplog.at_level(logging.INFO):
        perform_db_maintenance()

    assert f"Changed {tally} ebook resource(s) status to 'expired'." in caplog.text

    resource = test_session.query(Resource).filter_by(sierraId=22222222).one()
    assert resource.status == status

    # check the Event table
    event = test_session.query(Event).filter_by(sierraId=22222222).one_or_none()
    if tally == 0:
        assert event is None
    else:
        assert event is not None
        assert event.status == "expired"
        assert event.timestamp.date() == datetime.now(timezone.utc).date()


@pytest.mark.parametrize(
    "age,tally,expectation",
    [(91, 0, Resource), (191, 0, Resource), (300, 1, type(None))],
)
def test_perform_db_maintenance_delete(
    caplog,
    env_var,
    test_session,
    test_data_rich,
    stub_res_cat_by_name,
    age,
    tally,
    expectation,
):
    expiration_age = stub_res_cat_by_name["ebook"].queryDays[-1][1]

    # expired resource
    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.now(timezone.utc).date() - timedelta(days=age),
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
