from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from nightshift.datastore import Event, Resource, WorldcatQuery
from nightshift.manager import perform_db_maintenance, process_resources


class TestProcessResourcesMocked:
    def test_new_resources(
        self,
        test_data_core,
        mock_drive,
        mock_drive_unprocessed_files,
        mock_drive_fetch_file,
        mock_worldcat_brief_bib_matches,
        mock_check_resources_sierra_state_open,
        mock_get_worldcat_full_bibs,
        mock_transfer_to_drive,
        db_provider,
    ):
        process_resources(db_provider)

        with db_provider.session_scope() as session:
            results = list(session.scalars(select(Resource)).all())
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
        test_data_rich,
        mock_env_vars,
        mock_drive,
        stub_resource,
        mock_drive_unprocessed_files_empty,
        mock_check_resources_sierra_state_open,
        mock_worldcat_brief_bib_matches,
        mock_get_worldcat_full_bibs,
        mock_transfer_to_drive,
        db_provider,
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

        with db_provider.session_scope() as session:
            session.add(resource)

        process_resources(db_provider)

        with db_provider.session_scope() as session:
            results = list(session.scalars(select(Resource)).all())
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
    "age, status, tally",
    [(179, "open", 0), (181, "expired", 1)],
    ids=["under_expiration_threshold", "over_expiration_threshold"],
)
def test_perform_db_maintenance_set_expired(
    caplog, test_data_rich, age, status, tally, db_provider
):
    with db_provider.session_scope() as session:
        session.add(
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

    perform_db_maintenance(db_provider)

    assert f"Changed {tally} ebook resource(s) status to 'expired'." in caplog.text

    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).filter_by(sierraId=22222222)).one()
        assert resource.status == status

        event = session.scalars(
            select(Event).filter_by(sierraId=22222222)
        ).one_or_none()
        if tally == 0:
            assert event is None
        else:
            assert event is not None
            assert event.status == "expired"
            assert event.timestamp.date() == datetime.now(timezone.utc).date()


@pytest.mark.parametrize(
    "age, tally, expectation",
    [(191, 0, True), (300, 1, False)],
    ids=["under_deletion_threshold", "over_deletion_threshold"],
)
def test_perform_db_maintenance_delete(
    caplog, db_provider, test_data_rich, stub_res_cat_by_name, age, tally, expectation
):
    expiration_age = stub_res_cat_by_name["ebook"].queryDays[-1][1]

    with db_provider.session_scope() as session:
        session.add(
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

    perform_db_maintenance(db_provider)

    assert (
        f"Deleted {tally} ebook resource(s) older than {expiration_age + 90} days from the database."
        in caplog.text
    )

    with db_provider.session_scope() as session:
        resource = session.scalars(
            select(Resource).filter_by(sierraId=22222222)
        ).one_or_none()
        assert (resource is not None) == expectation
