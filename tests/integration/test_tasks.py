# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy import update

from nightshift.datastore import Resource
from nightshift.tasks import (
    get_worldcat_brief_bib_matches,
    get_worldcat_full_bibs,
    ingest_new_files,
)


@pytest.mark.local
def test_get_worldcat_brief_bib_matches_success(test_session, test_data, env_var):
    test_session.execute(
        update(Resource)
        .where(Resource.sierraId == 11111111, Resource.libraryId == 1)
        .values(distributorNumber="622708F6-78D7-453A-A7C5-3FE6853F3167")
    )
    resources = test_session.query(Resource).filter_by(nid=1).all()

    get_worldcat_brief_bib_matches(test_session, "NYP", resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query_record = res.queries[0]
    assert query_record.nid == 1
    assert query_record.match
    assert isinstance(query_record.response, dict)
    assert query_record.timestamp is not None
    assert res.oclcMatchNumber == "779356905"
    assert res.status == "open"


@pytest.mark.local
def test_get_worldcat_brief_bib_matches_failure(test_session, test_data, env_var):
    # modify existing resource
    test_session.execute(
        update(Resource)
        .where(Resource.nid == 1)
        .values(distributorNumber="ABC#1234", oclcMatchNumber=None)
    )
    test_session.commit()

    resources = test_session.query(Resource).filter_by(nid=1).all()

    get_worldcat_brief_bib_matches(test_session, "NYP", resources)

    res = test_session.query(Resource).filter_by(nid=1).one()
    assert res.oclcMatchNumber is None
    assert res.status == "open"

    query_record = res.queries[0]
    assert query_record.nid == 1
    assert query_record.match is False
    assert isinstance(query_record.response, dict)
    assert query_record.timestamp is not None


@pytest.mark.local
def test_get_worldcat_full_bibs(test_session, test_data, env_var):
    test_session.execute(
        update(Resource).where(Resource.nid == 1).values(oclcMatchNumber="779356905")
    )

    test_session.commit()

    resources = test_session.query(Resource).filter_by(nid=1).all()
    assert len(resources) == 1

    get_worldcat_full_bibs(test_session, "NYP", resources)

    res = test_session.query(Resource).filter_by(nid=1).one()
    assert isinstance(res.fullBib, bytes)


def test_ingest_new_files(
    env_var,
    test_data_core,
    test_session,
    mock_sftp_env,
    mock_drive_unprocessed_files,
    mock_drive_fetch_file,
):
    with does_not_raise():
        ingest_new_files(test_session, "NYP", 1)

    res = test_session.query(Resource).all()
    assert len(res) == 2
    assert res[0].standardNumber == "9780071830744"
    assert res[0].resourceCategoryId == 1
    assert res[1].standardNumber == "9780553906899"
    assert res[1].resourceCategoryId == 1
