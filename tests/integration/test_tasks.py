# -*- coding: utf-8 -*-

import pytest
from sqlalchemy import update

from nightshift.datastore import Resource
from nightshift.tasks import Tasks


@pytest.mark.local
def test_get_worldcat_brief_bib_matches_success(
    test_session, test_data_rich, env_var, stub_res_cat_by_name
):

    resource = test_session.query(Resource).filter_by(nid=1).one()
    resource.distributorNumber = "622708F6-78D7-453A-A7C5-3FE6853F3167"
    resource.status = "open"
    resource.fullBib = None
    resource.oclcMatchNumber = None
    resource.upgradeTimestamp = None

    test_session.commit()

    resources = test_session.query(Resource).filter_by(nid=1).all()

    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks.get_worldcat_brief_bib_matches(resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query_record = res.queries[1]
    assert query_record.nid == 2
    assert query_record.match
    assert isinstance(query_record.response, dict)
    assert query_record.timestamp is not None
    assert res.oclcMatchNumber == "779356905"
    assert res.status == "open"


@pytest.mark.local
def test_get_worldcat_brief_bib_matches_failure(
    test_session, test_data_rich, env_var, stub_res_cat_by_name
):
    # modify existing resource
    resource = test_session.query(Resource).filter_by(nid=1).one()
    resource.distributorNumber = "ABC#1234"
    resource.status = "open"
    resource.fullBib = None
    resource.oclcMatchNumber = None
    resource.upgradeTimestamp = None

    test_session.commit()

    resources = test_session.query(Resource).filter_by(nid=1).all()
    assert len(resources) == 1

    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks.get_worldcat_brief_bib_matches(resources)

    res = test_session.query(Resource).filter_by(nid=1).one()
    assert res.oclcMatchNumber is None
    assert res.status == "open"

    query_record = res.queries[1]
    assert query_record.nid == 2
    assert query_record.match is False
    assert isinstance(query_record.response, dict)
    assert query_record.timestamp is not None


@pytest.mark.local
def test_get_worldcat_full_bibs(
    test_session, test_data_rich, env_var, stub_res_cat_by_name
):
    test_session.execute(
        update(Resource).where(Resource.nid == 1).values(oclcMatchNumber="779356905")
    )

    test_session.commit()

    resources = test_session.query(Resource).filter_by(nid=1).all()
    assert len(resources) == 1

    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks.get_worldcat_full_bibs(resources)

    res = test_session.query(Resource).filter_by(nid=1).one()
    assert isinstance(res.fullBib, bytes)
