# -*- coding: utf-8 -*-
import datetime

import pytest
from sqlalchemy import update

from nightshift.datastore import Resource
from nightshift.tasks import get_worldcat_brief_bib_matches, get_worldcat_full_bibs


@pytest.mark.local
def test_get_worldcat_brief_bib_matches_success(local_db, test_data, test_nyp_worldcat):
    local_db.execute(
        update(Resource)
        .where(Resource.sierraId == 11111111, Resource.libraryId == 1)
        .values(distributorNumber="622708F6-78D7-453A-A7C5-3FE6853F3167")
    )
    resources = local_db.query(Resource).filter_by(nid=1).all()

    get_worldcat_brief_bib_matches(local_db, test_nyp_worldcat, resources)

    res = local_db.query(Resource).filter_by(nid=1).all()[0]
    query_record = res.queries[0]
    assert query_record.nid == 1
    assert query_record.match
    assert isinstance(query_record.response, dict)
    assert query_record.timestamp is not None
    assert res.oclcMatchNumber == "779356905"
    assert res.status == "open"


@pytest.mark.local
def test_get_worldcat_brief_bib_matches_failure(local_db, test_data, test_nyp_worldcat):
    # modify existing resource
    local_db.execute(
        update(Resource).where(Resource.nid == 1).values(distributorNumber="ABC#1234")
    )
    local_db.commit()

    resources = local_db.query(Resource).filter_by(nid=1).all()

    get_worldcat_brief_bib_matches(local_db, test_nyp_worldcat, resources)

    res = local_db.query(Resource).filter_by(nid=1).one()
    assert res.oclcMatchNumber is None
    assert res.status == "open"

    query_record = res.queries[0]
    assert query_record.nid == 1
    assert query_record.match is False
    assert isinstance(query_record.response, dict)
    assert query_record.timestamp is not None


@pytest.mark.local
def test_get_worldcat_full_bibs(local_db, test_data, test_nyp_worldcat):
    local_db.execute(
        update(Resource).where(Resource.nid == 1).values(oclcMatchNumber="779356905")
    )

    local_db.commit()

    resources = local_db.query(Resource).filter_by(nid=1).all()
    assert len(resources) == 1

    get_worldcat_full_bibs(local_db, test_nyp_worldcat, resources)

    res = local_db.query(Resource).filter_by(nid=1).one()
    assert isinstance(res.fullBib, bytes)