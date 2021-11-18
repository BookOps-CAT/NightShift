# -*- coding: utf-8 -*-
import datetime

import pytest
from sqlalchemy import update

from nightshift.datastore import Resource
from nightshift.tasks import get_worldcat_brief_bib_matches


@pytest.mark.local
def test_get_worldcat_brief_bib_matches_success(local_db, test_data, test_nyp_worldcat):
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
