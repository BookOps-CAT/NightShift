# -*- coding: utf-8 -*-
from datetime import datetime


from nightshift.datastore import Resource
from nightshift.tasks import (
    check_resources_sierra_state,
    get_worldcat_brief_bib_matches,
    get_worldcat_full_bibs,
)

from .conftest import (
    MockSuccessfulHTTP200SessionResponse,
    MockSuccessfulHTTP200SessionResponseNoMatches,
)


def test_check_resources_sierra_state(test_session, test_data_core):
    pass


def test_get_worldcat_brief_bib_matches_success(
    test_session,
    test_data_core,
    mock_Worldcat,
    mock_successful_session_get_request,
):
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=1,
            resourceCategoryId=1,
            sourceId=1,
            bibDate=datetime.utcnow().date(),
            title="Pride and prejudice.",
            distributorNumber="123",
            status="open",
        )
    )
    test_session.commit()
    resources = test_session.query(Resource).filter_by(nid=1).all()
    get_worldcat_brief_bib_matches(test_session, mock_Worldcat, resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query = res.queries[0]
    assert query.nid == 1
    assert query.match
    assert query.response == MockSuccessfulHTTP200SessionResponse().json()
    assert res.oclcMatchNumber == "44959645"
    assert res.status == "open"


def test_get_worldcat_brief_bib_matches_failed(
    test_session,
    test_data_core,
    mock_Worldcat,
    mock_successful_session_get_request_no_matches,
):
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=1,
            resourceCategoryId=1,
            sourceId=1,
            bibDate=datetime.utcnow().date(),
            title="Pride and prejudice.",
            distributorNumber="123",
            status="open",
        )
    )
    test_session.commit()
    resources = test_session.query(Resource).filter_by(nid=1).all()
    get_worldcat_brief_bib_matches(test_session, mock_Worldcat, resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query = res.queries[0]
    assert query.nid == 1
    assert query.resourceId == 1
    assert query.match is False
    assert query.response == MockSuccessfulHTTP200SessionResponseNoMatches().json()
    assert res.oclcMatchNumber is None
    assert res.status == "open"


def test_get_worldcat_full_bibs(
    test_session,
    test_data_core,
    mock_Worldcat,
    mock_successful_session_get_request,
):
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=1,
            resourceCategoryId=1,
            sourceId=1,
            bibDate=datetime.utcnow().date(),
            title="Pride and prejudice.",
            distributorNumber="123",
            status="open",
            oclcMatchNumber="44959645",
        )
    )
    test_session.commit()
    resources = test_session.query(Resource).filter_by(nid=1).all()
    get_worldcat_full_bibs(test_session, mock_Worldcat, resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    assert res.fullBib == MockSuccessfulHTTP200SessionResponse().content
