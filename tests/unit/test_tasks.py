# -*- coding: utf-8 -*-
from datetime import datetime
import logging

import pytest

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


def test_check_resources_sierra_state_nyp_platform(
    test_session,
    test_data_core,
    stub_resource,
    mock_platform_env,
    mock_successful_platform_post_token_response,
    mock_successful_platform_session_response,
):
    stub_resource.suppressed = True
    stub_resource.status = "open"
    test_session.add(stub_resource)
    test_session.commit()

    check_resources_sierra_state(test_session, "NYP", [stub_resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.suppressed is False
    assert resource.status == "upgraded_staff"


def test_check_resources_sierra_state_bpl_solr(
    test_session,
    test_data_core,
    stub_resource,
    mock_solr_env,
    mock_successful_solr_session_response,
):
    stub_resource.suppressed = False
    stub_resource.status = "expired"
    test_session.add(stub_resource)
    test_session.commit()

    check_resources_sierra_state(test_session, "BPL", [stub_resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.suppressed
    assert resource.status == "open"


def test_check_resources_sierra_state_invalid_library_arg(caplog):
    with pytest.raises(ValueError):
        with caplog.at_level(logging.ERROR):
            check_resources_sierra_state(None, "QPL", [])

    assert "Invalid library argument passed: 'QPL'. Must be 'NYP' or 'BPL'"


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
