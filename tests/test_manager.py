from datetime import datetime
import logging

from bookops_worldcat.errors import WorldcatSessionError
import pytest

from nightshift.datastore import dal, session_scope, Resource

# from nightshift.manager import get_worldcat_brief_bib_matches, get_worldcat_full_bibs

from .conftest import (
    MockSuccessfulHTTP200SessionResponse,
    MockSuccessfulHTTP200SessionResponseNoMatches,
)


LOGGER = logging.getLogger(__name__)


# def test_processing_resources_logging(caplog, test_connection, test_session, test_data):
#     # from nightshift.bot import run

#     dal.conn = test_connection

#     bib_date = datetime.utcnow().date()
#     test_session.add(
#         Resource(
#             sierraId=11111111,
#             libraryId=1,
#             resourceCategoryId=1,
#             bibDate=bib_date,
#             distributorNumber="123",
#             sourceId=1,
#             status="open",
#         )
#     )

#     with caplog.at_level(logging.INFO):
#         process_resources()
#     assert "Processing NYP new resources" in caplog.text
#     assert "Processing BPL new resources" in caplog.text


# def test_get_worldcat_brief_bib_matches_success(
#     test_session,
#     test_data_core,
#     mock_worldcat_creds,
#     mock_successful_post_token_response,
#     mock_successful_session_get_request,
# ):
#     test_session.add(
#         Resource(
#             nid=1,
#             sierraId=11111111,
#             libraryId=1,
#             resourceCategoryId=1,
#             sourceId=1,
#             bibDate=datetime.utcnow().date(),
#             title="Pride and prejudice.",
#             distributorNumber="123",
#             status="open",
#         )
#     )
#     test_session.commit()
#     resources = test_session.query(Resource).filter_by(nid=1).all()
#     get_worldcat_brief_bib_matches(test_session, "NYP", resources)

#     res = test_session.query(Resource).filter_by(nid=1).all()[0]
#     query = res.queries[0]
#     assert query.nid == 1
#     assert query.match
#     assert query.response == MockSuccessfulHTTP200SessionResponse().json()
#     assert res.oclcMatchNumber == "44959645"
#     assert res.status == "matched"


# def test_get_worldcat_brief_bib_matches_failed(
#     test_session,
#     test_data_core,
#     mock_worldcat_creds,
#     mock_successful_post_token_response,
#     mock_successful_session_get_request_no_matches,
# ):
#     test_session.add(
#         Resource(
#             nid=1,
#             sierraId=11111111,
#             libraryId=1,
#             resourceCategoryId=1,
#             sourceId=1,
#             bibDate=datetime.utcnow().date(),
#             title="Pride and prejudice.",
#             distributorNumber="123",
#             status="open",
#         )
#     )
#     test_session.commit()
#     resources = test_session.query(Resource).filter_by(nid=1).all()
#     get_worldcat_brief_bib_matches(test_session, "NYP", resources)

#     res = test_session.query(Resource).filter_by(nid=1).all()[0]
#     query = res.queries[0]
#     assert query.nid == 1
#     assert query.resourceId == 1
#     assert query.match is False
#     assert query.response == MockSuccessfulHTTP200SessionResponseNoMatches().json()
#     assert res.oclcMatchNumber == None
#     assert res.status == "open"


# def test_get_worldcat_brief_bib_session_error(
#     test_session,
#     test_data_core,
#     mock_worldcat_creds,
#     mock_successful_post_token_response,
#     mock_session_error,
# ):
#     test_session.add(
#         Resource(
#             nid=1,
#             sierraId=11111111,
#             libraryId=1,
#             resourceCategoryId=1,
#             sourceId=1,
#             bibDate=datetime.utcnow().date(),
#             title="Pride and prejudice.",
#             distributorNumber="123",
#             status="open",
#         )
#     )
#     test_session.commit()
#     resources = test_session.query(Resource).filter_by(nid=1).all()
#     with pytest.raises(WorldcatSessionError):
#         get_worldcat_brief_bib_matches(test_session, "NYP", resources)


# def test_get_worldcat_full_bibs(
#     test_session,
#     test_data_core,
#     mock_worldcat_creds,
#     mock_successful_post_token_response,
#     mock_successful_session_get_request,
# ):
#     test_session.add(
#         Resource(
#             nid=1,
#             sierraId=11111111,
#             libraryId=1,
#             resourceCategoryId=1,
#             sourceId=1,
#             bibDate=datetime.utcnow().date(),
#             title="Pride and prejudice.",
#             distributorNumber="123",
#             status="matched",
#             oclcMatchNumber="44959645",
#         )
#     )
#     test_session.commit()
#     resources = test_session.query(Resource).filter_by(nid=1).all()
#     get_worldcat_full_bibs(test_session, "NYP", resources)

#     res = test_session.query(Resource).filter_by(nid=1).all()[0]
#     assert res.fullBib == MockSuccessfulHTTP200SessionResponse().content
