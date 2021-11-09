# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
import logging

from bookops_nypl_platform import PlatformToken
import pytest

from nightshift import __title__, __version__
from nightshift.ns_exceptions import SierraSearchPlatformError
from nightshift.comms.sierra_search_platform import (
    is_eresource_callno,
    BplSolr,
    NypPlatform,
    SearchResponse,
)

from .conftest import (
    MockPlatformSessionResponseNotFound,
    MockPlatformSessionResponseSuccess,
    MockSearchSession500HTTPError,
    MockSolrSessionResponseSuccess,
    MockSolrSessionResponseNotFound,
)


@pytest.mark.parametrize(
    "arg,expectation",
    [
        ("", False),
        (None, False),
        (123, False),
        ("B ADAMS C", False),
        ("eNYPL Book", True),
        ("eNYPL Audio", True),
        ("eNYPL Video", True),
        ("eBOOK", True),
        ("eAUDIO", True),
        ("eVIDEO", True),
    ],
)
def test_is_eresource_callno(arg, expectation):
    assert is_eresource_callno(arg) == expectation


class TestSearchResponse:
    def test_init(self):
        response = MockSolrSessionResponseSuccess()
        with does_not_raise():
            sr = SearchResponse(11111111, "bpl", response)

        assert sr.library == "bpl"
        assert sr.sierraId == 11111111
        assert sr.response.json() == response.json()

    @pytest.mark.parametrize("arg", ["nyp", "bpl"])
    def test_500_error_response(self, arg, caplog):
        response = MockSearchSession500HTTPError()
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SierraSearchPlatformError):
                sr = SearchResponse(22222222, "nyp", response)

        assert f"{arg.upper()} search platform returned HTTP error code: 500 for request query_url_here"

    def test_nyp_suppression_not_found_response(self):
        response = MockPlatformSessionResponseNotFound()
        sr = SearchResponse(11111111, "nyp", response)
        assert sr._nyp_suppression() is False

    @pytest.mark.parametrize("arg,expectation", [(False, False), (True, True)])
    def test_nyp_suppression_match_response(self, arg, expectation):
        response = MockPlatformSessionResponseSuccess()
        sr = SearchResponse(11111111, "nyp", response)
        sr.json_response = {
            "data": {
                "suppressed": arg,
            }
        }
        assert sr._nyp_suppression() == expectation

    def test_bpl_suppression_not_found(self):
        response = MockSolrSessionResponseNotFound()
        sr = SearchResponse(11111111, "bpl", response)
        assert sr._bpl_suppression() is False

    @pytest.mark.parametrize("arg,expectation", [(False, False), (True, True)])
    def test_bpl_suppression_match_response(self, arg, expectation):
        response = MockSolrSessionResponseSuccess()
        sr = SearchResponse(11111111, "bpl", response)
        sr.json_response = {
            "response": {
                "docs": [
                    {
                        "suppressed": arg,
                    }
                ],
            }
        }
        assert sr._bpl_suppression() == expectation

    @pytest.mark.parametrize(
        "library,response,expectation",
        [
            ("nyp", MockPlatformSessionResponseSuccess(), False),
            ("bpl", MockSolrSessionResponseSuccess(), True),
            ("qpl", MockSolrSessionResponseSuccess(), None),
        ],
    )
    def test_is_suppressed(self, library, response, expectation):
        sr = SearchResponse(11111111, library, response)
        assert sr.is_suppressed() == expectation

    @pytest.mark.parametrize(
        "deleted,tag,tag_content,sub_content,expectation",
        [
            (True, "091", None, "eNYPL Book", "deleted"),
            (False, "100", None, "author", "brief-bib"),
            (False, "003", "OCoLC", None, "full-bib"),
            (False, "003", "RLIN", None, "brief-bib"),
            (False, "091", None, "FIC", "full-bib"),
            (False, "091", None, "eNYPL Book", "brief-bib"),
            (False, "091", None, "eNYPL Audio", "brief-bib"),
        ],
    )
    def test_determine_nyp_bib_status(
        self, deleted, tag, tag_content, sub_content, expectation
    ):
        response = MockPlatformSessionResponseSuccess()
        sr = SearchResponse(11111111, "nyp", response)
        sr.json_response = {
            "data": {
                "deleted": deleted,
                "varFields": [
                    {
                        "fieldTag": "c",
                        "marcTag": tag,
                        "ind1": " ",
                        "ind2": " ",
                        "content": tag_content,
                        "subfields": [
                            {"tag": "a", "content": sub_content},
                        ],
                    },
                ],
            }
        }
        assert sr._determine_nyp_bib_status() == expectation

    def test_determine_bpl_bib_status_not_found(self):
        response = MockSolrSessionResponseNotFound()
        sr = SearchResponse(11111111, "bpl", response)
        assert sr._determine_bpl_bib_status() == "deleted"

    @pytest.mark.parametrize(
        "deleted,field,value,expectation",
        [
            (True, "call_number", "eBOOK", "deleted"),
            (False, "ss_marc_tag_003", "OCoLC", "full-bib"),
            (False, "ss_marc_tag_003", "BT", "brief-bib"),
            (False, "call_number", "eBOOK", "brief-bib"),
            (False, "call_number", "eAUDIO", "brief-bib"),
            (False, "call_number", "eVIDEO", "brief-bib"),
            (False, "call_number", "FIC EGAN", "full-bib"),
        ],
    )
    def test_determine_bpl_bib_status(self, deleted, field, value, expectation):
        response = MockSolrSessionResponseSuccess()
        sr = SearchResponse(11111111, "bpl", response)
        sr.json_response = {
            "response": {
                "numFound": 1,
                "start": 0,
                "numFoundExact": True,
                "docs": [
                    {
                        "deleted": deleted,
                        field: value,
                    }
                ],
            }
        }
        assert sr._determine_bpl_bib_status() == expectation

    @pytest.mark.parametrize(
        "library,response,expectation",
        [
            ("nyp", MockPlatformSessionResponseSuccess(), "full-bib"),
            ("bpl", MockSolrSessionResponseSuccess(), "brief-bib"),
            ("bpl", MockSolrSessionResponseNotFound(), "deleted"),
        ],
    )
    def test_get_status(self, caplog, library, response, expectation):
        sr = SearchResponse(11111111, library, response)
        with caplog.at_level(logging.DEBUG):
            assert sr.get_status() == expectation
        assert f"{library.upper()} Sierra bib # 11111111 status: {expectation}"

    def test_get_status_404_http_response(self, caplog):
        response = MockPlatformSessionResponseNotFound()
        sr = SearchResponse(11111111, "nyp", response)
        with caplog.at_level(logging.WARN):
            assert sr.get_status() == "deleted"
        assert "NYP Sierra bib # 11111111 not found on Platform."


class TestNypPlatformMocked:
    def test_successful_initation(
        self, mock_platform_env, mock_successful_platform_post_token_response
    ):
        with does_not_raise():
            platform = NypPlatform()
            assert platform.headers == {
                "User-Agent": f"{__title__}/{__version__}",
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json",
                "Connection": "keep-alive",
                "Authorization": "Bearer token_string_here",
            }
            assert isinstance(platform.authorization, PlatformToken)

    def test_failed_authorization(
        self, caplog, mock_platform_env, mock_failed_platform_post_token_response
    ):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SierraSearchPlatformError):
                NypPlatform()

        assert "Unable to obtain access token for NYPL Platform." in caplog.text

    def test_get_sierra_bib_error(
        self,
        caplog,
        mock_platform_env,
        mock_successful_platform_post_token_response,
        mock_session_error,
    ):
        with NypPlatform() as platform:
            with caplog.at_level(logging.ERROR):
                with pytest.raises(SierraSearchPlatformError):
                    platform.get_sierra_bib(11111111)
        assert (
            "Error while querying NYPL Platform for Sierra bib # 11111111."
            in caplog.text
        )

    def test_get_sierra_bib(
        self,
        caplog,
        mock_platform_env,
        mock_successful_platform_post_token_response,
        mock_successful_platform_session_response,
    ):
        with NypPlatform() as platform:
            with caplog.at_level(logging.DEBUG):
                response = platform.get_sierra_bib(11111111)

            assert "NYPL Platform request (200): request_url_here." in caplog.text
            assert isinstance(response, SearchResponse)
            assert response.sierraId == 11111111
            assert response.library == "nyp"


class TestBplSolrMocked:
    def test_initiation(self, mock_solr_env):
        solr = BplSolr()
        assert solr.authorization == "solr_key"
        assert solr.endpoint == "solr_endpoint"
        assert solr.headers["User-Agent"] == f"{__title__}/{__version__}"

    def test_get_sierra_bib_error(self, caplog, mock_solr_env, mock_session_error):
        solr = BplSolr()
        with pytest.raises(SierraSearchPlatformError):
            with caplog.at_level(logging.ERROR):
                solr.get_sierra_bib(11111111)
        assert "Error while querying BPL Solr for Sierra bib # 11111111." in caplog.text

    def test_get_sierra_bib_success(
        self, mock_solr_env, mock_successful_solr_session_response
    ):
        solr = BplSolr()
        response = solr.get_sierra_bib(11111111)

        assert isinstance(response, SearchResponse)
        assert response.sierraId == 11111111
        assert response.library == "bpl"
