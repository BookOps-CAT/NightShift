import pytest

from nightshift.comms.sierra_search_platform import (
    BplSolr,
    NypPlatform,
    SearchResponse,
    is_eresource_callno,
)
from nightshift.ns_exceptions import SierraSearchPlatformError


@pytest.mark.parametrize(
    "arg,expectation",
    [(123, False), ("B ADAMS C", False), ("eNYPL Book", True), ("eBOOK", True)],
)
def test_is_eresource_callno(arg, expectation):
    assert is_eresource_callno(arg) == expectation


class TestSearchResponse:
    def test_search_response_solr(self, mock_solr_response):
        sr = SearchResponse(11111111, "BPL", mock_solr_response)
        assert sr.library == "BPL"
        assert sr.sierraId == 11111111
        assert sr.json_response == mock_solr_response.json()
        assert sr.is_suppressed() is False
        assert sr.get_status() == "staff_enhanced"

    def test_search_response_platform(self, mock_platform_response):
        sr = SearchResponse(11111111, "NYP", mock_platform_response)
        assert sr.library == "NYP"
        assert sr.sierraId == 11111111
        assert sr.json_response == mock_platform_response.json()
        assert sr.is_suppressed() is False
        assert sr.get_status() == "staff_enhanced"

    def test_search_response_solr_oclc_origin(self, mock_solr_record_oclc_origin):
        sr = SearchResponse(11111111, "BPL", mock_solr_record_oclc_origin)
        assert sr.library == "BPL"
        assert sr.sierraId == 11111111
        assert sr.get_status() == "staff_enhanced"

    def test_search_response_platform_oclc_origin(
        self, mock_platform_record_oclc_origin
    ):
        sr = SearchResponse(11111111, "NYP", mock_platform_record_oclc_origin)
        assert sr.library == "NYP"
        assert sr.sierraId == 11111111
        assert sr.get_status() == "staff_enhanced"

    def test_search_response_solr_not_found(self, mock_solr_record_not_found):
        sr = SearchResponse(11111111, "BPL", mock_solr_record_not_found)
        assert sr.is_suppressed() is False
        assert sr.get_status() == "staff_deleted"

    def test_search_response_platform_staff_deleted(self, mock_platform_deleted_record):
        sr = SearchResponse(11111111, "NYP", mock_platform_deleted_record)
        assert sr.is_suppressed() is False
        assert sr.get_status() == "staff_deleted"

    def test_search_response_platform_not_found(self, mock_platform_record_not_found):
        sr = SearchResponse(11111111, "NYP", mock_platform_record_not_found)
        assert sr.is_suppressed() is False
        assert sr.get_status() == "staff_deleted"

    def test_search_response_platform_error(self, mock_400_response, caplog):
        with pytest.raises(SierraSearchPlatformError):
            SearchResponse(11111111, "NYP", mock_400_response)
        assert (
            "NYP search platform returned HTTP error code 400 for request request_url_here"
            in caplog.text
        )

    def test_search_response_solr_suppressed(self, mock_solr_record_suppressed):
        sr = SearchResponse(11111111, "BPL", mock_solr_record_suppressed)
        assert sr.is_suppressed() is True
        assert sr.get_status() == "open"

    def test_search_response_platform_suppressed(self, mock_platform_record_suppressed):
        sr = SearchResponse(11111111, "NYP", mock_platform_record_suppressed)
        assert sr.is_suppressed() is True
        assert sr.get_status() == "open"


class TestNypPlatformMocked:
    def test_get_sierra_bib(self, caplog, mock_token, mock_platform_response):
        with NypPlatform() as platform:
            response = platform.get_sierra_bib(11111111)

        assert "NYPL Platform request (200): request_url_here." in caplog.text
        assert isinstance(response, SearchResponse)
        assert response.sierraId == 11111111
        assert response.library == "NYP"

    def test_failed_authorization(self, caplog, mock_token_error):
        with pytest.raises(SierraSearchPlatformError):
            NypPlatform()

        assert "Unable to obtain access token for NYPL Platform." in caplog.text

    def test_get_sierra_bib_error(self, caplog, mock_token, mock_sierra_session_error):
        with NypPlatform() as platform, pytest.raises(SierraSearchPlatformError):
            platform.get_sierra_bib(11111111)
        assert (
            "Error while querying NYPL Platform for Sierra bib # 11111111."
            in caplog.text
        )


class TestBplSolrMocked:
    def test_get_sierra_bib(self, mock_solr_response):
        solr = BplSolr()
        response = solr.get_sierra_bib(11111111)

        assert isinstance(response, SearchResponse)
        assert response.sierraId == 11111111
        assert response.library == "BPL"

    def test_get_sierra_bib_error(self, caplog, mock_sierra_session_error):
        solr = BplSolr()
        with pytest.raises(SierraSearchPlatformError):
            solr.get_sierra_bib(11111111)
        assert "Error while querying BPL Solr for Sierra bib # 11111111." in caplog.text
