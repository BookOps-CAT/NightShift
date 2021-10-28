# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
import logging

import pytest

from .conftest import (
    MockSuccessfulHTTP200SessionResponseNoMatches,
    MockSuccessfulHTTP200SessionResponse,
)

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import (
    WorldcatAuthorizationError,
    WorldcatSessionError,
)

from nightshift import __title__, __version__
from nightshift.datastore import Resource
from nightshift.comms.worldcat import Worldcat, BriefBibResponse


class TestBriefBibResponse:
    def test_successful_match_to_worldcat_record(self):
        response = MockSuccessfulHTTP200SessionResponse()
        data = BriefBibResponse(response)
        assert data.is_match
        assert data.oclc_number == "44959645"

    def test_failed_match_to_worldcat_record(self):
        response = MockSuccessfulHTTP200SessionResponseNoMatches()
        data = BriefBibResponse(response)
        assert data.is_match is False
        assert data.oclc_number is None


class TestWorldcatMocked:
    """Tests Worldcat class methods with mocking all interactions with OCLC service"""

    def test_invalid_library_argument(self):
        msg = "Invalid library argument provided. Must be 'NYP' or 'BPL'."
        with pytest.raises(ValueError) as exc:
            Worldcat("QPL")
        assert msg in str(exc.value)

    @pytest.mark.parametrize("arg", ["NYP", "BPL"])
    def test_init_library_args(
        self, arg, mock_worldcat_creds, mock_successful_post_token_response
    ):
        with does_not_raise():
            reader = Worldcat(arg)

        assert reader.library == arg

    def test_get_credentials(self, mock_Worldcat):
        assert mock_Worldcat._get_credentials() == {
            "key": "lib_key",
            "secret": "lib_secret",
            "scopes": "WorldCatMetadataAPI",
            "principal_id": "lib_principal_id",
            "principal_idns": "lib_principal_idns",
            "agent": "NightShift/0.1.0",
        }

    def test_get_access_token(self, mock_Worldcat):
        creds = {
            "key": "lib_key",
            "secret": "lib_secret",
            "scopes": "WorldCatMetadataAPI",
            "principal_id": "lib_principal_id",
            "principal_idns": "lib_principal_idns",
            "agent": "NightShift/0.1.0",
        }
        token = mock_Worldcat._get_access_token(creds)
        assert isinstance(token, WorldcatAccessToken)
        assert token.agent == "NightShift/0.1.0"

    def test_get_access_token_failure(
        self, caplog, mock_worldcat_creds, mock_failed_post_token_response
    ):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(WorldcatAuthorizationError):
                Worldcat("NYP")
        assert "Unable to obtain Worldcat MetadataAPI access token." in caplog.text

    def test_create_worldcat_session(self, mock_Worldcat):
        assert isinstance(mock_Worldcat.session, MetadataSession)

    def test_get_full_bibs(
        self, caplog, mock_Worldcat, mock_successful_session_get_request
    ):
        resource = Resource(
            nid=1,
            sierraId=22222222,
            title="TEST TITLE",
            oclcMatchNumber="123",
        )
        with caplog.at_level(logging.DEBUG):
            result = next(mock_Worldcat.get_full_bibs([resource]))

        resource, response = result
        assert isinstance(result, tuple)
        assert isinstance(resource, Resource)
        assert resource.nid == 1
        assert response.status_code == 200
        assert (
            "Full bib Worldcat request for NYP Sierra bib # 22222222: request_url_here."
            in caplog.text
        )

    def test_get_full_bibs_session_error(
        self, caplog, mock_Worldcat, mock_session_error
    ):
        resource = Resource(
            nid=1,
            sierraId=22222222,
            title="TEST TITLE",
            oclcMatchNumber="123",
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(WorldcatSessionError):
                next(mock_Worldcat.get_full_bibs([resource]))

        assert "WorldcatSessionError. Aborting." in caplog.text

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            pytest.param(
                1,
                [{"q": "sn=111", "itemType": "book", "itemSubType": "book-digital"}],
                id="ebook",
            ),
            pytest.param(
                2,
                [
                    {
                        "q": "sn=111",
                        "itemType": "audiobook",
                        "itemSubType": "audiobook-digital",
                    }
                ],
                id="eaudio",
            ),
            pytest.param(
                3,
                [{"q": "sn=111", "itemType": "video", "itemSubType": "video-digital"}],
                id="evideo",
            ),
            pytest.param(
                4,
                [
                    {
                        "q": "bn:222",
                        "itemType": "book",
                        "itemSubType": "book-printbook",
                        "catalogSource": "DLC",
                    },
                    {
                        "q": "ln:333",
                        "itemType": "book",
                        "itemSubType": "book-printbook",
                        "catalogSource": "DLC",
                    },
                ],
                id="book",
            ),
        ],
    )
    def test_prep_resource_queries_payloads(
        self, caplog, arg, expectation, mock_Worldcat
    ):
        resource = Resource(
            resourceCategoryId=arg,
            sierraId=22222222,
            distributorNumber=111,
            standardNumber=222,
            congressNumber=333,
        )
        with caplog.at_level(logging.DEBUG):
            payloads = mock_Worldcat._prep_resource_queries_payloads(resource)
        assert payloads == expectation
        assert f"Query payload for NYP Sierra bib # 22222222: {expectation}."

    def test_search_brief_bibs(
        self, caplog, mock_Worldcat, mock_successful_session_get_request
    ):
        resource = Resource(
            nid=1,
            sierraId=22222222,
            resourceCategoryId=1,  # ebook category id
            libraryId=1,
            title="TEST TITLE",
            distributorNumber="111",
        )
        with caplog.at_level(logging.DEBUG):
            result = next(mock_Worldcat.get_brief_bibs([resource]))

        assert isinstance(result, tuple)

        resource, brief_data = result
        assert isinstance(resource, Resource)
        assert isinstance(brief_data, BriefBibResponse)
        assert resource.nid == 1
        assert brief_data.is_match
        assert brief_data.oclc_number == "44959645"
        assert (
            "Brief bib Worldcat query for NYP Sierra bib # 22222222: request_url_here."
            in caplog.text
        )

    def test_search_brief_bibs_no_matches_found(
        self,
        mock_Worldcat,
        mock_successful_session_get_request_no_matches,
    ):
        resource = Resource(
            nid=1,
            sierraId=22222222,
            resourceCategoryId=1,
            libraryId=1,
            title="TEST TITLE",
            distributorNumber="111",
        )
        result = next(mock_Worldcat.get_brief_bibs([resource]))

        assert isinstance(result, tuple)

        res, data = result

        assert isinstance(res, Resource)
        assert isinstance(data, BriefBibResponse)
        assert resource.nid == 1
        assert (
            data.json_response == MockSuccessfulHTTP200SessionResponseNoMatches().json()
        )
        assert not data.is_match
        assert data.oclc_number is None

    def test_search_brief_bibs_session_error(
        self, caplog, mock_Worldcat, mock_session_error
    ):
        resource = Resource(nid=1, resourceCategoryId=1, distributorNumber="123")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(WorldcatSessionError):
                next(mock_Worldcat.get_brief_bibs([resource]))

        assert "WorldcatSessionError. Aborting." in caplog.text
