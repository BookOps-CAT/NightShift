# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise

import pytest

from .conftest import (
    MockSuccessfulHTTP200SessionResponseNoMatches,
    MockSuccessfulHTTP200SessionResponse,
)

from bookops_worldcat.errors import WorldcatAuthorizationError, WorldcatSessionError

from nightshift import __title__, __version__
from nightshift.datastore import Resource
from nightshift.worldcat import (
    get_credentials,
    get_access_token,
    get_oclc_number,
    is_match,
    prep_resource_queries_payloads,
    search_batch,
    get_full_bibs,
)


def test_get_credentials_invalid_library():
    err_msg = "Invalid library argument provided. Must be 'NYP' or 'BPL'."
    with pytest.raises(ValueError) as exc:
        get_credentials("QPL")

    assert err_msg in str(exc)


@pytest.mark.parametrize("arg", ["NYP", "BPL"])
def test_get_credentials(arg, mock_worldcat_creds):
    assert (get_credentials(library=arg)) == {
        "key": "lib_key",
        "secret": "lib_secret",
        "scopes": "WorldCatMetadataAPI",
        "principal_id": "lib_principal_id",
        "principal_idns": "lib_principal_idns",
        "agent": f"{__title__}/{__version__}",
    }


def test_get_access_token_failure(mock_worldcat_creds, mock_failed_post_token_response):
    creds = get_credentials(library="NYP")
    with pytest.raises(WorldcatAuthorizationError):
        get_access_token(creds)


def test_get_access_token_success(
    mock_worldcat_creds, mock_successful_post_token_response
):
    creds = get_credentials(library="NYP")
    token = get_access_token(creds)
    assert token.token_str == "tk_Yebz4BpEp9dAsghA7KpWx6dYD1OZKWBlHjqW"
    assert not token.is_expired()


def test_is_match_false():
    response = MockSuccessfulHTTP200SessionResponseNoMatches()
    assert is_match(response) is False


def test_is_match_true():
    response = MockSuccessfulHTTP200SessionResponse()
    assert is_match(response) is True


def test_get_oclc_number_from_response():
    response = MockSuccessfulHTTP200SessionResponse()
    assert get_oclc_number(response) == "44959645"


@pytest.mark.parametrize(
    "arg,expectation",
    [
        (1, [{"q": "sn=111", "itemType": "book", "itemSubType": "book-digital"}]),
        (
            2,
            [
                {
                    "q": "sn=111",
                    "itemType": "audiobook",
                    "itemSubType": "audiobook-digital",
                }
            ],
        ),
        (3, [{"q": "sn=111", "itemType": "video", "itemSubType": "video-digital"}]),
        (
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
        ),
    ],
)
def test_prep_resource_queries_payloads(arg, expectation):
    res = Resource(
        resourceCategoryId=arg,
        distributorNumber=111,
        standardNumber=222,
        congressNumber=333,
    )
    assert prep_resource_queries_payloads(res) == expectation


def test_search_batch_ebook_match(
    mock_worldcat_creds,
    mock_successful_post_token_response,
    mock_successful_session_get_request,
):
    resource = Resource(
        nid=1,
        sierraId=22222222,
        resourceCategoryId=1,
        libraryId=1,
        title="TEST TITLE",
        distributorNumber="111",
    )
    results = search_batch(library="NYP", resources=[resource])
    resource, response = next(results)
    assert resource.nid == 1
    assert response.json()["briefRecords"][0]["oclcNumber"] == "44959645"


def test_search_batch_ebook_no_match(
    mock_worldcat_creds,
    mock_successful_post_token_response,
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
    results = search_batch(library="NYP", resources=[resource])
    resource, response = next(results)
    assert resource.nid == 1
    assert response is None


def test_search_batch_session_exception(
    mock_worldcat_creds, mock_successful_post_token_response, mock_session_error
):
    resource = Resource(
        nid=1,
        sierraId=22222222,
        resourceCategoryId=1,
        libraryId=1,
        title="TEST TITLE",
        distributorNumber="111",
    )
    with pytest.raises(WorldcatSessionError):
        results = search_batch(library="NYP", resources=[resource])
        next(results)
