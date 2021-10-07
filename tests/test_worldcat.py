# -*- coding: utf-8 -*-
import pytest

from bookops_worldcat.errors import WorldcatAuthorizationError

from nightshift import __title__, __version__
from nightshift.datastore import Resource
from nightshift.worldcat import (
    get_credentials,
    get_access_token,
    prep_resource_queries_payloads,
    worldcat_search_request,
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


def test_worldcat_search_request(
    mock_worldcat_session, mock_successful_session_get_request
):
    response = worldcat_search_request(mock_worldcat_session, payload=dict(q="foo"))
    assert response.status_code == 200
