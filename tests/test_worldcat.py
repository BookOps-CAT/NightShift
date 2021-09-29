# -*- coding: utf-8 -*-
import pytest

from bookops_worldcat.errors import WorldcatAuthorizationError

from nightshift import __title__, __version__
from nightshift.worldcat import get_credentials, get_access_token


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
