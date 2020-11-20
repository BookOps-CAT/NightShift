# -*- coding: utf-8 -*-

"""
Tests bot's Worldcat Metadata API request methods
"""
import pytest

from nightshift.api_worldcat import get_token
from nightshift.errors import NightShiftError


def test_get_token_success(mock_keys, mock_successful_worldcat_post_token_response):
    token = get_token()

    assert token.token_str == "tk_Yebz4BpEp9dAsghA7KpWx6dYD1OZKWBlHjqW"


def test_get_token_error(mock_keys, mock_failed_worldcat_post_token_response):
    err_msg = "Worldcat authorization error:"
    with pytest.raises(NightShiftError) as exc:
        get_token()
        assert err_msg in str(exc.value)
