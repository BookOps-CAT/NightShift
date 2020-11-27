# -*- coding: utf-8 -*-

"""
Tests bot's Worldcat Metadata API request methods
"""
import xml.etree.ElementTree

import requests
import pytest

from nightshift.api_worldcat import (
    get_full_bib,
    get_token,
    find_matching_eresource,
    parse_oclcNumber_from_brief_bib_response,
    parse_record_from_full_bib_response,
    search_for_brief_eresource,
    string2xml,
)
from nightshift.errors import NightShiftError


def test_get_token_success(mock_keys, mock_successful_worldcat_post_token_response):
    token = get_token()

    assert token.token_str == "tk_Yebz4BpEp9dAsghA7KpWx6dYD1OZKWBlHjqW"


def test_get_token_error(mock_keys, mock_failed_worldcat_post_token_response):
    err_msg = "Worldcat authorization error:"
    with pytest.raises(NightShiftError) as exc:
        get_token()
    assert err_msg in str(exc.value)


def test_parse_oclcNumber(
    fake_successful_worldcat_metadata_search_response,
):
    assert (
        parse_oclcNumber_from_brief_bib_response(
            fake_successful_worldcat_metadata_search_response
        )
        == "1190756389"
    )


def test_parse_failed_search_response(fake_no_match_worldcat_metadata_search_response):
    assert (
        parse_oclcNumber_from_brief_bib_response(
            fake_no_match_worldcat_metadata_search_response
        )
        is None
    )


def test_string2xml(fake_xml_response):
    assert type(string2xml(fake_xml_response)) == xml.etree.ElementTree.Element


def test_parse_record_from_full_bib_response(
    fake_successful_worldcat_full_bib_response,
):
    record = parse_record_from_full_bib_response(
        fake_successful_worldcat_full_bib_response
    )
    assert type(record) == xml.etree.ElementTree.Element
    assert record.tag == "{http://www.loc.gov/MARC21/slim}record"
    assert record[0].text == "00000cam a2200000Ka 4500"


def test_search_for_brief_eresource_success(
    mock_successful_worldcat_metadata_search_response, fake_metadata_session
):
    session = fake_metadata_session
    response = search_for_brief_eresource(
        session, "40CC3B3F-4C30-4685-B391-DB7B2EA91455"
    )
    assert type(response) == requests.models.Response
    assert response.status_code == 200


def test_search_for_brief_eresource_request_error(
    fake_metadata_session,
    mock_failed_worldcat_metadata_search_response,
):
    err_msg = "Worldcat eresource (reserve_id-1) search error:"
    session = fake_metadata_session
    with pytest.raises(NightShiftError) as exc:
        search_for_brief_eresource(session, "reserve_id-1")
    assert err_msg in str(exc.value)


def test_get_full_bib_success(
    fake_metadata_session, mock_successful_worldcat_metadata_full_bib_response
):
    session = fake_metadata_session
    response = get_full_bib(session, "12345")
    assert type(response) == requests.models.Response
    assert response.status_code == 200


def test_get_full_bib_error(
    fake_metadata_session, mock_failed_worldcat_metadata_search_response
):
    err_msg = "Worldcat full eresource bib (12345) request error:"
    session = fake_metadata_session
    with pytest.raises(NightShiftError) as exc:
        get_full_bib(session, "12345")
    assert err_msg in str(exc.value)


def test_find_matching_eresource_success(
    fake_metadata_session, mock_search_for_brief_eresource_success, mock_get_full_bib
):
    session = fake_metadata_session
    response = find_matching_eresource(session, "40CC3B3F-4C30-4685-B391-DB7B2EA91455")
    assert type(response) is tuple
    assert response[0] == "1190756389"
    assert type(response[1]) == xml.etree.ElementTree.Element


def test_find_matching_eresource_fail(
    fake_metadata_session, mock_search_for_brief_eresource_fail, mock_get_full_bib
):
    session = fake_metadata_session
    response = find_matching_eresource(session, "40CC3B3F-4C30-4685-B391-DB7B2EA91455")
    assert response is None
