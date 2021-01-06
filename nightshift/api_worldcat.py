# -*- coding: utf-8 -*-

"""
This module includes methods to authenticate and query OCLC Worlcat
"""
import os
from typing import Optional, Tuple
import xml.etree.ElementTree as ET

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatAuthorizationError, WorldcatSessionError
import requests
from requests import Response


from . import __version__, __title__
from .errors import NightShiftError


# OCLC Worldcat response namespaces
ONS = {
    "response": "http://www.loc.gov/zing/srw/",
    "marc": "http://www.loc.gov/MARC21/slim",
    "atom": "http://www.w3.org/2005/Atom",
    "rb": "http://worldcat.org/rb",
}


def string2xml(marcxml_as_string: str) -> ET.Element:
    return ET.fromstring(marcxml_as_string)


def get_token(library_system: str) -> WorldcatAccessToken:
    """
    Aquires Worldcat access token.

    Args:
        library_system:         'nyp' or "bpl"

    Returns:
        bookops_worldcat.authorize.WorldcatAccessToken
    """
    try:
        token = WorldcatAccessToken(
            key=os.environ[f"{library_system}-worldcat-key"],
            secret=os.environ[f"{library_system}-worldcat-secret"],
            scopes=os.environ[f"{library_system}-worldcat-scopes"],
            principal_id=os.environ[f"{library_system}-worldcat-principal-id"],
            principal_idns=os.environ[f"{library_system}-worldcat-principal-idns"],
            agent=f"{__title__}/{__version__}",
        )
    except WorldcatAuthorizationError as exc:
        raise NightShiftError(f"Worldcat authorization error: {exc}")
    else:
        return token


def parse_oclcNumber_from_brief_bib_response(
    response: Response,
) -> Optional[str]:
    """
    Parses response and returns OCLC # of the returned record

    Args:
        response:               requests Response obj for Worlcat server response

    Returns:
        oclcNumber
    """
    data = response.json()

    try:
        return data["briefRecords"][0]["oclcNumber"].strip()
    except KeyError:
        return None


def parse_record_from_full_bib_response(
    response: Response,
) -> ET.Element:
    """
    Parses full record response returned by Worldcat server

    Args:
        response:               Worldcat full bib response as xml

    Returns:
        xml.etree.ElementTree.Element obj
    """
    response_body = string2xml(response.content)
    record = response_body.find(".//atom:content/rb:response/marc:record", ONS)
    return record


def search_for_brief_eresource(session: MetadataSession, reserve_id: str) -> Response:
    """
    Makes a search request to Metadata API using reserve_id phrase and returns
    OCLC number of the match.

    Args:
        session:                MetadataSession obj
        reserve_id:             Overdrive reserve id,
                                    example: 40CC3B3F-4C30-4685-B391-DB7B2EA91455

    returns:
        requests.models.Response
    """
    try:
        response = session.search_brief_bibs(
            q=f"sn={reserve_id}",
            inCatalogLanguage="eng",
            itemSubType="digital",
            orderBy="mostWidelyHeld",
            limit=1,
        )
        return response
    except WorldcatSessionError as exc:
        raise NightShiftError(f"Worldcat eresource ({reserve_id}) search error: {exc}")


def get_full_bib(session: MetadataSession, oclcNumber: str) -> Response:
    """
    Retrieves full MARC XML encoded record from Worldcat based given OCLC number

    Args:
        session:                MetadataSession obj
        oclcNumber:             OCLC number

    Returns:
        requests.models.Response
    """
    try:
        response = session.get_full_bib(
            oclcNumber=oclcNumber,
            response_format='application/atom+xml;content="application/vnd.oclc.marc21+xml"',
        )
        return response
    except WorldcatSessionError as exc:
        raise NightShiftError(
            f"Worldcat full eresource bib ({oclcNumber}) request error: {exc}"
        )


def find_matching_eresource(
    session: MetadataSession, reserve_id: str
) -> Tuple[str, bytes]:
    """


    Args:
        session:                MetadataSession obj
        reserve_id:             Overdrive reserve id,
                                    example: 40CC3B3F-4C30-4685-B391-DB7B2EA91455

    Returns:
        tuple:                  oclcNo str & response as bytes
    """
    search_response = search_for_brief_eresource(session, reserve_id)
    oclcNumber = parse_oclcNumber_from_brief_bib_response(search_response)

    # if found request full bib
    if oclcNumber:
        full_bib_response = get_full_bib(session, oclcNumber)
        # record = parse_record_from_full_bib_response(full_bib_response)
        record = full_bib_response.content
        return (oclcNumber, record)
    else:
        return None
