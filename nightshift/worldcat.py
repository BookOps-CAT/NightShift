# -*- coding: utf-8 -*-

"""
This module handles WorldCat Metadata API requests.
"""
import os
from typing import Optional

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import (
    WorldcatAuthorizationError,
    WorldcatRequestError,
    WorldcatSessionError,
)
from requests import Response

from nightshift import __title__, __version__
from nightshift.datastore import Resource


def get_credentials(library: str) -> dict:
    """
    Retrieves WorldCat Metadata API credentials from environmental
    variables.

    Args:
        library:                    'NYP' or 'BPL'

    Returns:
        credentials
    """
    return dict(
        key=os.getenv(f"WC{library}_KEY"),
        secret=os.getenv(f"WC{library}_SECRET"),
        scopes="WorldCatMetadataAPI",
        principal_id=os.getenv(f"WC{library}_PRINCIPALID"),
        principal_idns=os.getenv(f"WC{library}_PRINCIPALIDNS"),
        agent=f"{__title__}/{__version__}",
    )


def get_access_token(credentials: dict) -> WorldcatAccessToken:
    """
    Requests from the OCLC authentication server an access token

    Args:
        credentials:                library's credentials for OCLC services

    Returns:
        access_token:               an instance of `WorldcatAccessToken`
    """
    try:
        access_token = WorldcatAccessToken(**credentials)
        return access_token
    except WorldcatAuthorizationError:
        raise


def search_worldcat(session: MetadataSession, resource: Resource) -> Optional[Response]:
    """
    Searches Worldcat for matching records

    Args:
        session:                    'bookops_worldcat.MetadataSession` instance
        resource:                   'nightshift.datastore.Resource` instance

    Returns:
        `requests.Response` instance

    """

    def request(kwargs: dict) -> Optional[Response]:
        try:
            response = session.search_brief_bibs(
                **kwargs, inCatalogLanguage="eng", orderBy="mostWidelyHeld", limit=1
            )
            return response
        except WorldcatRequestError:
            return None
        except WorldcatSessionError:
            raise

    response = None

    if resource.resourceCategoryId == 1:
        # electronic resoruces
        if resource.distributorNumber:
            response = request(
                dict(q=f"sn={resource.distributorNumber}", itemSubType="digital")
            )
    elif resource.resourceCategoryId in range(2, 10):
        # print material, example below
        # if resource.standardNumber:
        #     response = request(q=f"bn={resource.standardNumber}")
        # if not response and resource.congressNumber:
        #     response = request(q=f"cn={resource.congressNumber}")
        pass

    return response
