# -*- coding: utf-8 -*-

"""
This module handles WorldCat Metadata API requests.
"""
import os

from . import __title__, __version__
from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatAuthorizationError


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
