# -*- coding: utf-8 -*-

"""
This module handles WorldCat Metadata API requests.
"""
import os

from . import __title__, __version__
from bookops_worldcat import WorldcatAccessToken, MetadataSession


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
