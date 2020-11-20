# -*- coding: utf-8 -*-

"""
This module includes methods to authenticate and query OCLC Worlcat
"""
import os
from typing import Type

import bookops_worldcat
from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatAuthorizationError


from . import __version__, __title__
from .errors import NightShiftError


def get_token() -> Type[bookops_worldcat.authorize.WorldcatAccessToken]:
    """
    Aquires Worldcat access token.

    Returns:
        token obj
    """
    try:
        token = WorldcatAccessToken(
            key=os.environ["worldcat-key"],
            secret=os.environ["worldcat-secret"],
            scopes=os.environ["worldcat-scopes"],
            principal_id=os.environ["worldcat-principal-id"],
            principal_idns=os.environ["worldcat-principal-idns"],
            agent=f"{__title__}/{__version__}",
        )
    except WorldcatAuthorizationError as exc:
        raise NightShiftError(f"Worldcat authorization error: {exc}")
    else:
        return token
