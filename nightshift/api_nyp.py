# -*- coding: utf-8 -*-

"""
This module provides methods for the bot to communicate with NYPL Platform (Sierra)
"""
import json
import os
from typing import List


from bookops_nypl_platform import PlatformToken, PlatformSession
from bookops_nypl_platform.errors import BookopsPlatformError


from . import __version__, __title__
from .errors import NightShiftError


class PlatformResponseReader:
    def __init__(self, response: Type[Response]):

        self.response = response.json()


def split_into_batches(sbids: List[int], size: int = 50) -> List[List]:
    """
    Splits list of Sierra bib numbers for querying into batches of
    desired size

    Args:
        sbids:                  list of Sierra bib numbers
        size:                   batch size

    Returns:
        batches
    """
    incomplete = True
    batches = []
    start = 0
    end = size
    while incomplete:
        batch = sbids[start:end]
        if not batch:
            incomplete = False
        elif len(batch) < size:
            batches.append(batch)
            incomplete = False
        else:
            batches.append(batch)
            start += size
            end += size

    return batches


def get_bibs(sbids: List[int]):
    """
    Queries NYPL Platform by Sierra bib number

    Args:
        sbids:                  list of Sierra bib numbers withough "b" prefix
                                or last digit check
    """

    # split sierra bib numbers into batches of 50
    batches = split_into_batches(sbids)

    token = PlatformToken(
        client_id=os.environ["platform-client-id"],
        client_secret=os.environ["platform-client-secret"],
        oauth_server=os.environ["platform-oauth-server"],
        agent=f"{__title__}/{__version__}",
    )
    with PlatformSession(
        authorization=token, agent=f"{__title__}/{__version__}"
    ) as session:
        for batch in batches:
            try:
                # include data if bib deleted
                response = session.get_bib_list(batch, deleted=None)
            except BookopsPlatformError as exc:
                raise NightShiftError(f"{exc}")
