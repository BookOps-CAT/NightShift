# -*- coding: utf-8 -*-

"""
This module handles WorldCat Metadata API requests.
"""
import os
import logging
from typing import Iterator, List, Tuple

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import (
    WorldcatAuthorizationError,
    WorldcatSessionError,
)
from requests import Response

from nightshift import __title__, __version__
from nightshift.datastore import Resource


logger = logging.getLogger("nightshift")


def get_credentials(library: str) -> dict:
    """
    Retrieves WorldCat Metadata API credentials from environmental
    variables.

    Args:
        library:                    'NYP' or 'BPL'

    Returns:
        credentials
    """
    if library not in ("NYP", "BPL"):
        raise ValueError("Invalid library argument provided. Must be 'NYP' or 'BPL'.")
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
        logger.error("Unable to obtain Worldcat MetadataAPI access token.")
        raise


def get_full_bibs(
    library: str, resources: List[Resource]
) -> Iterator[Tuple[Resource, Response]]:
    """
    Makes MetadataAPI requests for full bibliographic records

    Args:
        library:                    'NYP' or 'BPL'
        resources:                  `sqlalchemy.engine.Result` instance

    Yields:
        (Resource, Response)
    """
    creds = get_credentials(library)
    token = get_access_token(creds)
    with MetadataSession(authorization=token) as session:
        for resource in resources:
            try:
                response = session.get_full_bib(oclcNumber=resource.oclcMatchNumber)
                logger.debug(
                    f"Full bib request for resource.nid={resource.nid} : {response.url}"
                )
                yield (resource, response)
            except WorldcatSessionError:
                logger.error("WorldcatSessionError. Aborting.")
                raise


def get_oclc_number(response: Response) -> str:
    """
    Parses OCLC number from MetadataAPI brief bib search response

    Args:
        response:                   `requests.Response` intstance

    Returns:
        oclc_number
    """
    try:
        return response.json()["briefRecords"][0]["oclcNumber"]
    except (IndexError, KeyError):
        return None


def is_match(response: Response) -> bool:
    """
    Determines if Worldcat query response returned matching record

    Args:
        response:                   `requests.Response` instance

    Returns:
        bool
    """
    if response.json()["numberOfRecords"] == 0:
        return False
    else:
        return True


def prep_resource_queries_payloads(resource: Resource) -> List[dict]:
    """
    Prepares payloads with query parameters for different resources.

    Args:
        resource:                   `nightshift.datastore.Resource` instance

    Returns:
        payloads
    """
    payloads = []

    if resource.resourceCategoryId == 1:
        # ebook
        if resource.distributorNumber:
            payloads.append(
                dict(
                    q=f"sn={resource.distributorNumber}",
                    itemType="book",
                    itemSubType="book-digital",
                )
            )
    elif resource.resourceCategoryId == 2:
        # eaudio
        if resource.distributorNumber:
            payloads.append(
                dict(
                    q=f"sn={resource.distributorNumber}",
                    itemType="audiobook",
                    itemSubType="audiobook-digital",
                )
            )
    elif resource.resourceCategoryId == 3:
        # evideo
        if resource.distributorNumber:
            payloads.append(
                dict(
                    q=f"sn={resource.distributorNumber}",
                    itemType="video",
                    itemSubType="video-digital",
                )
            )
    elif resource.resourceCategoryId in range(4, 12):
        # print monograph materials
        if resource.standardNumber:
            payloads.append(
                dict(
                    q=f"bn:{resource.standardNumber}",
                    itemType="book",
                    itemSubType="book-printbook",
                    catalogSource="DLC",
                )
            )
        if resource.congressNumber:
            payloads.append(
                dict(
                    q=f"ln:{resource.congressNumber}",
                    itemType="book",
                    itemSubType="book-printbook",
                    catalogSource="DLC",
                )
            )
    logging.debug(f"Query payload for resource.nid={resource.nid}: {payloads}")
    return payloads


def search_brief_bibs(
    library: str, resources: List[Resource]
) -> Iterator[Tuple[Resource, Response]]:
    """
    Performes WorldCat queries for each resource in the passed library batch.
    Resources must belong to the same library.

    Args:
        library:                    'NYP' or 'BPL'
        resouces:

    Yields:
        (Resource, `requests.Response`)
    """
    creds = get_credentials(library)
    token = get_access_token(creds)
    with MetadataSession(authorization=token) as session:
        for resource in resources:
            payloads = prep_resource_queries_payloads(resource)
            for payload in payloads:
                try:
                    response = session.search_brief_bibs(
                        **payload,
                        inCatalogLanguage="eng",
                        orderBy="mostWidelyHeld",
                        limit=1,
                    )
                    logger.debug(
                        f"Brief bib query for resource.nid={resource.nid}: {response.url}"
                    )
                except WorldcatSessionError:
                    logger.error("WorldcatSessionError. Aborting.")
                    raise

                if is_match(response):
                    logger.debug(f"Match found for resource.nid={resource.nid}")
                    break
            yield (resource, response)
