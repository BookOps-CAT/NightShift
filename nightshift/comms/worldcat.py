# -*- coding: utf-8 -*-

"""
This module handles WorldCat Metadata API requests.
"""
import os
import logging
from typing import Iterator, List, Optional, Tuple

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import (
    WorldcatAuthorizationError,
    WorldcatSessionError,
)
from requests import Response

from nightshift import __title__, __version__
from nightshift.datastore import Resource


logger = logging.getLogger("nightshift")


class BriefBibResponse:
    def __init__(self, response: Response):
        self.as_json = response.json()
        self.is_match = self._is_match()
        self.oclc_number = self._parse_oclc_number()

    def _is_match(self) -> bool:
        """
        Determines if Worldcat brief bib search response returned matching record
        Returns:
            bool
        """
        if self.as_json["numberOfRecords"] == 0:
            return False
        else:
            return True

    def _parse_oclc_number(self) -> Optional[str]:
        """
        Parses OCLC number from MetadataAPI brief bib search response

        Returns:
            oclc_number
        """
        try:
            return self.as_json["briefRecords"][0]["oclcNumber"]
        except (IndexError, KeyError):
            return None


class Worldcat:
    def __init__(self, library: str):
        """
        Initiate reader by obtaining MetadataAPI access token and by creating
        a session

        Args:
            library:                'NYP' or "BPL"
        """
        if library not in ("NYP", "BPL"):
            raise ValueError(
                "Invalid library argument provided. Must be 'NYP' or 'BPL'."
            )

        self.library = library

        creds = self._get_credentials()
        token = self._get_access_token(creds)
        self.session = self._create_worldcat_session(token)

    def _create_worldcat_session(
        self, access_token: WorldcatAccessToken
    ) -> MetadataSession:
        """
        Opens a session with MetatadaAPI service

        Args:
            access_token:               `WorldcatAccessToken` instance
        Yields:
            `MetadataSession` object
        """
        with MetadataSession(authorization=access_token) as session:
            return session

    def _get_access_token(self, credentials: dict) -> WorldcatAccessToken:
        """
        Requests from the OCLC's authentication server an access token

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

    def _get_credentials(self) -> dict:
        """
        Retrieves library's credential from environmental variables

        Returns:
            credentials
        """
        return dict(
            key=os.getenv(f"WC{self.library}_KEY"),
            secret=os.getenv(f"WC{self.library}_SECRET"),
            scopes="WorldCatMetadataAPI",
            principal_id=os.getenv(f"WC{self.library}_PRINCIPALID"),
            principal_idns=os.getenv(f"WC{self.library}_PRINCIPALIDNS"),
            agent=f"{__title__}/{__version__}",
        )

    def _prep_resource_queries_payloads(self, resource: Resource) -> List[dict]:
        """
        Prepares payloads with query parameters for different resources.

        Args:
            resource:                   `datastore.Resource` instance

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
        logging.debug(
            f"Query payload for {self.library} Sierra bib # {resource.sierraId}: {payloads}."
        )
        return payloads

    def get_full_bibs(self, resources: list[Resource]) -> Iterator[bytes]:
        """
        Makes MetadataAPI requests for full bibliographic resources
        """
        try:
            for resource in resources:
                response = self.session.get_full_bib(
                    oclcNumber=resource.oclcMatchNumber
                )
                logger.debug(
                    f"Full bib Worldcat request for {self.library} Sierra bib # {resource.sierraId}: {response.url}."
                )
                yield (resource, response)
        except WorldcatSessionError:
            logger.error("WorldcatSessionError. Aborting.")
            raise

    def get_brief_bibs(self, resources: list[Resource]) -> Iterator:
        """
        Performes WorldCat queries for each resource in the passed library batch.
        Resources must belong to the same library.

        Args:
            resources:                   `datastore.Resource` instances


        """
        try:
            for resource in resources:
                for payload in self._prep_resource_queries_payloads(resource):
                    response = self.session.search_brief_bibs(
                        **payload,
                        inCatalogLanguage="eng",
                        orderBy="mostWidelyHeld",
                        limit=1,
                    )

                    brief_bib_response = BriefBibResponse(response)
                    logger.debug(
                        f"Brief bib Worldcat query for {self.library} Sierra bib # {resource.sierraId}: {response.url}."
                    )
                    if brief_bib_response.is_match:
                        logger.debug(
                            f"Match found for {self.library} Sierra bib #: {resource.sierraId}."
                        )
                        break
                yield (resource, brief_bib_response)
        except WorldcatSessionError:
            logger.error(f"WorldcatSessionError. Aborting.")
            raise
