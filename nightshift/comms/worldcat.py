# -*- coding: utf-8 -*-

"""
This module handles WorldCat Metadata API requests.
"""
from collections.abc import Iterator
import os
import logging
from typing import Any

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import (
    WorldcatAuthorizationError,
    WorldcatRequestError,
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

    def _parse_oclc_number(self) -> Any:
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
        if library not in ("NYP", "BPL", "nyp", "bpl"):
            raise ValueError(
                "Invalid library argument provided. Must be 'NYP' or 'BPL'."
            )

        self.library = library.upper()

        creds = self._get_credentials()
        token = self._get_access_token(creds)
        self.session = self._create_worldcat_session(token)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

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

    def _format_rotten_apples(
        self, resource_category_id: int, rotten_apples: dict[int, list[str]]
    ) -> str:
        """
        Formats a list of forbidden org codes to be includes in a Worldcat query

        Args:
            resource_category_id:       `nid` of applicable resource category
            rotten_apples:              dictionary of OCLC organization codes
                                        to be excluded from results;
                                        dict key is `ResourceCategory.nid`.

        Returns:
            a segment of query string
        """
        try:
            rotten_apples_str = "".join(
                [f" NOT cs={a}" for a in rotten_apples[resource_category_id]]
            )
        except KeyError:
            rotten_apples_str = ""

        return rotten_apples_str

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
            logger.info(f"{self.library} Worldcat Metadata API access token obtained.")
            return access_token
        except WorldcatAuthorizationError:
            logger.error(
                f"Unable to obtain {self.library} Worldcat MetadataAPI access token."
            )
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
            agent=f"{__title__}/{__version__}",
        )

    def _prep_resource_queries_payloads(
        self, resource: Resource, rotten_apples: dict[int, list[str]]
    ) -> list[dict]:
        """
        Prepares payloads with query parameters for different resources.

        Args:
            resource:                   `datastore.Resource` instance
            rotten_apples:              dictionary of OCLC organization codes
                                        to be excluded from results;
                                        dict key is `ResourceCategory.nid`.

        Returns:
            payloads
        """
        payloads = []

        forbidden_sources = self._format_rotten_apples(
            resource.resourceCategoryId, rotten_apples
        )

        if resource.resourceCategoryId == 1:
            # ebook
            if resource.distributorNumber:
                payloads.append(
                    dict(
                        q=(
                            f"sn={resource.distributorNumber} "
                            f"NOT lv:3{forbidden_sources}"
                        ),
                        itemType="book",
                        itemSubType="book-digital",
                    )
                )
        elif resource.resourceCategoryId == 2:
            # eaudio
            if resource.distributorNumber:
                payloads.append(
                    dict(
                        q=f"sn={resource.distributorNumber} NOT lv:3{forbidden_sources}",
                        itemType="audiobook",
                        itemSubType="audiobook-digital",
                    )
                )
        elif resource.resourceCategoryId == 3:
            # evideo
            if resource.distributorNumber:
                payloads.append(
                    dict(
                        q=f"sn={resource.distributorNumber} NOT lv:3 NOT lv:M{forbidden_sources}",
                        itemType="video",
                        itemSubType="video-digital",
                    )
                )
        elif resource.resourceCategoryId in range(4, 12):
            # print monograph materials
            if resource.standardNumber:
                payloads.append(
                    dict(
                        q=f"bn:{resource.standardNumber}{forbidden_sources}",
                        itemType="book",
                        itemSubType="book-printbook",
                        catalogSource="DLC",
                    )
                )
            if resource.congressNumber:
                payloads.append(
                    dict(
                        q=f"ln:{resource.congressNumber}{forbidden_sources}",
                        itemType="book",
                        itemSubType="book-printbook",
                        catalogSource="DLC",
                    )
                )
        logging.debug(
            f"Query payload for {self.library} Sierra bib # b{resource.sierraId}a: "
            f"{payloads}."
        )
        return payloads

    def get_brief_bibs(
        self, resources: list[Resource], rotten_apples: dict[int, list[str]] = {}
    ) -> Iterator[tuple[Resource, BriefBibResponse]]:
        """
        Performes WorldCat queries for each resource in the passed library batch.
        Resources must belong to the same library.

        Args:
            resources:                  `datastore.Resource` instances
            rotten_apples:              use to exclude a particular contributor to
                                        Worldcat from the results;
                                        pass as a dictionary where key is
                                        `ResourceCategory.nid` and value a list of
                                        OCLC organization codes

        yields:
            (`Resource`, `BriefBibResponse`)

        """
        try:
            for resource in resources:

                payloads = self._prep_resource_queries_payloads(resource, rotten_apples)
                if not payloads:
                    logger.warning(
                        f"Unable to create a payload for brief bib query for "
                        f"{self.library} resource nid={resource.nid}, "
                        f"sierraId=b{resource.sierraId}a."
                    )
                    continue

                for payload in payloads:
                    response = self.session.brief_bibs_search(
                        **payload,
                        inCatalogLanguage="eng",
                        orderBy="mostWidelyHeld",
                        limit=1,
                    )

                    brief_bib_response = BriefBibResponse(response)
                    logger.debug(
                        f"Brief bib Worldcat query for {self.library} Sierra bib "
                        f"# b{resource.sierraId}a: {response.url}"
                    )
                    if brief_bib_response.is_match:
                        logger.debug(
                            f"Match found for {self.library} Sierra bib # "
                            f"b{resource.sierraId}a."
                        )
                        break
                    else:
                        logger.debug(
                            f"No matches found for {self.library} Sierra bib # "
                            f"b{resource.sierraId}a: {response.url}"
                        )

                yield (resource, brief_bib_response)

        except WorldcatRequestError:
            logger.error(f"WorldcatRequestError. Aborting.")
            raise

    def get_full_bibs(
        self, resources: list[Resource]
    ) -> Iterator[tuple[Resource, bytes]]:
        """
        Makes MetadataAPI requests for full bibliographic resources
        """
        try:
            for resource in resources:
                response = self.session.bib_get(oclcNumber=resource.oclcMatchNumber)
                logger.debug(
                    f"Full bib Worldcat request for {self.library} Sierra bib # "
                    f"b{resource.sierraId}a: {response.url}."
                )
                yield (resource, response.content)
        except WorldcatRequestError:
            logger.error("WorldcatRequestError. Aborting.")
            raise
