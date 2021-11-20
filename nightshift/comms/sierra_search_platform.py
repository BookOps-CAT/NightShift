# -*- coding: utf-8 -*-

"""
This module handles communication with NYPL Platform and BPL Solr.
It is used to check status of records in Sierra between WorldCat queries
so fully cataloged or deleted bibs are dropped from the process.
"""
import logging
import os
from typing import Optional

from bookops_nypl_platform import PlatformToken, PlatformSession
from bookops_nypl_platform.errors import BookopsPlatformError
from bookops_bpl_solr import SolrSession
from bookops_bpl_solr.session import BookopsSolrError
from requests import Response

from .. import __title__, __version__
from ..ns_exceptions import SierraSearchPlatformError

logger = logging.getLogger("nightshift")


def is_eresource_callno(callno: str) -> bool:
    """
    Checks if call number is for electronic resource

    Args:
        callno:                         call number string

    Returns:
        bool
    """
    try:
        norm_callno = callno.lower()
    except AttributeError:
        return False

    if norm_callno.startswith("enypl"):  # NYPL pattern
        return True
    elif norm_callno in ("ebook", "eaudio", "evideo"):  # BPL pattern
        return True
    else:
        return False


class SearchResponse:
    def __init__(self, sierraId: int, library: str, response: Response) -> None:
        """
        Initiates SearchResponse object.

        Args:
            sierraId:                   Sierra bib number
            library:                    'NYP' or 'BPL'
            response:                   `requests.Response` instance from the service

        Raises:
            `ns_exceptions.SierraSearchPlatformError`
        """
        self.sierraId = sierraId
        self.library = library.upper()

        if response.status_code == 404:
            logger.warning(
                f"{self.library} Sierra b{self.sierraId}a not found (404 HTTP code). Request: {response.url}"
            )
        elif response.status_code >= 400:
            logger.error(
                f"{self.library} search platform returned HTTP error code {response.status_code} for request {response.url}"
            )
            raise SierraSearchPlatformError

        self.response = response
        self.json_response = response.json()

    def is_suppressed(self) -> Optional[bool]:
        """
        Checks if Sierra bib is suppressed.

        Returns:
            bool
        """
        if self.library == "NYP":
            return self._nyp_suppression()
        elif self.library == "BPL":
            return self._bpl_suppression()
        else:
            return None

    def get_status(self) -> Optional[str]:
        """
        Determines status of record in Sierra

        Returns:
            'brief-bib', 'full-bib', or 'deleted' status
        """
        bib_status = None

        if self.response.status_code == 200:
            if self.library == "NYP":
                bib_status = self._determine_nyp_bib_status()
            elif self.library == "BPL":
                bib_status = self._determine_bpl_bib_status()
        elif self.response.status_code == 404:
            # on a rare occasion NYPL bibs may not get ingested into Platform
            bib_status = "deleted_staff"

        logger.debug(
            f"{self.library} Sierra bib # {self.sierraId} status: {bib_status}"
        )
        return bib_status

    def _determine_bpl_bib_status(self) -> str:
        """
        Determines status of record in BPL Sierra.

        Returns:
            'brief-bib', 'full-bib', 'deleted' status
        """
        try:
            data = self.json_response["response"]["docs"][0]
        except IndexError:
            # no results, treat as deleted
            return "deleted_staff"
        else:
            if data["deleted"]:
                return "deleted_staff"
            # if bib orignated from Worldcat asssume full bib
            if "ss_marc_tag_003" in data and data["ss_marc_tag_003"] == "OCoLC":
                return "upgraded_staff"

            # print material with call number tag - assume full bib
            # exclude electronic resources
            if "call_number" in data and not is_eresource_callno(data["call_number"]):
                return "upgraded_staff"

        # assume at this point it must be a brief bib
        return "open"

    def _determine_nyp_bib_status(self) -> str:
        """
        Determines status of record in NYPL Sierra.

        Returns:
            'bief-bib', 'full-bib' or 'deleted' status
        """
        data = self.json_response["data"]
        if data["deleted"]:
            return "deleted_staff"
        else:
            # check first if Sierra bib came from the Worldcat;
            # and catch here upgraded/enhanced electronic resources
            for field in data["varFields"]:
                if field["marcTag"] == "003" and field["content"] == "OCoLC":
                    return "upgraded_staff"

            # print material full bibs may come from other sources than
            # Worldcat (no or diff content of the '003' tag);
            # brief bibs lack call numbers
            for field in data["varFields"]:
                if field["marcTag"] == "091":  # filter out electronic resources
                    if not is_eresource_callno(field["subfields"][0]["content"]):
                        return "upgraded_staff"

            # assume response failing previous clauses is a brief bib
            return "open"

    def _bpl_suppression(self) -> bool:
        """
        Checks if BPL Sierra bib is suppressed from display.

        Returns:
            bool
        """
        if len(self.json_response["response"]["docs"]) == 0:
            return False
        elif self.json_response["response"]["docs"][0]["suppressed"]:
            return True
        else:
            return False

    def _nyp_suppression(self) -> bool:
        """
        Checks if NYPL Sierra bib is suppressed from display.

        Returns:
            bool
        """
        if self.response.status_code == 404:
            return False
        elif self.json_response["data"]["suppressed"]:
            return True
        else:
            return False


class NypPlatform(PlatformSession):
    def __init__(self) -> None:
        """
        Authenticates and opens a session with NYPL Platform.
        Relies on credentials stored in evironmental variables.
        """
        client_id, client_secret, oauth_server, target = self._get_credentials()
        token = self._get_token(client_id, client_secret, oauth_server)
        agent = f"{__title__}/{__version__}"

        super().__init__(authorization=token, agent=agent, target=target)
        logger.info("NYPL Platform session initiated.")

    def _get_credentials(
        self,
    ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Retrieves NYPL Platform credentials from environmental variables.

        Returns:
                (client_id, secret_id, oauth_server, platform_env)
        """
        return (
            os.getenv("NYPL_PLATFORM_CLIENT"),
            os.getenv("NYPL_PLATFORM_SECRET"),
            os.getenv("NYPL_PLATFORM_OAUTH"),
            os.getenv("NYPL_PLATFORM_ENV"),
        )

    def _get_token(
        self,
        client_id: Optional[str],
        client_secret: Optional[str],
        oauth_server: Optional[str],
    ) -> PlatformToken:
        """
        Obtains an access token for NYPL Platform

        Args:
            client_id:                      NYPL Platfrom client id
            client_secret:                  NYPL Platform client secret
            oauth_server:                   NYPL Platform authorization server

        Returns:
            `PlatformToken` instance

        Raises:
            SierraSearchPlatformError
        """
        try:
            token = PlatformToken(client_id, client_secret, oauth_server)
            logger.info("NYPL Platform access token obtained.")
            return token
        except BookopsPlatformError as exc:
            logger.error(f"Unable to obtain access token for NYPL Platform. {exc}")
            raise SierraSearchPlatformError

    def get_sierra_bib(self, sierraId: int) -> SearchResponse:
        """
        Searches NYPL Platform for a given sierra bib and returns its status

        Args:
            sierraId:                       Sierra bib number

        Returns:
            `SearchResponse` instance

        Raises:
            `ns_exceptions.SierraSearchPlatformError`
        """
        try:

            response = self.get_bib(sierraId)
            logger.debug(
                f"NYPL Platform request ({response.status_code}): {response.url}."
            )
            search_response = SearchResponse(sierraId, "NYP", response)

            return search_response

        except BookopsPlatformError as exc:
            logger.error(
                f"Error while querying NYPL Platform for Sierra bib # {sierraId}. {exc}"
            )
            raise SierraSearchPlatformError(exc)


class BplSolr(SolrSession):
    def __init__(self) -> None:
        """
        Creates BPL Solr session.
        """
        client_key, endpoint = self._get_credentials()
        agent = f"{__title__}/{__version__}"

        super().__init__(
            authorization=client_key,
            endpoint=endpoint,
            agent=agent,
        )

    def _get_credentials(self) -> tuple[Optional[str], Optional[str]]:
        """
        Obtains credentials from environmental variables.

        Returns:
            (client_key, endpoint)
        """
        return (os.getenv("BPL_SOLR_CLIENT_KEY"), os.getenv("BPL_SOLR_ENDPOINT"))

    def get_sierra_bib(self, sierraId: int) -> SearchResponse:
        """
        Searches BPL Solr for a given sierra bib and returns its status

        Args:
            sierraId:                       Sierra bib number

        Returns:
            `sierra_search_platform.SearchResponse` instance

        Raises:
            `ns_exceptions.SierraSearchPlatformError`
        """
        try:
            response = self.search_bibNo(
                sierraId,
                default_response_fields=False,
                response_fields=[
                    "id",
                    "suppressed",
                    "deleted",
                    "call_number",
                    "ss_marc_tag_003",
                ],
            )
            search_response = SearchResponse(sierraId, "BPL", response)

            return search_response

        except BookopsSolrError as exc:
            logger.error(
                f"Error while querying BPL Solr for Sierra bib # {sierraId}. {exc}"
            )
            raise SierraSearchPlatformError(exc)
