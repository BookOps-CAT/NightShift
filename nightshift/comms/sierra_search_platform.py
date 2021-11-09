# -*- coding: utf-8 -*-

"""
This module handles communication with NYPL Platform and BPL Solr. 
It is used to check status of records in Sierra.
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

        Args:
            sierraId:                   Sierra bib number
            library:                    'nyp' or 'bpl'
            response:                   `requests.Response` instance from the service
        """
        self.sierraId = sierraId
        self.library = library

        if response.status_code > 404:
            logger.error(
                f"{(self.library).upper()} search platform returned HTTP error code {response.status_code} for request {response.url}"
            )
            raise SierraSearchPlatformError
        else:
            self.response = response

        self.json_response = response.json()

    def is_suppressed(self) -> bool:
        """
        Checks if Sierra bib is suppressed or not

        Returns:
            bool
        """
        if self.library == "nyp":
            return self._nyp_suppression()
        elif self.library == "bpl":
            return self._bpl_suppression()

    def get_status(self) -> str:
        """
        Determines status of record in Sierra

        Returns:
            status:                     'brief-bib', 'full-bib', 'deleted'
        """
        bib_status = None

        if self.response.status_code == 200:
            if self.library == "nyp":
                bib_status = self._determine_nyp_bib_status()
            elif self.library == "bpl":
                bib_status = self._determine_bpl_bib_status()
        elif self.response.status_code == 404:
            # on a rare occasion NYPL bibs don't get ingested into Platform
            logger.warning(
                f"{(self.library).upper()} Sierra bib # {self.sierraId} not found on Platform."
            )
            bib_status = "deleted"

        logger.debug(
            f"{(self.library).upper()} Sierra bib # {self.sierraId} status: {bib_status}"
        )
        return bib_status

    def _determine_bpl_bib_status(self) -> str:
        """
        Returns:
            brief-bib, full-bib, deleted
        """
        try:
            data = self.json_response["response"]["docs"][0]
        except IndexError:
            # no results, treat as deleted
            return "deleted"
        else:
            if data["deleted"]:
                return "deleted"
            # if bib orignated from Worldcat asssume full bib
            if "ss_marc_tag_003" in data and data["ss_marc_tag_003"] == "OCoLC":
                return "full-bib"

            # print material with call number tag - assume full bib
            # exclude electronic resources
            if "call_number" in data and not is_eresource_callno(data["call_number"]):
                return "full-bib"

        # assume at this point it must be a brief bib
        return "brief-bib"

    def _determine_nyp_bib_status(self) -> str:
        """
        Returns:
            bief-bib, full-bib or deleted
        """
        data = self.json_response["data"]
        if data["deleted"]:
            return "deleted"
        else:
            # check first if Sierra bib came from the Worldcat;
            # and catch here upgraded/enhanced electronic resources
            for field in data["varFields"]:
                if field["marcTag"] == "003" and field["content"] == "OCoLC":
                    return "full-bib"

            # print material full bibs may come from other sources than
            # Worldcat (no or diff content of the '003' tag);
            # brief bibs lack call numbers
            for field in data["varFields"]:
                if field["marcTag"] == "091":  # filter out electronic resources
                    if not is_eresource_callno(field["subfields"][0]["content"]):
                        return "full-bib"

            # assume response failing previous clauses is a brief bib
            return "brief-bib"

    def _bpl_suppression(self) -> bool:
        try:
            self.json_response["response"]["docs"][0]
        except IndexError:
            return False
        else:
            return self.json_response["response"]["docs"][0]["suppressed"]

    def _nyp_suppression(self) -> bool:
        if self.response.status_code == 404:
            return False
        else:
            return self.json_response["data"]["suppressed"]


class NypPlatform(PlatformSession):
    def __init__(self):
        """
        Authenticates and opens a session with NYPL Platform

        Args:
            target:                     'prod' or 'dev'
        """
        client_id, client_secret, oauth_server, target = self._get_credentials()
        token = self._get_token(client_id, client_secret, oauth_server)
        agent = f"{__title__}/{__version__}"

        PlatformSession.__init__(self, authorization=token, agent=agent, target=target)

    def _get_credentials(self):
        """
        Retrieves NYPL Platform credentials from environmental variables.

        Returns:
                (client_id, secret_id, oauth_server)
        """
        return (
            os.getenv("NYPL_PLATFORM_CLIENT"),
            os.getenv("NYPL_PLATFORM_SECRET"),
            os.getenv("NYPL_PLATFORM_OAUTH"),
            os.getenv("NYPL_PLATFORM_ENV"),
        )

    def _get_token(
        self, client_id: str, client_secret: str, oauth_server: str
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
            return token
        except BookopsPlatformError as exc:
            logger.error(f"Unable to obtain access token for NYPL Platform. {exc}")
            raise SierraSearchPlatformError

    def get_sierra_bib(self, sierraId: int) -> str:
        """
        Searches NYPL Platform for a given sierra bib and returns its status

        Args:
            sierraId:                       Sierra bib number

        Returns:
            status
        """
        try:
            response = self.get_bib(sierraId)
        except BookopsPlatformError as exc:
            logger.error(
                f"Error while querying NYPL Platform for Sierra bib # {sierraId}. {exc}"
            )
            raise SierraSearchPlatformError(exc)
        else:
            search_response = SearchResponse(sierraId, "nyp", response)
            return search_response


class BplSolr(SolrSession):
    def __init__(self, client_key: str, endpoint: str) -> None:
        SolrSession.__init__(self, authorization=client_key, endpoint=endpoint)

    def get_siera_bib(self, sierraId: int) -> str:
        """
        Searches BPL Solr for a given sierra bib and returns its status

        Args:
            sierraId:                       Sierra bib number

        Returns:
            status
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
        except BookopsSolrError as exc:
            logger.error(
                f"Error while querying BPL Solr for Sierra bib # {sierraId}. {exc}"
            )
            raise SierraSearchPlatformError(exc)
        else:
            sierra_bib = SearchResponse(sierraId, "bpl", response)
            return sierra_bib
