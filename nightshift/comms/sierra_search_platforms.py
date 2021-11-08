# -*- coding: utf-8 -*-

"""
This module handles communication with NYPL Platform and BPL Solr. It is used to check status of
records in Sierra.
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


class SierraBib:
    def __init__(self, sierraId: int, library: str, response: Response) -> None:
        """

        Args:
            response:                   `requests.Response` instance from the service
            library:                    'nyp' or 'bpl'
        """
        self.sierraId = sierraId
        self.library = library
        self.response = response
        # self.status = self.status()
        # self.opac = self.opac_display()

    def is_suppressed(self) -> bool:
        """
        Checks if Sierra bib is suppressed or not

        Returns:
            bool
        """
        if self.library == "nyp":
            return self._determine_nyp_bib_opac_display()
        elif self.library == "bpl":
            return self._determine_bpl_bib_opac_display()

    def get_status(self) -> Optional[str]:
        """
        Determines status of record in Sierra

        Returns:
            status:                     'brief-bib', 'full-bib', 'deleted', 'suppressed'
        """
        bib_status = None
        if self.response.status_code == 404:
            logger.warning(
                f"{(self.library).upper()} Sierra bib # {self.sierraId} not found on Platform."
            )
            bib_status = "deleted"
        else:
            if self.library == "nyp":
                bib_status = self._determine_nyp_bib_status()
            elif self.library == "bpl":
                bib_status = self._determine_bpl_bib_status()

        logger.debug(
            f"{(self.library).upper()} Sierra bib # {self.sierraId} status: {bib_status}"
        )
        return bib_status

    def _determine_nyp_bib_status(self) -> str:
        """
        Returns:
            bief-bib, full-bib or deleted
        """
        data = self.response.json()["data"]
        if data["deleted"]:
            return "deleted"
        else:
            # check first if Sierra bib came from the Worldcat;
            # and catch here upgraded/enhanced electronic resources
            for field in data["varFields"]:
                if field["marcTag"] == "003" and field["content"] == "OCoLC":
                    return "full-bib"

            # print full bibs may come from other sources than
            # Worldcat (no or diff content of the '003' tag);
            # brief bibs lack call numbers
            for field in data["varFields"]:
                if field["marcTag"] in (
                    "091",
                    "852",
                ):  # filter out electronic resources
                    if not self._is_nyp_eresource_call_no(field["content"]):
                        return "full-bib"

            # assume response failing previous clauses is a brief bib
            return "brief-bib"

    def _determine_nyp_bib_opac_display(self) -> bool:
        if self.response.status_code == 404:
            return False
        else:
            return self.response.json()["data"]["suppressed"]

    def _determine_bpl_bib_status(self) -> str:
        """
        Returns:
            brief-bib, full-bib, deleted
        """
        try:
            data = self.response.json()["response"]["docs"][0]
        except IndexError:
            # no results
            return "deleted"
        else:
            if "ss_marc_tag_003" in data and data["ss_marc_tag_003"] == "OCoLC":
                return "full-bib"
            if data["call_number"] != "" and not self._is_bpl_eresouce_call_no(
                data["call_number"]
            ):
                return "full-bib"

        # assume at this point it must be a brief bib
        return "brief-bib"

    def _determine_bpl_bib_opac_display(self) -> bool:
        pass

    def _is_nyp_eresource_call_no(self, content: str) -> bool:
        """
        Checks if call number starts with 'eNYPL' for electronic resources

        Args:
            content:                    variable field content

        Returns:
            bool
        """
        if "enypl" in content.lower():
            return True
        else:
            return False


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
            sierraId:                       Sierra bib

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
            sierra_bib = SierraBib(sierraId, "nyp", response)
            return sierra_bib


class BplSolr(SolrSession):
    def __init__(self, client_key: str, endpoint: str) -> None:
        SolrSession.__init__(self, authorization=client_key, endpoint=endpoint)

    def get_siera_bib(self, sierraId: int) -> str:
        """
        Searches BPL Solr for a given sierra bib and returns its status

        Args:
            sierraId:                       Sierra bib

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
            sierra_bib = SierraBib(sierraId, "bpl", response)
            return sierra_bib
