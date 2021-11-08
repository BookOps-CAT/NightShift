# -*- coding: utf-8 -*-

"""
This module handles communication with NYPL Platform. It is used to check status of
records in Sierra.
"""
import logging
import os

from bookops_nypl_platform import PlatformToken, PlatformSession
from bookops_nypl_platform.errors import BookopsPlatformError

from .. import __title__, __version__
from ..ns_exceptions import SierraSearchPlatformError

logger = logging.getLogger("nightshift")


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
            client_id:                      NYPL Platform client_id
            client_secret:                  NYPL Platform client_secret
            oauth_server:                   NYPl Platform authorization server

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

    def bib_status(self, sierraId: int) -> str:
        """
        Searches NYPL Platform for a given sierra bib and returns its status

        Args:
            sierraId:                       Sierra bib

        Returns:
            status
        """
        pass
