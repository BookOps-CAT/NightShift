# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
import logging

from bookops_nypl_platform import PlatformToken
import pytest

from nightshift import __title__, __version__
from nightshift.ns_exceptions import SierraSearchPlatformError
from nightshift.comms.nyp_platform import NypPlatform


class TestNypPlatformMocked:
    def test_successful_initation(
        self, mock_platform_env, mock_successful_platform_post_token_response
    ):
        with does_not_raise():
            platform = NypPlatform()
            assert platform.headers == {
                "User-Agent": f"{__title__}/{__version__}",
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json",
                "Connection": "keep-alive",
                "Authorization": "Bearer token_string_here",
            }
            assert isinstance(platform.authorization, PlatformToken)

    def test_failed_authorization(
        self, caplog, mock_platform_env, mock_failed_platform_post_token_response
    ):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SierraSearchPlatformError):
                NypPlatform()

        assert "Unable to obtain access token for NYPL Platform." in caplog.text


@pytest.mark.local
class TestNypPlatformLiveDev:
    """
    This mail fail a couple of first times before the service spins up
    """

    def test_successful_initiation(self, live_dev_nyp_platform_env):
        with does_not_raise():
            NypPlatform()
