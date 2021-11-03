# -*- coding: utf-8 -*-

"""
This module contains NightShift exceptions
"""


class DriveError(Exception):
    """
    Exception raised when SFTP/Drive error is encountered
    """

    pass


class SierraSearchPlatformError(Exception):
    """
    Exception raised during sessions with NYPL Platform and BPL Solr
    """

    pass
