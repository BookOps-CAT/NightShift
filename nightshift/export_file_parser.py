# -*- coding: utf-8 -*-

"""
This module reads and parses exports to a file from Siera ILS
"""

import csv
from collections import namedtuple
import datetime
from typing import Type

from .datastore_values import LIB_SYS, BIB_CAT
from .errors import SierraExportReaderError


ResourceMeta = namedtuple(
    "ResourceMeta",
    [
        "sbid",
        "lsid",
        "bcid",
        "efid",
        "sbn",
        "lcn",
        "did",
        "sid",
        "wcn",
        "bibDate",
        "title",
        "author",
        "pubDate",
    ],
)


class SierraExportReader:
    def __init__(self, file_handle):

        if type(file_handle) is not str or file_handle == "":
            raise SierraExportReaderError(
                "No file handle was passed to Sierra export reader."
            )

        self.fh = file_handle
        self.lsid = self._determine_library_system_id(file_handle)
        self.bcid = self._determine_bib_category_id(file_handle)

    def _determine_library_system_id(self, fh: str) -> int:
        """
        Determines library system based on file name prefix

        Args:
            fh:                     file handle

        Returns:
            lsid
        """
        if "nyp" in fh:
            lsid = LIB_SYS["nyp"]["lsid"]
        elif "bpl" in fh:
            lsid = LIB_SYS["bpl"]["lsid"]
        else:
            raise SierraExportReaderError(
                "Sierra export file handle has invalid format"
            )
        return lsid

    def _determine_bib_category_id(self, fh: str) -> int:
        """
        Determines bib category based on a file name

        Args:
            fh:                     file handle

        Returns:
            bcid
        """
        if "ebk" in fh:
            bcid = BIB_CAT["ebk"]["bcid"]
        elif "pre" in fh:
            bcid = BIB_CAT["pre"]["bcid"]
        else:
            raise SierraExportReaderError(
                "Sierra export file handle has invalid bib category."
            )
        return bcid

    def _determine_bib_created_date(self, created: str) -> Type[datetime.date]:
        """
        Parses Sierra bib created_date and converts it date obj

        Args:
            created:                created date as str

        Returns:
            `datetime.date` object
        """
        pass
