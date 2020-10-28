# -*- coding: utf-8 -*-

"""
This module reads and parses exports to a file from Siera ILS
"""

import csv
from collections import namedtuple
import datetime
from typing import List, Type

from .datastore_values import LIB_SYS, BIB_CAT
from .errors import SierraExportReaderError


ResourceMeta = namedtuple(
    "ResourceMeta",
    ["sbid", "lsid", "bcid", "cno", "bibDate"],
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
        try:
            created = datetime.datetime.strptime(created, "%m-%d-%Y").date()
        except (ValueError, TypeError) as exc:
            raise SierraExportReaderError(f"ValueError: {exc}")
        return created

    def _prep_sierra_bibno(self, sid: str) -> int:
        """
        Verifies and formats Sierra bib numbers

        Args:
            sid:            full Sierra bib number

        Returns:
            sid_int
        """
        err_msg = "Invalid Sierra number passed."
        try:
            sid_int = int(sid[1:-1])
        except (TypeError, ValueError) as exc:
            raise SierraExportReaderError(f"{err_msg}: {exc}")
        return sid_int

    def _map_data(self, row: List[str]) -> Type[namedtuple]:
        """
        Maps csv row data into datastore Resource record
        """
        bid = self._prep_sierra_bibno(row[0])
        created = self._determine_bib_created_date(row[1])
        cno = row[2].strip()
        return ResourceMeta(bid, self.lsid, self.bcid, cno, created)

    def __iter__(self):
        with open(self.fh, "r") as src_file:
            data = csv.reader(src_file)
            # skip header
            data.__next__()
            for row in data:
                if row:
                    record = self._map_data(row)
                    yield record
