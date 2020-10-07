# -*- coding: utf-8 -*-

"""
This module reads and parses exports to a file from Siera ILS
"""

import csv
from collections import namedtuple

from .datastore_values import LIB_SYS
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

        print(file_handle)
        if type(file_handle) is not str or file_handle == "":
            raise SierraExportReaderError(
                "No file handle was passed to Sierra export reader."
            )

        self.fh = file_handle
        self.lsid = None
        self.bcid = None

        self._determine_library_system_id()

    def _determine_library_system_id(self):
        print(LIB_SYS["nyp"]["lsid"])
        if "nyp" in self.fh:
            self.lsid = LIB_SYS["nyp"]["lsid"]
        elif "bpl" in self.fh:
            self.lsid = LIB_SYS["bpl"]["lsid"]
        else:
            raise SierraExportReaderError("Sierr export file handle has invalid format")
