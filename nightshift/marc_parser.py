# -*- coding: utf-8 -*-

"""
This module reads and parses MARC Sierra records (bibliographic and order data)
to be inserted into the DB.
"""
from collections import namedtuple

from bookops_marc import SierraBibReader


BibMeta = namedtuple(
    "BibMeta",
    [
        "sierraId",
        "libraryId",
        "resourceCategoryId",
        "bibDate",
        "author",
        "title",
        "pubDate",
        "congressNumber",
        "controlNumber",
        "distributorNumber",
        "otherNumber",
        "srcFieldsToKeep",
        "standardNumber",
        "status",
    ],
)


class BibReader:
    def __init__(self, file_handle):
        pass
