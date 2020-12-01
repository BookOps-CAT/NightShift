# -*- coding: utf-8 -*-

"""
This module provide data models used by NightShift bot
"""

from collections import namedtuple

FileMeta = namedtuple(
    "FileMeta",
    ["sbid", "librarySystemId", "sierraFormatId", "bibCategoryId", "cno", "bibDate"],
)

SierraMeta = namedtuple(
    "SierraMeta",
    [
        "sbid",
        "sbn",
        "lcn",
        "did",
        "sid",
        "wcn",
        "deleted",
        "sierraFormatId",
        "title",
        "author",
        "pubDate",
        "upgradeStamp",
        "upgraded",
        "upgradeSourceId",
        "urls",
    ],
)
