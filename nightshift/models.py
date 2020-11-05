# -*- coding: utf-8 -*-

"""
This module provide data models used by NightShift bot
"""

from collections import namedtuple

SierraMeta = namedtuple(
    "SierraMeta",
    [
        "sbid",
        "sbn",
        "lcn",
        "did",
        "sid",
        "wcn",
        "title",
        "author",
        "pubDate",
        "upgradeStamp",
        "upgraded",
        "upgradeSourceId",
        "urls",
    ],
)
