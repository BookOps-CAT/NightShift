# -*- coding: utf-8 -*-

"""
Predefined datastore values
"""


LIB_SYS = {
    "nyp": {"code": "nyp", "lsid": 1, "name": "New York Public Library"},
    "bpl": {"code": "bpl", "lsid": 2, "name": "Brooklyn Public Library"},
}

BIB_CAT = {
    "ebk": {"code": "ebk", "bcid": 1, "description": "Overdrive e-resources"},
    "pre": {"code": "pre", "bcid": 2, "description": "English print monograph"},
}

UPGRADE_SRC = {
    "bot": {
        "usid": 1,
        "name": "bot",
        "description": "full bib brought in by nightshift bot",
    },
    "staff": {
        "usid": 2,
        "name": "staff",
        "description": "full bib brought in by staff",
    },
}
