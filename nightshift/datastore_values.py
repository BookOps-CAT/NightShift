# -*- coding: utf-8 -*-

"""
Predefined datastore values
"""


BIB_CAT = {
    "ere": {"code": "ere", "bcid": 1, "description": "Overdrive e-resources"},
    "pre": {"code": "pre", "bcid": 2, "description": "English print monograph"},
}

LIB_SYS = {
    "nyp": {"code": "nyp", "lsid": 1, "name": "New York Public Library"},
    "bpl": {"code": "bpl", "lsid": 2, "name": "Brooklyn Public Library"},
}

SIERRA_FORMAT = {
    "unknown": {"name": "unknown", "sfid": 1},
    "ebook": {"name": "ebook", "sfid": 2},
    "eaudio": {"name": "eaudio", "sfid": 3},
    "evideo": {"name": "evideo", "sfid": 4},
    "book": {"name": "print", "sfid": 5},
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

URL_TYPE = {
    "content": {"utid": 1, "utype": "content"},
    "excerpt": {"utid": 2, "utype": "excerpt"},
    "image": {"utid": 3, "utype": "image"},
    "thumbnail": {"utid": 4, "utype": "thumbnail"},
}
