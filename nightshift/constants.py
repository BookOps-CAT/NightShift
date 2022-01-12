# -*- coding: utf-8 -*-

LIBRARIES = {"NYP": {"nid": 1}, "BPL": {"nid": 2}}

PRINT_TAGS_TO_DELETE = ["029", "090", "263", "936", "938"]
PRINT_QUERY_DAYS = [(15, 30), (30, 45)]

RESOURCE_CATEGORIES = {
    "ebook": {
        "nid": 1,
        "description": "digital book",
        "sierra_format_code": {"NYP": "z", "BPL": "x"},
        "src_tags2keep": ["020", "037", "856"],
        "dst_tags2delete": ["020", "029", "037", "090", "856", "910", "938"],
        "query_days": [(30, 90), (90, 180)],
    },
    "eaudio": {
        "nid": 2,
        "description": "digital audiobook",
        "sierra_format_code": {"NYP": "n", "BPL": "z"},
        "src_tags2keep": ["020", "037", "856"],
        "dst_tags2delete": ["020", "029", "037", "090", "856", "910", "938"],
        "query_days": [(30, 90), (90, 180)],
    },
    "evideo": {
        "nid": 3,
        "description": "video digital",
        "sierra_format_code": {"NYP": "3", "BPL": "v"},
        "src_tags2keep": ["020", "037", "856"],
        "dst_tags2delete": ["020", "029", "037", "090", "856", "910", "938"],
        "query_days": [(30, 90)],
    },
    "print_eng_adult_fic": {
        "nid": 4,
        "description": "Print English general adult fiction",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_bio": {
        "nid": 5,
        "description": "Print English adult biography",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_nonfic": {
        "nid": 6,
        "description": "Print English adult non-fiction",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_mystery": {
        "nid": 7,
        "description": "Print English adult mysteries",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_scifi": {
        "nid": 8,
        "description": "Print English adult science-fiction",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_fic": {
        "nid": 9,
        "description": "Print English juvenile fiction",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_bio": {
        "nid": 10,
        "description": "Print English juvenile bio",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_nonfic": {
        "nid": 11,
        "description": "Print English juvenile fiction",
        "sierra_format_code": {"NYP": "a", "BPL": "a"},
        "src_tags2keep": ["910"],
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
}


SIERRA_FORMATS = {
    "NYP": {
        "ebook": "z",
        "eaudio": "n",
        "evideo": "3",
        "print": "a",
        "large_print": "l",
    },
    "BPL": {
        "ebook": "x",
        "eaudio": "z",
        "evideo": "v",
        "print": "a",
        "large_print": "l",
    },
}


def library_by_id() -> dict[int, str]:
    """
    Creates a dictionary where the key is `datastore.Library.nid` and a value is the
    library code.
    """
    return {v["nid"]: k for k, v in LIBRARIES.items()}


def resource_category_by_id() -> dict[int, str]:
    """
    Creates a dictionary of resource categories by their id
    """
    return {v["nid"]: k for k, v in RESOURCE_CATEGORIES.items()}


def sierra_format_code() -> dict[int, dict[str, str]]:
    """
    Returns dictionary of sierra codes for each resource category with keys being
    resource IDs.
    """
    return {v["nid"]: v["sierra_format_code"] for v in RESOURCE_CATEGORIES.values()}


def tags2delete() -> dict[int, list[str]]:
    """
    Produces a dictionary of tags to be deleted from WorldCat records.
    The dictionary's keys are IDs of rows in the `datastore.ResourceCategory` table.
    """
    return {v["nid"]: v["dst_tags2delete"] for v in RESOURCE_CATEGORIES.values()}
