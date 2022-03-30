# -*- coding: utf-8 -*-
"""
Use following patterns to record data:
    src_tags2delete & dst_tags2delete: string of MARC tags separated by comma, example:
        "020,037,856"

    queryDays: 
        record as string;

        all numbers indicate days since bib was created, example: '15' means 15 days after
        bib created date

        to indicate an individual period record the starting and ending age in days separated
        by hyphen, example: '15-30' which means query should happen between 15th and 30th day
        since bib was created

        separate individual periods with a comma, example: '15-30,30-60'

        multiple time periods will trigger as many query attemps, one in each period
        
"""


LIBRARIES = {"NYP": {"nid": 1}, "BPL": {"nid": 2}}

PRINT_TAGS_TO_DELETE = "029,090,263,936,938"
PRINT_QUERY_DAYS = "15-30,30-45"

BPL_SIERRA_FORMAT = {
    "ebook": "x",
    "eaudio": "z",
    "evideo": "v",
    "print": "a",
    "large_print": "l",
}

NYP_SIERRA_FORMAT = {
    "ebook": "z",
    "eaudio": "n",
    "evideo": "3",
    "print": "a",
    "large_print": "l",
}


RESOURCE_CATEGORIES = {
    "ebook": {
        "nid": 1,
        "description": "digital book",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["ebook"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["ebook"],
        "src_tags2keep": "020,037,856",
        "dst_tags2delete": "020,029,037,090,263,856,910,938",
        "query_days": "30-90,90-180",
    },
    "eaudio": {
        "nid": 2,
        "description": "digital audiobook",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["eaudio"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["eaudio"],
        "src_tags2keep": "020,037,856",
        "dst_tags2delete": "020,029,037,090,263,856,910,938",
        "query_days": "30-90,90-180",
    },
    "evideo": {
        "nid": 3,
        "description": "video digital",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["evideo"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["evideo"],
        "src_tags2keep": "020,037,856",
        "dst_tags2delete": "020,029,037,090,263,856,910,938",
        "query_days": "30-90",
    },
    "print_eng_adult_fic": {
        "nid": 4,
        "description": "Print English general adult fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_bio": {
        "nid": 5,
        "description": "Print English adult biography",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_nonfic": {
        "nid": 6,
        "description": "Print English adult non-fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_mystery": {
        "nid": 7,
        "description": "Print English adult mysteries",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_scifi": {
        "nid": 8,
        "description": "Print English adult science-fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_fic": {
        "nid": 9,
        "description": "Print English juvenile fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_bio": {
        "nid": 10,
        "description": "Print English juvenile bio",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_nonfic": {
        "nid": 11,
        "description": "Print English juvenile fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "src_tags2keep": "910",
        "dst_tags2delete": PRINT_TAGS_TO_DELETE,
        "query_days": PRINT_QUERY_DAYS,
    },
}


ROTTEN_APPLES = {"UKAHL": ["ebook", "eaudio", "evideo"], "UAH": ["ebook"]}


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
