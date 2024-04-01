# -*- coding: utf-8 -*-
"""
Use following patterns to record data:
    src_tags2delete & dst_tags2delete: string of MARC tags separated by comma, example:
        "020,037,856"

    queryDays: 
        record as string;

        all numbers indicate days since bib was created, example: '15' means 15 days
        after bib created date

        to indicate an individual period record the starting and ending age in days
        separated by hyphen, example: '15-30' which means query should happen between
        15th and 30th day since bib was created

        separate individual periods with a comma, example: '15-30,30-60'

        multiple time periods will trigger as many query attempts, one in each period
        
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
        "srcTags2Keep": "020,037,856",
        "dstTags2Delete": "020,029,037,090,263,856,910,938",
        "queryDays": "30-90,90-180",
    },
    "eaudio": {
        "nid": 2,
        "description": "digital audiobook",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["eaudio"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["eaudio"],
        "srcTags2Keep": "020,037,856",
        "dstTags2Delete": "020,029,037,090,263,856,910,938",
        "queryDays": "30-90,90-180",
    },
    "evideo": {
        "nid": 3,
        "description": "video digital",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["evideo"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["evideo"],
        "srcTags2Keep": "020,037,856",
        "dstTags2Delete": "020,029,037,090,263,856,910,938",
        "queryDays": "30-90",
    },
    "print_eng_adult_fic": {
        "nid": 4,
        "description": "Print English general adult fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_bio": {
        "nid": 5,
        "description": "Print English adult biography",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_nonfic": {
        "nid": 6,
        "description": "Print English adult non-fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_mystery": {
        "nid": 7,
        "description": "Print English adult mysteries",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
    "print_eng_adult_scifi": {
        "nid": 8,
        "description": "Print English adult science-fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_fic": {
        "nid": 9,
        "description": "Print English juvenile fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_bio": {
        "nid": 10,
        "description": "Print English juvenile bio",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
    "print_eng_juv_nonfic": {
        "nid": 11,
        "description": "Print English juvenile fiction",
        "sierraBibFormatBpl": BPL_SIERRA_FORMAT["print"],
        "sierraBibFormatNyp": NYP_SIERRA_FORMAT["print"],
        "srcTags2Keep": "910",
        "dstTags2Delete": PRINT_TAGS_TO_DELETE,
        "queryDays": PRINT_QUERY_DAYS,
    },
}


ROTTEN_APPLES = {"UKAHL": ["ebook", "eaudio", "evideo"], "UAH": ["ebook"]}
