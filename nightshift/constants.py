# -*- coding: utf-8 -*-

LIBRARIES = {"nyp": {"nid": 1}, "bpl": {"nid": 2}}
RESOURCE_CATEGORIES = {
    "e_resource": {
        "nid": 1,
        "description": "Electronic resources (including ebooks, eaudio, evideo)",
        "src_fields2keep": ["001", "020", "037", "856"],
        "dst_fields2delete": ["020", "029", "037", "090", "856", "938"],
        "query_days": [30, 90, 180],
    },
    "print_eng_adult_fic": {
        "nid": 2,
        "description": "Print English general adult fiction",
        "src_fields2keep": [],
        "dst_fields2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
    "print_eng_adult_bio": {
        "nid": 3,
        "description": "Print English adult biography",
        "src_fields2keep": [],
        "dst_fields2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
    "print_eng_adult_nonfic": {
        "nid": 4,
        "description": "Print English adult non-fiction",
        "src_fields2keep": [],
        "dst_fiels2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
    "print_eng_adult_mystery": {
        "nid": 5,
        "description": "Print English adult mysteries",
        "src_fields2keep": [],
        "dst_fields2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
    "print_eng_adult_scifi": {
        "nid": 6,
        "description": "Print English adult science-fiction",
        "src_fields2keep": [],
        "dst_fields2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
    "print_eng_juv_fic": {
        "nid": 7,
        "description": "Print English juvenile fiction",
        "src_fields2keep": [],
        "dst_fields2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
    "print_eng_juv_bio": {
        "nid": 8,
        "description": "Print English juvenile bio",
        "src_fields2keep": [],
        "dst_fields2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
    "print_eng_juv_nonfic": {
        "nid": 9,
        "description": "Print English juvenile fiction",
        "src_fields2keep": [],
        "dst_fields2delete": ["029", "090", "263", "936", "938"],
        "query_days": [15, 30],
    },
}
