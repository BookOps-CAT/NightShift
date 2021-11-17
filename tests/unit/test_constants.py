# -*- coding: utf-8 -*-


from nightshift.constants import (
    library_by_id,
    resource_category_by_id,
    sierra_format_code,
    tags2delete,
)


def test_library_by_nid():
    result = library_by_id()
    assert isinstance(result, dict)
    for key in result.keys():
        assert isinstance(key, int)

    assert result[1] == "NYP"
    assert result[2] == "BPL"


def test_resource_category_by_nid():
    result = resource_category_by_id()
    assert isinstance(result, dict)
    for k, v in result.items():
        assert isinstance(k, int)
        assert isinstance(v, str)

    assert result[1] == "ebook"
    assert result[2] == "eaudio"
    assert result[3] == "evideo"
    assert result[4] == "print_eng_adult_fic"


def test_sierra_format_code():
    result = sierra_format_code()
    assert isinstance(result, dict)
    for key in result.keys():
        assert isinstance(key, int)

    assert result[1] == {"NYP": "z", "BPL": "x"}
    assert result[4] == {"NYP": "a", "BPL": "a"}


def test_tags2delete_correct_keys():
    result = tags2delete()
    assert isinstance(result, dict)
    for key in result.keys():
        assert isinstance(key, int)


def test_tags2delete_correct_values():
    result = tags2delete()
    values = result.values()
    for v in values:
        assert isinstance(v, list)

    assert result[1] == ["020", "029", "037", "090", "856", "910", "938"]
    assert result[4] == ["029", "090", "263", "936", "938"]
