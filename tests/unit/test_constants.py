# -*- coding: utf-8 -*-


from nightshift.constants import library_by_nid, tags2delete, sierra_format_code


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


def test_library_by_nid():
    result = library_by_nid()
    assert isinstance(result, dict)
    for key in result.keys():
        assert isinstance(key, int)

    assert result[1] == "nyp"
    assert result[2] == "bpl"


def test_sierra_format_code():
    result = sierra_format_code()
    assert isinstance(result, dict)
    for key in result.keys():
        assert isinstance(key, int)

    assert result[1] == {"nyp": "z", "bpl": "x"}
    assert result[4] == {"nyp": "a", "bpl": "a"}
