# -*- coding: utf-8 -*-


from nightshift.constants import library_by_nid, tags2delete


def test_tags2delete_by_resource_category_nid_correct_keys():
    result = tags2delete()
    assert isinstance(result, dict)
    for key in result.keys():
        assert isinstance(key, int)


def test_tags2delete_by_resource_category_nid_correct_values():
    result = tags2delete()
    values = result.values()
    for v in values:
        assert isinstance(v, list)

    assert result[1] == ["020", "029", "037", "090", "856", "938"]
    assert result[4] == ["029", "090", "263", "936", "938"]


def test_library_by_nid():
    result = library_by_nid()
    assert isinstance(result, dict)
    for key in result.keys():
        assert isinstance(key, int)

    assert result[1] == "nyp"
    assert result[2] == "bpl"
