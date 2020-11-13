# -*- coding: utf-8 -*-

"""
datastore.py schema tests
"""
import pytest

from nightshift.datastore import *


@pytest.mark.parametrize("column", ["lsid", "code", "name"])
def test_librarySystemTable_columns(column):
    assert hasattr(LibrarySystem(), column) is True


def test_LibrarySystemTable_str():
    table = LibrarySystem()
    assert table.__tablename__ == "library_system"
    assert hasattr(table, "lsid") is True
    assert hasattr(table, "code") is True
    assert hasattr(table, "name") is True
    assert (
        str(table)
        == "<LibrarySystem(lsid=symbol('NO_VALUE'), code=symbol('NO_VALUE'), name=symbol('NO_VALUE'))>"
    )


@pytest.mark.parametrize("column", ["bcid", "code", "description"])
def test_BibCategoryTable_columns(column):
    assert hasattr(BibCategory(), column) is True


def test_BibCategoryTable_str():
    table = BibCategory()
    assert table.__tablename__ == "bib_category"
    assert (
        str(table)
        == "<BibCategory(bcid=symbol('NO_VALUE'), code=symbol('NO_VALUE'), description=symbol('NO_VALUE'))>"
    )


@pytest.mark.parametrize("column", ["efid", "handle", "dateCreated"])
def test_ExportFileTable_columns(column):
    assert hasattr(ExportFile(), column) is True


def test_ExportFileTable_str():
    table = ExportFile()
    assert table.__tablename__ == "export_file"
    assert (
        str(table)
        == "<ExportFile(efid=symbol('NO_VALUE'), handle=symbol('NO_VALUE'), dateCreated=symbol('NO_VALUE'))>"
    )


@pytest.mark.parametrize("column", ["ofid", "handle", "dateCreated"])
def test_OutputFileTable_columns(column):
    assert hasattr(OutputFile(), column) is True


def test_OutputFileTable_str():
    table = OutputFile()
    assert table.__tablename__ == "output_file"
    assert (
        str(table)
        == "<OutputFile(ofid=symbol('NO_VALUE'), handle=symbol('NO_VALUE'), dateCreated=symbol('NO_VALUE'))>"
    )


@pytest.mark.parametrize("column", ["usid", "name", "description"])
def test_UpgradeSourceTable_columns(column):
    assert hasattr(UpgradeSource(), column) is True


def test_UpgradeSourceTable_str():
    table = UpgradeSource()
    assert table.__tablename__ == "upgrade_source"
    assert (
        str(table)
        == "<UpgradeSource(usid=symbol('NO_VALUE'), name=symbol('NO_VALUE'), description=symbol('NO_VALUE'))>"
    )


@pytest.mark.parametrize(
    "column",
    [
        "sbid",
        "librarySystemId",
        "bibCategoryId",
        "exportFileId",
        "outputFileId",
        "cno",
        "sbn",
        "lcn",
        "did",
        "sid",
        "wcn",
        "bibDate",
        "deleted",
        "title",
        "author",
        "pubDate",
        "upgradeStamp",
        "upgraded",
        "upgradeSourceId",
    ],
)
def test_ResourceTable_columns(column):
    assert hasattr(Resource(), column) is True


def test_ResourceTable_str():
    table = Resource()
    assert table.__tablename__ == "resource"
    assert (
        str(table)
        == "<Resource(urls=symbol('NO_VALUE'), wqueries=symbol('NO_VALUE'), sbid=symbol('NO_VALUE'), librarySystemId=symbol('NO_VALUE'), bibCategoryId=symbol('NO_VALUE'), exportFileId=symbol('NO_VALUE'), outputFileId=symbol('NO_VALUE'), cno=symbol('NO_VALUE'), sbn=symbol('NO_VALUE'), lcn=symbol('NO_VALUE'), did=symbol('NO_VALUE'), sid=symbol('NO_VALUE'), wcn=symbol('NO_VALUE'), bibDate=symbol('NO_VALUE'), deleted=symbol('NO_VALUE'), title=symbol('NO_VALUE'), author=symbol('NO_VALUE'), pubDate=symbol('NO_VALUE'), upgradeStamp=symbol('NO_VALUE'), upgraded=symbol('NO_VALUE'), upgradeSourceId=symbol('NO_VALUE'))>"
    )


@pytest.mark.parametrize("column", ["utid", "utype", "__tablename__"])
def test_UrlType(column):
    assert hasattr(UrlType(), column) is True


def test_UrlType_str():
    table = UrlType()
    assert table.__tablename__ == "url_type"
    assert str(table) == "<UrlType(utid=symbol('NO_VALUE'), utype=symbol('NO_VALUE'))>"


@pytest.mark.parametrize(
    "column", ["ufid", "sBibId", "uTypeId", "url", "__tablename__"]
)
def test_UrlField_columns(column):
    assert hasattr(UrlField(), column) is True


def test_UrlFieldTable_str():
    table = UrlField()
    assert table.__tablename__ == "url_field"
    assert (
        str(table)
        == "<UrlField(ufid=symbol('NO_VALUE'), sBibId=symbol('NO_VALUE'), librarySystemId=symbol('NO_VALUE'), uTypeId=symbol('NO_VALUE'), url=symbol('NO_VALUE'))>"
    )


@pytest.mark.parametrize(
    "column", ["wqid", "sBibId", "queryStamp", "found", "wcResponse"]
)
def test_WorlcatQueryTable_columns(column):
    assert hasattr(WorldcatQuery(), column) is True


def test_WorlcatQueryTable_str():
    table = WorldcatQuery()
    assert table.__tablename__ == "worldcat_query"
    assert (
        str(table)
        == "<WorldcatQuery(wqid=symbol('NO_VALUE'), sBibId=symbol('NO_VALUE'), queryStamp=symbol('NO_VALUE'), found=symbol('NO_VALUE'), wcResponse=symbol('NO_VALUE'))>"
    )
