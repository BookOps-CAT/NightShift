# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
from datetime import datetime

from sqlalchemy import create_engine

from nightshift.datastore import (
    conf_db,
    DataAccessLayer,
    Library,
    OutputFile,
    Resource,
    ResourceCategory,
    SourceFile,
    WorldcatQuery,
)


def test_conf_db(mock_db_env):
    assert sorted(conf_db().keys()) == [
        "NS_DBHOST",
        "NS_DBNAME",
        "NS_DBPASSW",
        "NS_DBPORT",
        "NS_DBUSER",
    ]


def test_DataAccessLayer_connect(test_connection):
    dal = DataAccessLayer()
    dal.conn = test_connection
    with does_not_raise():
        dal.connect()


def test_Library_tbl_repr():
    assert str(Library(nid=1, code="foo")) == "<Library(nid='1', code='foo')>"


def test_OutputFile_tbl_repr():
    stamp = datetime.now()
    assert (
        str(OutputFile(nid=1, libraryId=2, handle="foo.mrc", timestamp=stamp))
        == f"<OutputFile(nid='1', libraryId='2', handle='foo.mrc', timestamp='{stamp}')>"
    )


def test_Resource_tbl_repr():
    stamp = datetime.utcnow()
    bibDate = stamp.date()
    assert (
        str(
            Resource(
                sierraId=1,
                libraryId=2,
                resourceCategoryId=3,
                bibDate=bibDate,
                author="foo",
                title="spam",
                pubDate="2021",
                congressNumber="0001",
                controlNumber="0002",
                distributorNumber="0003",
                otherNumber="0004",
                outputId=None,
                sourceId=5,
                srcFieldsToKeep=None,
                standardNumber="0005",
                status="open",
                deleted=False,
                deletedTimestamp=stamp,
                oclcMatchNumber=None,
                upgradeTimestamp=stamp,
            )
        )
        == f"<Resource(sierraId='1', libraryId='2', sourceId='5', resourceCategoryId='3', bibDate='{bibDate}', author='foo', title='spam', pubDate='2021', controlNumber='0002', congressNumber='0001', standardNumber='0005', distributorNumber='0003', status='open', deleted='False', deletedTimestamp='{stamp}', outputId='None', oclcMatchNumber='None', upgradeTimestamp='{stamp}')>"
    )


def test_ResourceCategory_tbl_repr():
    assert (
        str(ResourceCategory(nid=1, code="foo", description="spam"))
        == "<ResourceCategory(nid='1', code='foo', description='spam')>"
    )


def test_SourceFile_tbl_repr():
    stamp = datetime.now()
    assert (
        str(SourceFile(nid=1, libraryId=2, handle="foo.mrc", timestamp=stamp))
        == f"<SourceFile(nid='1', libraryId='2', handle='foo.mrc', timestamp='{stamp}')>"
    )


def test_WorldcatQuery_tbl_repr():
    assert (
        str(
            WorldcatQuery(
                nid=1,
                resourceId=2,
                libraryId=1,
                match=False,
                responseCode="404",
                response=None,
            )
        )
        == "<WorldcatQuery(nid='1', resourceId='2', libraryId='1', match='False', responseCode='404')>"
    )


def test_datastore_connection(test_connection):
    with does_not_raise():
        engine = create_engine(test_connection)
        engine.connect()
