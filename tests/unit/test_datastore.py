# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session
from sqlalchemy.exc import DataError

from ..conftest import MockSuccessfulHTTP200SessionResponse

from nightshift.datastore import (
    conf_db,
    DataAccessLayer,
    Event,
    Library,
    OutputFile,
    Resource,
    ResourceCategory,
    RottenApple,
    RottenAppleResource,
    session_scope,
    SourceFile,
    WorldcatQuery,
)


def test_conf_db(mock_db_env):
    assert sorted(conf_db().keys()) == [
        "POSTGRES_DB",
        "POSTGRES_HOST",
        "POSTGRES_PASSWORD",
        "POSTGRES_PORT",
        "POSTGRES_USER",
    ]


def test_DataAccessLayer_connect(test_connection):
    dal = DataAccessLayer()
    dal.conn = test_connection
    with does_not_raise():
        dal.connect()


def test_session_scope_success(test_connection, test_session):
    with session_scope() as session:
        assert isinstance(session, Session)
        session.add(Library(nid=1, code="nyp"))

    # assert record was committed
    res = test_session.query(Library).filter_by(nid=1).one()
    assert res.code == "nyp"


def test_session_scope_exception_rollback(test_connection, test_session):
    with pytest.raises(DataError):
        with session_scope() as session:
            session.add(Library(nid=1, code="new york public"))

    res = test_session.query(Library).filter_by(nid=1).one_or_none()
    assert res is None


def test_Event_tbl_repr():
    stamp = datetime.now(timezone.utc)
    today = stamp.date()
    assert (
        str(
            Event(
                nid=1,
                timestamp=stamp,
                libraryId=1,
                sierraId=11111111,
                bibDate=today,
                resourceCategoryId=1,
                status="bot_cataloged",
            )
        )
        == f"<Event(nid='1', timestamp='{stamp}', libraryId='1', sierraId='11111111', "
        f"bibDate='{today}', resourceCategoryId='1', status='bot_cataloged')>"
    )


def test_Library_tbl_repr():
    assert str(Library(nid=1, code="foo")) == "<Library(nid='1', code='foo')>"


def test_OutputFile_tbl_repr():
    stamp = datetime.now(timezone.utc)
    assert (
        str(OutputFile(nid=1, libraryId=2, handle="foo.mrc", timestamp=stamp))
        == f"<OutputFile(nid='1', libraryId='2', handle='foo.mrc', timestamp='{stamp}')>"
    )


def test_Resource_tbl_repr():
    stamp = datetime.now(timezone.utc)
    bibDate = stamp.date()
    assert (
        str(
            Resource(
                nid=1,
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
                suppressed=True,
                status="open",
                oclcMatchNumber=None,
                enhanceTimestamp=stamp,
            )
        )
        == f"<Resource(nid='1', sierraId='1', libraryId='2', sourceId='5', "
        f"resourceCategoryId='3', bibDate='{bibDate}', author='foo', title='spam', "
        "pubDate='2021', controlNumber='0002', congressNumber='0001', "
        "standardNumber='0005', distributorNumber='0003', suppressed='True', "
        "status='open', outputId='None', oclcMatchNumber='None', "
        f"enhanceTimestamp='{stamp}')>"
    )


def test_ResourceCategory_tbl_repr():
    assert (
        str(
            ResourceCategory(
                nid=1,
                name="foo",
                description="spam",
                sierraBibFormatBpl="x",
                sierraBibFormatNyp="v",
                srcTags2Keep="020,037",
                dstTags2Delete="019,020",
                queryDays="15-30,30-45",
            )
        )
        == "<ResourceCategory(nid='1', name='foo', description='spam', "
        "sierraBibFormatBpl='x', sierraBibFormatNyp='v', srcTags2Keep='020,037', "
        "dstTags2Delete='019,020', queryDays='15-30,30-45')>"
    )


def test_RottenApple_tbl_repr():
    assert str(RottenApple(nid=1, code="FOO")) == "<RottenApple(nid='1', code='FOO')>"


def test_RottenAppleResource_tbl_repr():
    assert (
        str(RottenAppleResource(rottenAppleId=1, resourceCategoryId=2))
        == "<RottenAppleResource(rottenAppleId='1', resourceCategoryId='2')>"
    )


def test_SourceFile_tbl_repr():
    stamp = datetime.now()
    assert str(SourceFile(nid=1, libraryId=2, handle="foo.mrc", timestamp=stamp)) == (
        f"<SourceFile(nid='1', libraryId='2', handle='foo.mrc', "
        f"timestamp='{stamp}')>"
    )


def test_WorldcatQuery_tbl_repr():
    stamp = datetime.now()
    assert (
        str(
            WorldcatQuery(
                nid=1,
                resourceId=2,
                match=False,
                response=None,
                timestamp=stamp,
            )
        )
        == f"<WorldcatQuery(nid='1', resourceId='2', match='False', timestamp='{stamp}')>"
    )


def test_WorldcatQuery_tbl_json_column(test_session, test_data_core):
    test_session.add(
        Resource(
            nid=1,
            sierraId=22222222,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=datetime.now().date(),
            title="TEST",
            sourceId=1,
        )
    )
    test_session.commit()

    resp_json = MockSuccessfulHTTP200SessionResponse().json()
    test_session.add(WorldcatQuery(nid=1, resourceId=1, match=True, response=resp_json))
    test_session.commit()
    result = test_session.query(WorldcatQuery).filter_by(nid=1).first()
    assert type(result.response) == dict


def test_datastore_connection(test_connection):
    with does_not_raise():
        engine = create_engine(test_connection)
        engine.connect()
