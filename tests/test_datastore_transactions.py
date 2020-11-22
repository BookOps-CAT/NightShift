# -*- coding: utf-8 -*-

"""
Tests datastore transactions
"""

from datetime import date, datetime, timedelta

import pytest

from nightshift.datastore import (
    BibCategory,
    dal,
    ExportFile,
    LibrarySystem,
    Resource,
    UpgradeSource,
    UrlField,
    UrlType,
    WorldcatQuery,
)
from nightshift.datastore_transactions import (
    calculate_date_using_days_from_today,
    construct_url_records,
    create_datastore,
    enhance_resource,
    insert,
    insert_export_file,
    insert_resource,
    recent_worldcat_query_records,
    retrieve_brief_records_bibnos,
    retrieve_records_not_queried_in_days,
    retrieve_never_queried_records,
    retrieve_records,
)

from nightshift.models import SierraMeta


@pytest.mark.parametrize(
    "arg,expectation",
    [(1, date.today() - timedelta(days=1)), (30, date.today() - timedelta(days=30))],
)
def test_calculate_date_using_days(arg, expectation):
    assert calculate_date_using_days_from_today(arg) == expectation


def test_init_in_memory_db(init_dataset):
    session = init_dataset
    assert len(session.query(LibrarySystem).all()) == 2
    assert len(session.query(BibCategory).all()) == 2
    assert len(session.query(UpgradeSource).all()) == 2
    assert len(session.query(UrlType).all()) == 4


def test_brief_bib_dataset(brief_bib_dataset):
    session = brief_bib_dataset
    assert len(session.query(LibrarySystem).all()) == 2
    assert len(session.query(BibCategory).all()) == 2
    assert len(session.query(UpgradeSource).all()) == 2
    assert len(session.query(UrlType).all()) == 4
    assert len(session.query(ExportFile).all()) == 4
    assert len(session.query(Resource).all()) == 10


def test_create_datastore():
    dal.conn_string = "sqlite:///:memory:"
    create_datastore(dal)

    session = dal.Session()

    assert len(session.query(LibrarySystem).all()) == 2
    assert len(session.query(BibCategory).all()) == 2
    assert len(session.query(UpgradeSource).all()) == 2
    assert len(session.query(UrlType).all()) == 4
    assert len(session.query(ExportFile).all()) == 0
    assert len(session.query(Resource).all()) == 0


def test_construct_url_records():
    sbid = 12345678
    lsid = 1
    urls = [
        dict(uTypeId=1, url="content_url"),
    ]
    assert (
        str(construct_url_records(sbid, lsid, urls)[0])
        == "<UrlField(ufid=symbol('NO_VALUE'), sBibId=12345678, librarySystemId=1, uTypeId=1, url='content_url')>"
    )


def test_enhance_resource_nyp(brief_bib_dataset):
    # setup
    session = brief_bib_dataset
    sbid = 22259002
    data = SierraMeta(
        sbid=sbid,
        sbn="isbns1,isbn2",
        lcn="lccn1",
        did="resourceId1",
        sid="upc1",
        wcn="oclc1",
        deleted=False,
        title="title_here",
        author="author_here",
        pubDate="2016",
        upgradeStamp=datetime(2020, 1, 1, 17, 0, 0),
        upgraded=True,
        upgradeSourceId=2,
        urls=[
            dict(uTypeId=1, url="content_url"),
            dict(
                uTypeId=2,
                url="sample_url",
            ),
            dict(uTypeId=3, url="image_url"),
            dict(uTypeId=4, url="thumbnail_url"),
        ],
    )
    # act
    enhance_resource(session, data, "nyp")

    # verify
    rec = session.query(Resource).filter_by(sbid=sbid).one()
    assert rec.sbid == 22259002
    assert rec.sbn == "isbns1,isbn2"
    assert rec.lcn == "lccn1"
    assert rec.did == "resourceId1"
    assert rec.sid == "upc1"
    assert rec.wcn == "oclc1"
    assert rec.deleted is False
    assert rec.title == "title_here"
    assert rec.author == "author_here"
    assert rec.pubDate == "2016"
    assert rec.upgradeStamp == datetime(2020, 1, 1, 17, 0, 0)
    assert rec.upgraded is True
    assert rec.upgradeSourceId == 2

    content = (
        session.query(UrlField)
        .filter_by(sBibId=22259002, librarySystemId=1, uTypeId=1)
        .one()
    )
    assert content.url == "content_url"

    sample = (
        session.query(UrlField)
        .filter_by(sBibId=22259002, librarySystemId=1, uTypeId=2)
        .one()
    )
    assert sample.url == "sample_url"

    image = (
        session.query(UrlField)
        .filter_by(sBibId=22259002, librarySystemId=1, uTypeId=3)
        .one()
    )
    assert image.url == "image_url"

    thumbnail = (
        session.query(UrlField)
        .filter_by(sBibId=22259002, librarySystemId=1, uTypeId=4)
        .one()
    )
    assert thumbnail.url == "thumbnail_url"


def test_insert(init_dataset):
    session = init_dataset
    record = insert(
        session, BibCategory, bcid=3, code="test", description="test category"
    )
    assert record.bcid == 3
    assert record.code == "test"
    assert record.description == "test category"


def test_insert_or_ignore_new(init_dataset):
    session = init_dataset
    resource = dict(
        sbid=12345678,
        librarySystemId=1,
        bibCategoryId=1,
        exportFileId=1,
        cno="ODN123456789",
        bibDate=date(2020, 9, 30),
    )
    rec = insert_resource(session, **resource)
    session.commit()

    assert len(session.query(Resource).all()) == 1
    assert rec.sbid == 12345678
    assert rec.cno == "ODN123456789"
    assert rec.bibDate == date(2020, 9, 30)


def test_insert_or_ingore_dup(init_dataset):
    session = init_dataset
    data = dict(
        sbid=12345678,
        librarySystemId=1,
        bibCategoryId=1,
        exportFileId=1,
        cno="ODN123456789",
        bibDate=date(2020, 9, 30),
    )
    # insert for the first time
    rec = insert_resource(session, **data)
    session.commit()

    assert len(session.query(Resource).all()) == 1
    assert rec.sbid == 12345678

    # make sure second insertions does not create dups
    rec = insert_resource(session, **data)
    session.commit()
    assert len(session.query(Resource).all()) == 1
    assert rec.sbid == 12345678


def test_insert_export_file_new(init_dataset):
    session = init_dataset
    data = dict(handle="test.txt")
    rec = insert_export_file(session, **data)
    session.commit()

    assert len(session.query(ExportFile).all()) == 1
    assert rec.efid == 1
    assert rec.handle == "test.txt"


def test_insert_export_file_dup(init_dataset):
    session = init_dataset
    data = dict(handle="test.txt")
    rec = insert_export_file(session, **data)
    session.commit()

    assert len(session.query(ExportFile).all()) == 1

    rec = insert_export_file(session, **data)
    session.commit()
    assert len(session.query(ExportFile).all()) == 1
    assert rec.efid == 1
    assert rec.handle == "test.txt"


@pytest.mark.parametrize(
    "lsid,bcid,expectation",
    [
        (1, 1, [22259002, 22259003]),
        (1, 2, [12345670, 12345671]),
        (2, 1, [22345678, 22345679]),
        (2, 2, [22345670, 22345671]),
    ],
)
def test_retrieve_brief_records_bibnos(lsid, bcid, expectation, brief_bib_dataset):
    session = brief_bib_dataset
    assert retrieve_brief_records_bibnos(session, lsid, bcid) == expectation


def test_retrieve_records(init_dataset):
    session = init_dataset
    recs = retrieve_records(session, LibrarySystem, code="nyp")
    assert len(recs) == 1


@pytest.mark.parametrize("lsid,bcid", [(1, 1), (2, 1)])
def test_retrieve_never_queried_records(
    lsid, bcid, brief_bib_dataset, stub_nyp_platform_404_response
):
    session = brief_bib_dataset

    # add some fake data
    session.add(
        Resource(
            sbid=22259004,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN1",
            did="reserve-id-14",
            bibDate=date(2020, 9, 30),
            wqueries=[
                WorldcatQuery(
                    sBibId=22259004,
                    found=False,
                )
            ],
        )
    )

    session.add(
        Resource(
            sbid=22345673,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN2",
            did="reserve-id-5",
            bibDate=date(2020, 9, 30),
            wqueries=[
                WorldcatQuery(
                    sBibId=22345673,
                    found=False,
                )
            ],
        )
    )
    session.commit()

    records = retrieve_never_queried_records(session=session, lsid=lsid, bcid=bcid)
    assert len(records) == 2


def test_retrieve_records_not_queried_in_days_default_mode(
    brief_bib_dataset, stub_nyp_platform_404_response
):
    session = brief_bib_dataset

    session.add(
        Resource(
            sbid=10000001,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN10",
            did="reserve-id-10",
            bibDate=date.today() - timedelta(days=14),
            wqueries=[
                WorldcatQuery(
                    wqid=1,
                    sBibId=10000001,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=7),
                ),
            ],
        )
    )
    session.add(
        Resource(
            sbid=10000002,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN11",
            did="reserve-id-11",
            bibDate=date.today() - timedelta(days=6),
            wqueries=[
                WorldcatQuery(
                    wqid=2,
                    sBibId=10000002,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=6),
                ),
                WorldcatQuery(
                    wqid=3,
                    sBibId=10000002,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=2),
                ),
            ],
        )
    )
    session.commit()
    records = retrieve_records_not_queried_in_days(session, lsid=1, bcid=1)

    assert [r.sbid for r in records] == [10000001]


@pytest.mark.parametrize(
    "arg,expectation",
    [
        (1, [10000001, 10000002, 10000003]),
        (6, [10000001, 10000003]),
        (13, [10000003]),
        (19, [10000003]),
        (25, []),
    ],
)
def test_retrieve_unqueried_one_month_old_records(
    arg, expectation, brief_bib_dataset, stub_nyp_platform_404_response
):
    session = brief_bib_dataset

    session.add(
        Resource(
            sbid=10000001,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN10",
            did="reserve-id-10",
            bibDate=date.today() - timedelta(days=14),
            wqueries=[
                WorldcatQuery(
                    wqid=1,
                    sBibId=10000001,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=7),
                ),
                WorldcatQuery(
                    wqid=2,
                    sBibId=10000001,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=28),
                ),
            ],
        )
    )
    session.add(
        Resource(
            sbid=10000002,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN11",
            did="reserve-id-11",
            bibDate=date.today() - timedelta(days=6),
            wqueries=[
                WorldcatQuery(
                    wqid=3,
                    sBibId=10000002,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=6),
                ),
                WorldcatQuery(
                    wqid=4,
                    sBibId=10000002,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=2),
                ),
            ],
        )
    )

    session.add(
        Resource(
            sbid=10000003,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN12",
            did="reserve-id-12",
            bibDate=date.today() - timedelta(days=20),
            wqueries=[
                WorldcatQuery(
                    wqid=5,
                    sBibId=10000003,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=21),
                )
            ],
        )
    )

    # this record should always be excluded - older than one month
    session.add(
        Resource(
            sbid=10000004,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN13",
            did="reserve-id-13",
            bibDate=date.today() - timedelta(days=60),
            wqueries=[
                WorldcatQuery(
                    wqid=6,
                    sBibId=10000004,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=30),
                )
            ],
        )
    )
    session.commit()

    records = retrieve_records_not_queried_in_days(
        session, lsid=1, bcid=1, query_cutoff_age=arg
    )

    assert [r.sbid for r in records] == expectation


@pytest.mark.parametrize(
    "bib_min,bib_max,query_age,expectation",
    [(29, 57, 28, [10000001]), (58, 86, 57, []), (87, 142, 86, [10000004])],
)
def test_retrieve_records_not_queried_in_days_custom(
    bib_min,
    bib_max,
    query_age,
    expectation,
    brief_bib_dataset,
    stub_nyp_platform_404_response,
):
    session = brief_bib_dataset

    # + one month old bib not queried in the last month
    session.add(
        Resource(
            sbid=10000001,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN10",
            did="reserve-id-10",
            bibDate=date.today() - timedelta(days=40),
            wqueries=[
                WorldcatQuery(
                    wqid=1,
                    sBibId=10000001,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=30),
                ),
                WorldcatQuery(
                    wqid=2,
                    sBibId=10000001,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=40),
                ),
            ],
        )
    )

    # younger than one month bib
    session.add(
        Resource(
            sbid=10000002,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN11",
            did="reserve-id-11",
            bibDate=date.today() - timedelta(days=6),
            wqueries=[
                WorldcatQuery(
                    wqid=3,
                    sBibId=10000002,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=6),
                ),
                WorldcatQuery(
                    wqid=4,
                    sBibId=10000002,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=2),
                ),
            ],
        )
    )
    # older than 3 months bib not queried for 2 months
    session.add(
        Resource(
            sbid=10000004,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN13",
            did="reserve-id-13",
            bibDate=date.today() - timedelta(days=120),
            wqueries=[
                WorldcatQuery(
                    wqid=5,
                    sBibId=10000004,
                    found=False,
                    queryStamp=datetime.now() - timedelta(days=90),
                )
            ],
        )
    )
    session.commit()

    records = retrieve_records_not_queried_in_days(
        session,
        lsid=1,
        bcid=1,
        bib_min_age=bib_min,
        bib_max_age=bib_max,
        query_cutoff_age=query_age,
    )

    assert [r.sbid for r in records] == expectation


def test_recent_worldcat_query_records(brief_bib_dataset):
    session = brief_bib_dataset

    subquery = recent_worldcat_query_records(session)
    assert str(type(subquery)) == "<class 'sqlalchemy.sql.selectable.Alias'>"
    assert (
        str(subquery)
        == '''SELECT worldcat_query."sBibId", worldcat_query.wqid AS wqid, max(worldcat_query."queryStamp") AS "wqStamp" 
FROM worldcat_query 
WHERE worldcat_query.found = false GROUP BY worldcat_query."sBibId"'''
    )
