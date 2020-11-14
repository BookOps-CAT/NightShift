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
    retrieve_brief_records_bibnos,
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
        (1, 1, [22259002, 22259003, 19099433]),
        (1, 2, [12345670, 12345671]),
        (2, 1, [22345678, 22345679, 19099433]),
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
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN1",
            did="reserve-id-5",
            bibDate=date(2020, 9, 30),
            wqueries=[
                WorldcatQuery(
                    sBibId=22259004,
                    found=False,
                    wcResponse=stub_nyp_platform_404_response,
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
                    wcResponse=stub_nyp_platform_404_response,
                )
            ],
        )
    )
    session.commit()

    records = retrieve_never_queried_records(session=session, lsid=lsid, bcid=bcid)
    assert len(records) == 3
