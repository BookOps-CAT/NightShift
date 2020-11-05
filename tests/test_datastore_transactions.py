# -*- coding: utf-8 -*-

"""
Tests datastore transactions
"""

from datetime import date

import pytest

from nightshift.datastore import (
    dal,
    ExportFile,
    LibrarySystem,
    BibCategory,
    UpgradeSource,
    UrlType,
    Resource,
)
from nightshift.datastore_transactions import (
    create_datastore,
    enhance_resource,
    insert,
    insert_resource,
    insert_export_file,
    retrieve_bibnos,
    retrieve_records,
)


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
    assert len(session.query(Resource).all()) == 8


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


def test_enhance_resource_nyp(brief_bib_dataset):
    session = brief_bib_dataset
    # enhance_resource(session, record, "nyp")
    # assert session.query().one() ==


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


def test_retrieve_records(init_dataset):
    session = init_dataset
    recs = retrieve_records(session, LibrarySystem, code="nyp")
    assert len(recs) == 1


@pytest.mark.parametrize(
    "lsid,bcid,expectation",
    [
        (1, 1, [12345678, 12345679]),
        (1, 2, [12345670, 12345671]),
        (2, 1, [22345678, 22345679]),
        (2, 2, [22345670, 22345671]),
    ],
)
def test_retrieve_bibnos(lsid, bcid, expectation, brief_bib_dataset):
    session = brief_bib_dataset
    assert retrieve_bibnos(session, lsid, bcid) == expectation
