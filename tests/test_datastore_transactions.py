# -*- coding: utf-8 -*-

"""
Tests datastore transactions
"""

from datetime import date

from nightshift.datastore import LibrarySystem, BibCategory, UpgradeSource, Resource
from nightshift.datastore_transactions import insert, insert_resource


def test_init_in_memory_db(init_dataset):
    session = init_dataset
    assert len(session.query(LibrarySystem).all()) == 2
    assert len(session.query(BibCategory).all()) == 2
    assert len(session.query(UpgradeSource).all()) == 2


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
