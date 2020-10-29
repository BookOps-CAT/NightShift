# -*- coding: utf-8 -*-

"""
Tests datastore transactions
"""


from nightshift.datastore import LibrarySystem, BibCategory
from nightshift.datastore_transactions import insert


def test_init_in_memory_db(init_dataset):
    session = init_dataset
    assert len(session.query(LibrarySystem).all()) == 2
    assert len(session.query(BibCategory).all()) == 2


def test_insert(init_dataset):
    session = init_dataset
    record = insert(
        session, BibCategory, bcid=3, code="test", description="test category"
    )
    assert record.bcid == 3
    assert record.code == "test"
    assert record.description == "test category"
