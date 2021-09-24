# -*- coding: utf-8 -*-

from nightshift.datastore import Library, Resource, ResourceCategory, SourceFile
from nightshift.datastore_transactions import insert_or_ignore, update_resource


def test_insert_or_ignore_new(test_session):
    rec = insert_or_ignore(test_session, Library, code="nyp")
    test_session.commit()
    assert type(rec) == Library
    assert rec.code == "nyp"
    assert rec.nid == 1


def test_insert_or_ingore_dup(test_session):
    rec1 = insert_or_ignore(test_session, Library, code="bpl")
    test_session.commit()
    assert rec1.nid == 1

    rec2 = insert_or_ignore(test_session, Library, code="bpl")
    test_session.commit()
    assert rec2 is None


def test_update_resource(test_session):
    lib_rec = insert_or_ignore(test_session, Library, code="nyp")
    cat_rec = insert_or_ignore(test_session, ResourceCategory, code="ebook")
    test_session.commit()
    src_rec = insert_or_ignore(
        test_session, SourceFile, libraryId=lib_rec.nid, handle="foo"
    )
    test_session.commit()

    sierraId = 22222222
    rec1 = insert_or_ignore(
        test_session,
        Resource,
        sierraId=sierraId,
        libraryId=lib_rec.nid,
        resourceCategoryId=cat_rec.nid,
        title="TEST TITLE",
        sourceId=src_rec.nid,
        status="open",
    )
    test_session.commit()
    assert rec1.status == "open"
    rec2 = update_resource(
        test_session, sierraId=sierraId, libraryId=lib_rec.nid, status="expired"
    )
    assert rec2.nid == rec1.nid
    assert rec1.status == "expired"


def test_update_resource_instance_does_not_exist(test_session):
    rec = update_resource(
        test_session, sierraId=22222222, libraryId=1, status="expired"
    )
    assert rec is None
