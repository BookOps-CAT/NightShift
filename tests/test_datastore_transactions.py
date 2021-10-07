# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from nightshift.datastore import (
    Base,
    Library,
    Resource,
    ResourceCategory,
    SourceFile,
    WorldcatQuery,
)
from nightshift.datastore_transactions import (
    init_db,
    insert_or_ignore,
    retrieve_new_resources,
    update_resource,
)


def test_init_db(mock_db_env, test_connection):

    # initiate database
    with does_not_raise():
        init_db()

    # verify tables created and populated
    engine = create_engine(test_connection)
    insp = inspect(engine)
    assert sorted(insp.get_table_names()) == sorted(
        [
            "library",
            "output_file",
            "source_file",
            "resource",
            "resource_category",
            "worldcat_query",
        ]
    )

    Session = sessionmaker(bind=engine)
    session = Session()

    res = session.query(Library).all()
    session.commit()
    assert len(res) == 2

    res = session.query(ResourceCategory).all()
    assert len(res) == 11
    session.commit()
    session.close()

    # tear db down
    Base.metadata.drop_all(engine)


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


def test_retrieve_new_resources(test_session, test_data):
    test_session.add(SourceFile(libraryId=1, handle="foo.mrc"))
    test_session.add(SourceFile(libraryId=2, handle="bar.mrc"))
    test_session.commit()

    # BPL resources
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=2,
            resourceCategoryId=2,
            title="TEST TITLE 5",
            sourceId=2,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=2,
            sierraId=11111112,
            libraryId=2,
            resourceCategoryId=1,
            title="TEST TITLE 6",
            sourceId=2,
            status="open",
            deleted=False,
            queries=[WorldcatQuery(resourceId=2, libraryId=2, match=False)],
        )
    )
    test_session.add(
        Resource(
            nid=3,
            sierraId=11111113,
            libraryId=2,
            resourceCategoryId=1,
            title="TEST TITLE 7",
            sourceId=2,
            status="open",
            deleted=False,
        )
    )

    # NYP resources
    test_session.add(
        Resource(
            nid=4,
            sierraId=22222222,
            libraryId=1,
            resourceCategoryId=1,
            title="TEST TITLE 1",
            sourceId=1,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=5,
            sierraId=22222223,
            libraryId=1,
            resourceCategoryId=2,
            title="TEST TITLE 2",
            sourceId=1,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=6,
            sierraId=22222224,
            libraryId=1,
            resourceCategoryId=1,
            title="TEST TITLE 3",
            sourceId=1,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=7,
            sierraId=22222225,
            libraryId=1,
            resourceCategoryId=1,
            title="TEST TITLE 4",
            sourceId=1,
            status="open",
            deleted=False,
            queries=[WorldcatQuery(resourceId=7, libraryId=1, match=False)],
        )
    )

    test_session.commit()

    res = retrieve_new_resources(test_session)
    # correct rows are retrieved
    assert len(res) == 5

    # in correct grouping/order
    assert res[0].nid == 4
    assert res[1].nid == 6
    assert res[2].nid == 5
    assert res[3].nid == 3
    assert res[4].nid == 1


def test_update_resource(test_session):
    lib_rec = insert_or_ignore(test_session, Library, code="nyp")
    cat_rec = insert_or_ignore(test_session, ResourceCategory, name="ebook")
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
