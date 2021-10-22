# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

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
    retrieve_full_bib_resources,
    retrieve_matched_resources,
    retrieve_new_resources,
    retrieve_older_open_resources,
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


def test_insert_or_ignore_resubmitted_changed_record_exception(
    test_session, test_data_core
):
    # not possible to add the same record but exception raised
    # should this be trapped somehow and instead return None?
    insert_or_ignore(
        test_session,
        Resource,
        sierraId=22222222,
        libraryId=1,
        resourceCategoryId=1,
        sourceId=1,
        bibDate=datetime.utcnow().date(),
        title="TEST TITLE 1",
    )
    test_session.commit()
    insert_or_ignore(
        test_session,
        Resource,
        sierraId=22222222,
        libraryId=1,
        resourceCategoryId=1,
        sourceId=2,
        bibDate=datetime.utcnow().date(),
        title="REV TEST TITLE 1",
    )
    with pytest.raises(IntegrityError):
        test_session.commit()


@pytest.mark.parametrize(
    "library_id,status,deleted,full_bib,expectation",
    [
        pytest.param(1, "open", False, "<foo>spam</foo>", [], id="wrong status"),
        pytest.param(1, "matched", True, "<foo>spam</foo", [], id="deleted resource"),
        pytest.param(1, "matched", False, None, [], id="missing full bib"),
        pytest.param(1, "matched", False, "<foo>spam</foo>", [2], id="found 1 match"),
        pytest.param(
            2, "matched", False, "<foo>spam</foo>", [1, 2], id="found 2 matches"
        ),
    ],
)
def test_retrieve_full_bib_resources(
    test_session, test_data_core, library_id, status, deleted, full_bib, expectation
):
    bib_date = datetime.utcnow().date()
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=2,
            sourceId=1,
            resourceCategoryId=1,
            bibDate=bib_date,
            title="TEST TITLE 1",
            status="matched",
            deleted=False,
            fullBib="<foo>spam</foo>",
        )
    )
    test_session.add(
        Resource(
            nid=2,
            sierraId=22222222,
            libraryId=library_id,
            sourceId=1,
            resourceCategoryId=1,
            bibDate=bib_date,
            title="TEST TITLE 2",
            status=status,
            deleted=deleted,
            fullBib=full_bib,
        )
    )
    test_session.commit()
    res = retrieve_full_bib_resources(test_session, library_id)
    assert [r.nid for r in res] == expectation


@pytest.mark.parametrize(
    "bib_date,status,deleted,match,days_since_last_query,expectation",
    [
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            False,
            False,
            31,
            [1],
            id="query needed",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            False,
            False,
            15,
            [],
            id="already queried",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            False,
            False,
            100,
            [],
            id="last query > maxAge",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=100),
            "open",
            False,
            False,
            31,
            [],
            id="too old resource",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "expired",
            False,
            False,
            31,
            [],
            id="expired status",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            True,
            False,
            31,
            [],
            id="deleted resource",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=100),
            "open",
            False,
            True,
            31,
            [],
            id="matched resource",
        ),
    ],
)
def test_retrieve_older_open_resources(
    test_session,
    test_data_core,
    bib_date,
    status,
    deleted,
    match,
    days_since_last_query,
    expectation,
):

    test_session.add(
        Resource(
            nid=1,
            sierraId=22222222,
            libraryId=1,
            bibDate=bib_date,
            resourceCategoryId=1,
            title="TEST TITLE",
            sourceId=1,
            status=status,
            deleted=deleted,
            queries=[
                WorldcatQuery(
                    nid=1,
                    resourceId=1,
                    match=False,
                    timestamp=datetime.now() - timedelta(days=200),
                ),
                WorldcatQuery(
                    nid=2,
                    resourceId=1,
                    match=match,
                    timestamp=datetime.now() - timedelta(days=days_since_last_query),
                ),
            ],
        )
    )
    test_session.commit()

    res = retrieve_older_open_resources(test_session, 1, 30, 90)
    assert [r.nid for r in res] == expectation


def test_retrieve_new_resources(test_session, test_data_core):

    # BPL resources
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=2,
            resourceCategoryId=2,
            bibDate=datetime.utcnow().date(),
            title="TEST TITLE 5",
            sourceId=2,
            status="open",
            deleted=False,
        )
    )

    # NYP resources
    test_session.add(
        Resource(
            nid=2,
            sierraId=22222222,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=datetime.utcnow().date(),
            title="TEST TITLE 1",
            sourceId=1,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=3,
            sierraId=22222223,
            libraryId=1,
            resourceCategoryId=2,
            bibDate=datetime.utcnow().date(),
            title="TEST TITLE 2",
            sourceId=1,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=4,
            sierraId=22222224,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=datetime.utcnow().date(),
            title="TEST TITLE 3",
            sourceId=1,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=5,
            sierraId=22222225,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=datetime.utcnow().date(),
            title="TEST TITLE 4",
            sourceId=1,
            status="open",
            deleted=False,
            queries=[WorldcatQuery(resourceId=5, match=False)],
        )
    )

    test_session.commit()

    res = retrieve_new_resources(session=test_session, libraryId=1)
    # correct rows are retrieved
    assert len(res) == 3

    # in correct grouping/order
    assert res[0].nid == 2
    assert res[1].nid == 4
    assert res[2].nid == 3


def test_retrieve_matched_resources(test_session, test_data_core):
    some_date = (datetime.utcnow().date(),)
    # BPL resources
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=2,
            resourceCategoryId=2,
            bibDate=some_date,
            title="TEST TITLE 5",
            sourceId=2,
            status="open",
            deleted=False,
        )
    )

    # NYP resources
    test_session.add(
        Resource(
            nid=2,
            sierraId=22222222,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=some_date,
            title="TEST TITLE 1",
            sourceId=1,
            status="open",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=3,
            sierraId=22222223,
            libraryId=1,
            resourceCategoryId=2,
            bibDate=some_date,
            title="TEST TITLE 2",
            sourceId=1,
            status="matched",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=4,
            sierraId=22222224,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=some_date,
            title="TEST TITLE 3",
            sourceId=1,
            status="matched",
            deleted=False,
        )
    )
    test_session.add(
        Resource(
            nid=5,
            sierraId=22222225,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=some_date,
            title="TEST TITLE 4",
            sourceId=1,
            status="matched",
            deleted=False,
        )
    )
    test_session.commit()

    res1 = retrieve_matched_resources(test_session, libraryId=2)
    assert len(res1) == 0

    res2 = retrieve_matched_resources(test_session, libraryId=1)
    assert len(res2) == 3
    assert res2[0].nid == 4
    assert res2[1].nid == 5
    assert res2[2].nid == 3


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
        bibDate=datetime.utcnow().date(),
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
