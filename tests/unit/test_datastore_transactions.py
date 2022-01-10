# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from contextlib import nullcontext as does_not_raise
from copy import deepcopy

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError


from nightshift.constants import RESOURCE_CATEGORIES

from nightshift.datastore import (
    Base,
    Library,
    Resource,
    ResourceCategory,
    SourceFile,
    WorldcatQuery,
)
from nightshift.datastore_transactions import (
    add_output_file,
    add_resource,
    add_source_file,
    init_db,
    insert_or_ignore,
    retrieve_open_matched_resources_with_full_bib_obtained,
    retrieve_open_matched_resources_without_full_bib,
    retrieve_new_resources,
    retrieve_open_older_resources,
    retrieve_processed_files,
    set_resources_to_expired,
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


def test_add_output_file(test_session, test_data_core):
    result = add_output_file(test_session, 1, "bar.mrc")
    assert result.nid == 1
    assert result.handle == "bar.mrc"
    assert result.libraryId == 1


@pytest.mark.parametrize(
    "sierraId,libraryId,expectation", [(22222222, 1, 2), (11111111, 2, 2)]
)
def test_add_resource_success(
    sierraId, libraryId, expectation, test_session, test_data_core
):
    bib_date = datetime.now().date()
    test_session.add(
        Resource(
            sierraId=11111111,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            bibDate=bib_date,
        )
    )
    test_session.commit()
    result = add_resource(
        test_session,
        Resource(
            sierraId=sierraId,
            libraryId=libraryId,
            sourceId=1,
            resourceCategoryId=1,
            bibDate=bib_date,
        ),
    )
    assert isinstance(result, Resource)
    assert result.nid == expectation


def test_add_resource_unique_constraint_violation(test_session, test_data_core):
    bib_date = datetime.now().date()
    test_session.add(
        Resource(
            sierraId=11111111,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            bibDate=bib_date,
        )
    )
    test_session.commit()

    with does_not_raise():
        result = add_resource(
            test_session,
            Resource(
                sierraId=11111111,
                libraryId=1,
                sourceId=2,
                resourceCategoryId=1,
                bibDate=bib_date,
            ),
        )
        test_session.commit()
        assert result is None


def test_add_source_file(test_session, test_data_core):
    rec = add_source_file(test_session, 1, "bar.mrc")
    assert rec.nid == 3
    assert rec.libraryId == 1
    assert rec.handle == "bar.mrc"


def test_insert_or_ignore_new(test_session):
    rec = insert_or_ignore(test_session, Library, code="NYP")
    test_session.commit()
    assert type(rec) == Library
    assert rec.code == "NYP"
    assert rec.nid == 1


def test_insert_or_ingore_dup(test_session):
    rec1 = insert_or_ignore(test_session, Library, code="BPL")
    test_session.commit()
    assert rec1.nid == 1

    rec2 = insert_or_ignore(test_session, Library, code="BPL")
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
    "library_id, resource_cat_id, status,deleted,full_bib,expectation",
    [
        pytest.param(1, 1, "expired", False, b"<foo>spam</foo>", [], id="wrong status"),
        pytest.param(1, 1, "open", True, b"<foo>spam</foo", [], id="deleted resource"),
        pytest.param(1, 1, "open", False, None, [], id="missing full bib"),
        pytest.param(1, 1, "open", False, b"<foo>spam</foo>", [2], id="found 1 match"),
        pytest.param(
            2, 1, "open", False, b"<foo>spam</foo>", [1, 2], id="found 2 matches"
        ),
    ],
)
def test_retrieve_open_matched_resources_with_full_bib_obtained(
    test_session,
    test_data_core,
    library_id,
    resource_cat_id,
    status,
    deleted,
    full_bib,
    expectation,
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
            status="open",
            deleted=False,
            fullBib=b"<foo>spam</foo>",
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
    res = retrieve_open_matched_resources_with_full_bib_obtained(
        test_session, library_id, resource_cat_id
    )
    assert [r.nid for r in res] == expectation


@pytest.mark.parametrize(
    "bib_date,status,deleted,oclc_number,match,days_since_last_query,expectation",
    [
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            False,
            None,
            False,
            31,
            [1],
            id="query needed",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            False,
            None,
            False,
            15,
            [],
            id="already queried",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            False,
            None,
            False,
            100,
            [],
            id="last query > maxAge",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=100),
            "open",
            False,
            None,
            False,
            31,
            [],
            id="too old resource",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "expired",
            False,
            None,
            False,
            31,
            [],
            id="expired status",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=80),
            "open",
            True,
            None,
            False,
            31,
            [],
            id="deleted resource",
        ),
        pytest.param(
            datetime.utcnow() - timedelta(days=100),
            "open",
            False,
            "123",
            True,
            31,
            [],
            id="matched resource",
        ),
    ],
)
def test_retrieve_open_older_resources(
    test_session,
    test_data_core,
    bib_date,
    status,
    deleted,
    oclc_number,
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
            oclcMatchNumber=oclc_number,
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

    res = retrieve_open_older_resources(test_session, 1, 1, 30, 90)
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


def test_retrieve_open_matched_resources_without_full_bib(test_session, test_data_core):
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
            status="upgraded_bot",
            oclcMatchNumber="123",
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
            oclcMatchNumber=None,
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
            status="open",
            oclcMatchNumber="123",
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
            status="open",
            oclcMatchNumber="124",
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
            status="open",
            oclcMatchNumber="125",
            deleted=False,
        )
    )
    # exclude (full bib already obtained)
    test_session.add(
        Resource(
            nid=6,
            sierraId=22222226,
            libraryId=1,
            resourceCategoryId=1,
            bibDate=some_date,
            title="TEST TITLE 5",
            sourceId=1,
            status="open",
            oclcMatchNumber="126",
            fullBib=b"<foo>spam</foo>",
            deleted=False,
        )
    )
    test_session.commit()

    # BPL
    res1 = retrieve_open_matched_resources_without_full_bib(test_session, libraryId=2)
    assert len(res1) == 0

    # NYPL
    res2 = retrieve_open_matched_resources_without_full_bib(test_session, libraryId=1)
    assert len(res2) == 3
    assert res2[0].nid == 4
    assert res2[1].nid == 5
    assert res2[2].nid == 3


@pytest.mark.parametrize(
    "libraryId,expectation", [(1, ["foo1.mrc"]), (2, ["foo2.mrc"])]
)
def test_retrieve_processed_files(test_session, test_data_rich, libraryId, expectation):
    results = retrieve_processed_files(test_session, libraryId)
    assert results == expectation


def test_set_resources_to_expired(test_session, test_data_rich, stub_resource):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    age = RESOURCE_CATEGORIES["ebook"]["query_days"][-1][1]

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.utcnow() - timedelta(days=age + 1),
        )
    )
    test_session.commit()

    result = set_resources_to_expired(test_session, nid, age)
    assert result == 1

    resource_not_changed = (
        test_session.query(Resource).filter_by(sierraId=11111111).one()
    )
    resource_set_to_expired = (
        test_session.query(Resource).filter_by(sierraId=22222222).one()
    )
    assert resource_not_changed.status == "upgraded_bot"
    assert resource_set_to_expired.status == "expired"


def test_set_resources_to_expired_too_early(
    test_session, test_data_rich, stub_resource
):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    age = RESOURCE_CATEGORIES["ebook"]["query_days"][-1][1]

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.utcnow() - timedelta(days=age - 1),
        )
    )
    test_session.commit()

    result = set_resources_to_expired(test_session, nid, age)
    assert result == 0

    resource_not_changed = (
        test_session.query(Resource).filter_by(sierraId=11111111).one()
    )
    resource_set_to_expired = (
        test_session.query(Resource).filter_by(sierraId=22222222).one()
    )
    assert resource_not_changed.status == "upgraded_bot"
    assert resource_set_to_expired.status == "open"


def test_update_resource(test_session):
    lib_rec = insert_or_ignore(test_session, Library, code="NYP")
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
