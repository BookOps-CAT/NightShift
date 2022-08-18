# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError


from nightshift.constants import RESOURCE_CATEGORIES

from nightshift.datastore import (
    Base,
    Event,
    Library,
    Resource,
    ResourceCategory,
    RottenApple,
    RottenAppleResource,
    SourceFile,
    WorldcatQuery,
)
from nightshift.datastore_transactions import (
    ResCatById,
    ResCatByName,
    add_event,
    add_output_file,
    add_resource,
    add_source_file,
    delete_resources,
    init_db,
    insert_or_ignore,
    library_by_id,
    parse_query_days,
    resource_category_by_name,
    retrieve_expired_resources,
    retrieve_open_matched_resources_with_full_bib_obtained,
    retrieve_open_matched_resources_without_full_bib,
    retrieve_new_resources,
    retrieve_open_older_resources,
    retrieve_processed_files,
    retrieve_rotten_apples,
    set_resources_to_expired,
    update_resource,
)


def test_init_db(mock_db_env, test_connection):
    # make sure drop any tables left over after any previous
    # failed test
    engine = create_engine(test_connection)
    Base.metadata.drop_all(engine)

    # initiate database
    with does_not_raise():
        init_db()

    # verify tables created and populated
    insp = inspect(engine)
    assert sorted(insp.get_table_names()) == sorted(
        [
            "event",
            "library",
            "output_file",
            "source_file",
            "resource",
            "rotten_apple",
            "rotten_apple_resource",
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


def test_init_db_invalid_data(mock_db_env, test_connection, mock_init_libraries):
    # drop in case any tables left after failed test
    engine = create_engine(test_connection)
    Base.metadata.drop_all(engine)

    with pytest.raises(AssertionError) as exc:
        init_db()

    assert "Invalid number of initial libraries." in str(exc.value)

    # clean-up
    Base.metadata.drop_all(engine)


def test_add_event(test_session, test_data_rich):
    resource = test_session.query(Resource).where(Resource.nid == 1).one()
    event = add_event(test_session, resource, status="expired")
    test_session.commit()

    assert event.nid == 1
    assert event.libraryId == resource.libraryId
    assert event.sierraId == resource.sierraId
    assert event.bibDate == resource.bibDate
    assert event.resourceCategoryId == resource.resourceCategoryId
    assert event.status == "expired"


def test_add_event_always_insert(test_session, test_data_rich):
    resource = test_session.query(Resource).where(Resource.nid == 1).one()
    event1 = add_event(test_session, resource, status="expired")
    event2 = add_event(test_session, resource, status="expired")
    test_session.commit()

    results = test_session.query(Event).all()

    assert len(results) == 2


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


def test_delete_resources(test_session, test_data_rich):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="expired",
            bibDate=datetime.utcnow() - timedelta(days=last_day + 91),
            queries=[WorldcatQuery(match=False)],
        )
    )
    test_session.commit()

    result = delete_resources(test_session, nid, last_day + 90)
    assert result == 1

    control_resource = test_session.query(Resource).filter_by(sierraId=11111111).one()
    resource_deleted = (
        test_session.query(Resource).filter_by(sierraId=22222222).one_or_none()
    )
    assert control_resource.status == "bot_enhanced"
    assert resource_deleted is None

    # check related table queries
    # only query related to control resource
    result = test_session.query(WorldcatQuery).all()
    assert len(result) == 1
    assert result[0].resourceId == 1


def test_delete_resources_too_early(test_session, test_data_rich):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="expired",
            bibDate=datetime.utcnow() - timedelta(days=last_day + 89),
            queries=[WorldcatQuery(match=False)],
        )
    )
    test_session.commit()

    result = delete_resources(test_session, nid, last_day + 90)
    assert result == 0

    control_resource = test_session.query(Resource).filter_by(sierraId=11111111).one()
    resource_deleted = (
        test_session.query(Resource).filter_by(sierraId=22222222).one_or_none()
    )
    assert control_resource.status == "bot_enhanced"
    assert resource_deleted is not None

    # check related table queries
    # only query related to control resource
    result = test_session.query(WorldcatQuery).all()
    assert len(result) == 2


def test_insert_or_ignore_new(test_session):
    rec = insert_or_ignore(test_session, Library, code="NYP")
    test_session.commit()
    assert type(rec) == Library
    assert rec.code == "NYP"
    assert rec.nid == 1


def test_insert_or_ignore_dup(test_session):
    rec1 = insert_or_ignore(test_session, Library, code="BPL")
    test_session.commit()
    assert rec1.nid == 1

    rec2 = insert_or_ignore(test_session, Library, code="BPL")
    test_session.commit()
    assert rec2.nid == 1


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


def test_library_by_id(test_session, test_data_core):
    assert library_by_id(test_session) == {1: "NYP", 2: "BPL"}


@pytest.mark.parametrize(
    "arg,expectation",
    [("15-30", [(15, 30)]), ("1-5,5-6,7-8", [(1, 5), (5, 6), (7, 8)])],
)
def test_parse_query_days(arg, expectation):
    assert parse_query_days(arg) == expectation


@pytest.mark.parametrize(
    "nid, name, formatBpl, formatNyp, srcTags, dstTags, days",
    [
        pytest.param(
            1,
            "ebook",
            "x",
            "z",
            ["020", "037", "856"],
            ["020", "029", "037", "090", "263", "856", "910", "938"],
            [(30, 90), (90, 180)],
            id="ebook",
        ),
        pytest.param(
            2,
            "eaudio",
            "z",
            "n",
            ["020", "037", "856"],
            ["020", "029", "037", "090", "263", "856", "910", "938"],
            [(30, 90), (90, 180)],
            id="eaudio",
        ),
        pytest.param(
            3,
            "evideo",
            "v",
            "3",
            ["020", "037", "856"],
            ["020", "029", "037", "090", "263", "856", "910", "938"],
            [(30, 90)],
            id="evideo",
        ),
        pytest.param(
            4,
            "print_eng_adult_fic",
            "a",
            "a",
            ["910"],
            ["029", "090", "263", "936", "938"],
            [(15, 30), (30, 45)],
            id="eng adult fic",
        ),
    ],
)
def test_resource_category_by_name(
    test_session,
    test_data_core,
    nid,
    name,
    formatBpl,
    formatNyp,
    srcTags,
    dstTags,
    days,
):
    rs = resource_category_by_name(test_session)
    assert isinstance(rs, dict)

    assert isinstance(rs[name], ResCatByName)
    assert rs[name].nid == nid
    assert rs[name].sierraBibFormatBpl == formatBpl
    assert rs[name].sierraBibFormatNyp == formatNyp
    assert rs[name].srcTags2Keep == srcTags
    assert rs[name].dstTags2Delete == dstTags
    assert rs[name].queryDays == days


def test_retrieve_expired_resources(test_session, test_data_rich):
    # single test record serves as control data
    # it should not be caught by this query

    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=nid,
            status="open",
            bibDate=datetime.utcnow() - timedelta(days=last_day + 1),
        )
    )
    test_session.commit()

    resources = retrieve_expired_resources(test_session, nid, last_day)

    assert len(resources) == 1
    assert resources[0].nid == 2


def test_retrieve_expired_resources_too_early(test_session, test_data_rich):
    # single test record serves as control data
    # it should not be caught by this query

    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=nid,
            status="open",
            bibDate=datetime.utcnow() - timedelta(days=last_day - 1),
        )
    )
    test_session.commit()

    resources = retrieve_expired_resources(test_session, nid, last_day)

    assert len(resources) == 0


@pytest.mark.parametrize(
    "library_id, resource_cat_id, status,full_bib,expectation",
    [
        pytest.param(1, 1, "expired", b"<foo>spam</foo>", [], id="wrong status"),
        pytest.param(1, 1, "open", None, [], id="missing full bib"),
        pytest.param(1, 1, "open", b"<foo>spam</foo>", [2], id="found 1 match"),
        pytest.param(2, 1, "open", b"<foo>spam</foo>", [1, 2], id="found 2 matches"),
    ],
)
def test_retrieve_open_matched_resources_with_full_bib_obtained(
    test_session,
    test_data_core,
    library_id,
    resource_cat_id,
    status,
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
            fullBib=full_bib,
        )
    )
    test_session.commit()
    res = retrieve_open_matched_resources_with_full_bib_obtained(
        test_session, library_id, resource_cat_id
    )
    assert [r.nid for r in res] == expectation


@pytest.mark.parametrize(
    "min_age,max_age,query_age,expectation",
    [
        pytest.param(30, 90, 25, [1], id="query needed"),
        pytest.param(30, 90, 31, [], id="queried already"),
        pytest.param(30, 70, 1, [], id="resource too old"),
    ],
)
def test_retrieve_open_older_resources(
    test_session, test_data_core, min_age, max_age, query_age, expectation
):
    bib_date = datetime.utcnow() - timedelta(days=80)

    test_session.add(
        Resource(
            nid=1,
            sierraId=22222222,
            libraryId=1,
            bibDate=bib_date,
            resourceCategoryId=1,
            title="TEST TITLE",
            sourceId=1,
            oclcMatchNumber=None,
            status="open",
            queries=[
                WorldcatQuery(
                    nid=1,
                    resourceId=1,
                    match=False,
                    timestamp=bib_date + timedelta(days=query_age),
                ),
            ],
        )
    )
    test_session.commit()

    res = retrieve_open_older_resources(test_session, 1, 1, min_age, max_age)
    assert [r.nid for r in res] == expectation


def test_retrieve_open_older_resources_no_queries_performed(
    test_session, test_data_core
):
    test_session.add(
        Resource(
            nid=1,
            sierraId=22222222,
            libraryId=1,
            bibDate=datetime.utcnow() - timedelta(days=80),
            resourceCategoryId=1,
            title="TEST TITLE",
            sourceId=1,
            oclcMatchNumber=None,
            status="open",
        )
    )
    test_session.commit()

    res = retrieve_open_older_resources(test_session, 1, 1, 30, 90)
    assert res == []


@pytest.mark.parametrize(
    "query_age,expectation",
    [
        pytest.param(91, [], id="queried already"),
        pytest.param(80, [1], id="query needed"),
    ],
)
def test_retrieve_open_older_resources_multiple_queries_performed(
    test_session, test_data_core, query_age, expectation
):
    bib_date = datetime.utcnow() - timedelta(days=100)

    test_session.add(
        Resource(
            nid=1,
            sierraId=22222222,
            libraryId=1,
            bibDate=bib_date,
            resourceCategoryId=1,
            title="TEST TITLE",
            sourceId=1,
            oclcMatchNumber=None,
            status="open",
            queries=[
                WorldcatQuery(
                    nid=1,
                    resourceId=1,
                    match=False,
                    timestamp=bib_date + timedelta(days=1),
                ),
                WorldcatQuery(
                    nid=2,
                    resourceId=1,
                    match=False,
                    timestamp=bib_date + timedelta(days=30),
                ),
                WorldcatQuery(
                    nid=3,
                    resourceId=1,
                    match=True,
                    timestamp=bib_date + timedelta(days=query_age),
                ),
            ],
        )
    )
    test_session.commit()

    res = retrieve_open_older_resources(test_session, 1, 1, 90, 180)
    assert [r.nid for r in res] == expectation


@pytest.mark.parametrize(
    "lib_id,res_cat_id,status,oclc_match_no,max_age,expectation",
    [
        pytest.param(1, 1, "open", None, 50, [1], id="match"),
        pytest.param(2, 1, "open", None, 50, [], id="wrong library"),
        pytest.param(1, 2, "open", None, 50, [], id="wrong res category"),
        pytest.param(1, 1, "staff_deleted", None, 50, [], id="wrong status"),
        pytest.param(1, 1, "open", "1234", 50, [], id="wrong oclc # value"),
        pytest.param(1, 1, "open", None, 120, [], id="wrong too old bib"),
    ],
)
def test_retrieve_open_older_resources_invalid_resources(
    test_session,
    test_data_core,
    lib_id,
    res_cat_id,
    status,
    oclc_match_no,
    max_age,
    expectation,
):
    bib_date = datetime.utcnow() - timedelta(days=max_age)

    test_session.add(
        Resource(
            nid=1,
            sierraId=22222222,
            libraryId=lib_id,
            bibDate=bib_date,
            resourceCategoryId=res_cat_id,
            title="TEST TITLE",
            sourceId=1,
            oclcMatchNumber=oclc_match_no,
            status=status,
            queries=[
                WorldcatQuery(
                    nid=1,
                    resourceId=1,
                    match=False,
                    timestamp=bib_date + timedelta(days=1),
                ),
                WorldcatQuery(
                    nid=2,
                    resourceId=1,
                    match=False,
                    timestamp=bib_date + timedelta(days=20),
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
            status="bot_enhanced",
            oclcMatchNumber="123",
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


def test_retrieve_rotten_apples(test_session, test_data_core):
    test_session.add(RottenApple(code="FOO"))
    test_session.commit()
    test_session.add(RottenAppleResource(resourceCategoryId=1, rottenAppleId=3))
    test_session.commit()
    orgs = retrieve_rotten_apples(test_session)
    assert orgs == {1: ["UKAHL", "UAH", "FOO"], 2: ["UKAHL"], 3: ["UKAHL"]}


def test_set_resources_to_expired(test_session, test_data_rich, stub_resource):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.utcnow() - timedelta(days=last_day + 1),
        )
    )
    test_session.commit()

    result = set_resources_to_expired(test_session, nid, last_day)
    assert result == 1

    resource_not_changed = (
        test_session.query(Resource).filter_by(sierraId=11111111).one()
    )
    resource_set_to_expired = (
        test_session.query(Resource).filter_by(sierraId=22222222).one()
    )
    assert resource_not_changed.status == "bot_enhanced"
    assert resource_set_to_expired.status == "expired"


def test_set_resources_to_expired_too_early(
    test_session, test_data_rich, stub_resource
):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    test_session.add(
        Resource(
            sierraId=22222222,
            libraryId=1,
            sourceId=1,
            resourceCategoryId=1,
            status="open",
            bibDate=datetime.utcnow() - timedelta(days=last_day - 1),
        )
    )
    test_session.commit()

    result = set_resources_to_expired(test_session, nid, last_day)
    assert result == 0

    resource_not_changed = (
        test_session.query(Resource).filter_by(sierraId=11111111).one()
    )
    resource_set_to_expired = (
        test_session.query(Resource).filter_by(sierraId=22222222).one()
    )
    assert resource_not_changed.status == "bot_enhanced"
    assert resource_set_to_expired.status == "open"


def test_update_resource(test_session):
    lib_rec = insert_or_ignore(test_session, Library, code="NYP")
    cat_rec = insert_or_ignore(
        test_session,
        ResourceCategory,
        name="ebook",
        sierraBibFormatBpl="x",
        sierraBibFormatNyp="z",
        queryDays="1-5",
    )
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
    with pytest.raises(NoResultFound):
        update_resource(test_session, sierraId=22222222, libraryId=1, status="expired")
