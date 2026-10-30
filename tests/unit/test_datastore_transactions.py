from contextlib import nullcontext as does_not_raise
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.orm.exc import NoResultFound

from nightshift import constants
from nightshift.constants import RESOURCE_CATEGORIES
from nightshift.datastore import (
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
    retrieve_new_resources,
    retrieve_open_matched_resources_with_full_bib_obtained,
    retrieve_open_matched_resources_without_full_bib,
    retrieve_open_older_resources,
    retrieve_processed_files,
    retrieve_rotten_apples,
    set_resources_to_expired,
    update_resource,
)
from nightshift.ns_exceptions import DatabaseInitializationError


def test_init_db(db_provider):
    with does_not_raise():
        init_db(db_provider)


def test_init_db_invalid_library_count(db_provider, monkeypatch):
    monkeypatch.setattr(constants, "LIBRARIES", {"NYP": {"nid": 1}})
    with pytest.raises(DatabaseInitializationError) as exc:
        init_db(db_provider)

    assert "Invalid number of initial libraries." in str(exc.value)


@pytest.mark.parametrize(
    "libs, expected_error_msg",
    [
        (["NYP", "QPL"], "'BPL' code missing in 'Library' table."),
        (["BPL", "QPL"], "'NYP' code missing in 'Library' table."),
    ],
)
def test_init_db_missing_library(db_provider, libs, expected_error_msg, monkeypatch):
    monkeypatch.setattr(
        constants, "LIBRARIES", {libs[i]: {"nid": i + 1} for i in range(len(libs))}
    )
    with pytest.raises(DatabaseInitializationError) as exc:
        init_db(db_provider)

    assert expected_error_msg in str(exc.value)


def test_init_db_invalid_res_cat_count(db_provider, monkeypatch):
    res_cat = {k: v for k, v in constants.RESOURCE_CATEGORIES.items() if k != "ebook"}
    monkeypatch.setattr(constants, "RESOURCE_CATEGORIES", res_cat)
    monkeypatch.setattr(
        constants, "ROTTEN_APPLES", {"UKAHL": ["eaudio", "evideo"], "UAH": []}
    )
    with pytest.raises(DatabaseInitializationError) as exc:
        init_db(db_provider)

    assert "Invalid number of 'ResourceCategory' records." in str(exc.value)


@pytest.mark.parametrize(
    "res_cats, error_msg",
    [
        (["eaudio", "evideo"], "Missing 'ebook' category"),
        (["ebook", "evideo"], "Missing 'eaudio' category"),
        (["ebook", "eaudio"], "Missing 'evideo' category"),
    ],
)
def test_init_db_missing_res_cat(db_provider, mocker, res_cats, error_msg):
    mock_scalars = mocker.patch("sqlalchemy.orm.Session.scalars")

    def scalars_side_effect(stmt):
        mock_chain = mocker.MagicMock()
        target_entity = stmt.column_descriptions[0]["entity"]

        if target_entity == Library:
            mock_chain.all.return_value = [Library(code="NYP"), Library(code="BPL")]

        elif target_entity == ResourceCategory:
            mock_records = [ResourceCategory(name=name) for name in res_cats]
            while len(mock_records) < 11:
                mock_records.append(ResourceCategory(name=f"dummy_{len(mock_records)}"))
            mock_chain.all.return_value = mock_records

        return mock_chain

    mock_scalars.side_effect = scalars_side_effect

    with pytest.raises(DatabaseInitializationError) as exc:
        init_db(db_provider)

    assert error_msg in str(exc.value)


def test_init_db_missing_tables(db_provider, mocker):
    mock_inspect = mocker.MagicMock()
    mock_inspect.get_table_names.return_value = []
    mocker.patch("nightshift.datastore_transactions.inspect")
    with pytest.raises(DatabaseInitializationError) as exc:
        init_db(db_provider)
    assert "Database is missing required tables." in str(exc.value)


def test_add_event(db_provider, test_data_rich):
    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).where(Resource.nid == 1)).one()
        event = add_event(session, resource, status="expired")
        session.flush()

        assert event.nid == 1
        assert event.libraryId == resource.libraryId
        assert event.sierraId == resource.sierraId
        assert event.bibDate == resource.bibDate
        assert event.resourceCategoryId == resource.resourceCategoryId
        assert event.status == "expired"


def test_add_event_always_insert(db_provider, test_data_rich):
    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).where(Resource.nid == 1)).one()
        add_event(session, resource, status="expired")
        add_event(session, resource, status="expired")

    with db_provider.session_scope() as session:
        results = list(session.scalars(select(Event)).all())
        assert len(results) == 2


def test_add_output_file(db_provider, test_data_core):
    with db_provider.session_scope() as session:
        result = add_output_file(session, 1, "bar.mrc")
        assert result.nid == 1
        assert result.handle == "bar.mrc"
        assert result.libraryId == 1


@pytest.mark.parametrize(
    "sierraId,libraryId,expectation", [(22222222, 1, 2), (11111111, 2, 2)]
)
def test_add_resource_success(
    sierraId, libraryId, expectation, db_provider, test_data_core
):
    bib_date = datetime.now().date()
    with db_provider.session_scope() as session:
        session.add(
            Resource(
                sierraId=11111111,
                libraryId=1,
                sourceId=1,
                resourceCategoryId=1,
                bibDate=bib_date,
            )
        )

    with db_provider.session_scope() as session:
        result = add_resource(
            session,
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


def test_add_resource_unique_constraint_violation(db_provider, test_data_core):
    bib_date = datetime.now().date()
    with db_provider.session_scope() as session:
        session.add(
            Resource(
                sierraId=11111111,
                libraryId=1,
                sourceId=1,
                resourceCategoryId=1,
                bibDate=bib_date,
            )
        )

    with db_provider.session_scope() as session:
        result = add_resource(
            session,
            Resource(
                sierraId=11111111,
                libraryId=1,
                sourceId=2,
                resourceCategoryId=1,
                bibDate=bib_date,
            ),
        )
        assert result is None


def test_add_source_file(db_provider, test_data_core):
    with db_provider.session_scope() as session:
        rec = add_source_file(session, 1, "bar.mrc")
        assert rec.nid == 3
        assert rec.libraryId == 1
        assert rec.handle == "bar.mrc"


@pytest.mark.parametrize(
    "day_offset, expected_deleted, should_exist, expected_query_count",
    [(1, 1, False, 1), (-1, 0, True, 2)],
    ids=["eligible_for_deletion", "too_early_for_deletion"],
)
def test_delete_resources(
    db_provider,
    test_data_rich,
    day_offset,
    expected_deleted,
    should_exist,
    expected_query_count,
):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    with db_provider.session_scope() as session:
        session.add(
            Resource(
                sierraId=22222222,
                libraryId=1,
                sourceId=1,
                resourceCategoryId=1,
                status="expired",
                bibDate=datetime.now(timezone.utc)
                - timedelta(days=last_day + 90 + day_offset),
                queries=[WorldcatQuery(match=False)],
            )
        )

    with db_provider.session_scope() as session:
        result = delete_resources(session, nid, last_day + 90)
        assert result == expected_deleted

    with db_provider.session_scope() as session:
        control_resource = session.scalars(
            select(Resource).filter_by(sierraId=11111111)
        ).one()
        resource_tested = session.scalars(
            select(Resource).filter_by(sierraId=22222222)
        ).one_or_none()
        assert control_resource.status == "bot_enhanced"
        assert (resource_tested is not None) == should_exist

        queries = list(session.scalars(select(WorldcatQuery)).all())
        assert len(queries) == expected_query_count
        assert queries[0].resourceId == 1


def test_insert_or_ignore_new(db_provider):
    with db_provider.session_scope() as session:
        rec = insert_or_ignore(session, Library, code="NYP")
        assert isinstance(rec, Library)
        assert rec.code == "NYP"
        assert rec.nid == 1


def test_insert_or_ignore_dup(db_provider):
    with db_provider.session_scope() as session:
        rec1 = insert_or_ignore(session, Library, code="BPL")
        assert rec1.nid == 1

    with db_provider.session_scope() as session:
        rec2 = insert_or_ignore(session, Library, code="BPL")
        assert rec2.nid == 1


def test_library_by_id(db_provider, test_data_core):
    with db_provider.session_scope() as session:
        assert library_by_id(session) == {1: "NYP", 2: "BPL"}


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
    db_provider, test_data_core, nid, name, formatBpl, formatNyp, srcTags, dstTags, days
):
    with db_provider.session_scope() as session:
        rs = resource_category_by_name(session)
        assert isinstance(rs, dict)
        assert isinstance(rs[name], ResCatByName)
        assert rs[name].nid == nid
        assert rs[name].sierraBibFormatBpl == formatBpl
        assert rs[name].sierraBibFormatNyp == formatNyp
        assert rs[name].srcTags2Keep == srcTags
        assert rs[name].dstTags2Delete == dstTags
        assert rs[name].queryDays == days


@pytest.mark.parametrize(
    "day_offset, expected_count",
    [(1, 1), (-1, 0)],
    ids=["past_expiration", "before_expiration"],
)
def test_retrieve_expired_resources(
    db_provider, test_data_rich, day_offset, expected_count
):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    with db_provider.session_scope() as session:
        session.add(
            Resource(
                sierraId=22222222,
                libraryId=1,
                sourceId=1,
                resourceCategoryId=nid,
                status="open",
                bibDate=datetime.now(timezone.utc)
                - timedelta(days=last_day + day_offset),
            )
        )

    with db_provider.session_scope() as session:
        resources = retrieve_expired_resources(session, nid, last_day)
        assert len(resources) == expected_count


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
    db_provider,
    test_data_core,
    library_id,
    resource_cat_id,
    status,
    full_bib,
    expectation,
):
    bib_date = datetime.now(timezone.utc).date()
    with db_provider.session_scope() as session:
        session.add(
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
        session.add(
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

    with db_provider.session_scope() as session:
        res = retrieve_open_matched_resources_with_full_bib_obtained(
            session, library_id, resource_cat_id
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
    db_provider, test_data_core, min_age, max_age, query_age, expectation
):
    bib_date = datetime.now(timezone.utc) - timedelta(days=80)

    with db_provider.session_scope() as session:
        session.add(
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
                    )
                ],
            )
        )

    with db_provider.session_scope() as session:
        res = retrieve_open_older_resources(session, 1, 1, min_age, max_age)
        assert [r.nid for r in res] == expectation


def test_retrieve_open_older_resources_no_queries_performed(
    db_provider, test_data_core
):
    with db_provider.session_scope() as session:
        session.add(
            Resource(
                nid=1,
                sierraId=22222222,
                libraryId=1,
                bibDate=datetime.now(timezone.utc) - timedelta(days=80),
                resourceCategoryId=1,
                title="TEST TITLE",
                sourceId=1,
                oclcMatchNumber=None,
                status="open",
            )
        )

    with db_provider.session_scope() as session:
        res = retrieve_open_older_resources(session, 1, 1, 30, 90)
        assert res == []


@pytest.mark.parametrize(
    "query_age,expectation",
    [
        pytest.param(91, [], id="queried already"),
        pytest.param(80, [1], id="query needed"),
    ],
)
def test_retrieve_open_older_resources_multiple_queries_performed(
    db_provider, test_data_core, query_age, expectation
):
    bib_date = datetime.now(timezone.utc) - timedelta(days=100)

    with db_provider.session_scope() as session:
        session.add(
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

    with db_provider.session_scope() as session:
        res = retrieve_open_older_resources(session, 1, 1, 90, 180)
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
    db_provider,
    test_data_core,
    lib_id,
    res_cat_id,
    status,
    oclc_match_no,
    max_age,
    expectation,
):
    bib_date = datetime.now(timezone.utc) - timedelta(days=max_age)

    with db_provider.session_scope() as session:
        session.add(
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

    with db_provider.session_scope() as session:
        res = retrieve_open_older_resources(session, 1, 1, 30, 90)
        assert [r.nid for r in res] == expectation


def test_retrieve_new_resources(db_provider, test_data_core):
    with db_provider.session_scope() as session:
        # BPL resources
        session.add(
            Resource(
                nid=1,
                sierraId=11111111,
                libraryId=2,
                resourceCategoryId=2,
                bibDate=datetime.now(timezone.utc).date(),
                title="TEST TITLE 5",
                sourceId=2,
                status="open",
            )
        )

        # NYP resources
        session.add(
            Resource(
                nid=2,
                sierraId=22222222,
                libraryId=1,
                resourceCategoryId=1,
                bibDate=datetime.now(timezone.utc).date(),
                title="TEST TITLE 1",
                sourceId=1,
                status="open",
            )
        )
        session.add(
            Resource(
                nid=3,
                sierraId=22222223,
                libraryId=1,
                resourceCategoryId=2,
                bibDate=datetime.now(timezone.utc).date(),
                title="TEST TITLE 2",
                sourceId=1,
                status="open",
            )
        )
        session.add(
            Resource(
                nid=4,
                sierraId=22222224,
                libraryId=1,
                resourceCategoryId=1,
                bibDate=datetime.now(timezone.utc).date(),
                title="TEST TITLE 3",
                sourceId=1,
                status="open",
            )
        )
        session.add(
            Resource(
                nid=5,
                sierraId=22222225,
                libraryId=1,
                resourceCategoryId=1,
                bibDate=datetime.now(timezone.utc).date(),
                title="TEST TITLE 4",
                sourceId=1,
                status="open",
                queries=[WorldcatQuery(resourceId=5, match=False)],
            )
        )

    with db_provider.session_scope() as session:
        res = retrieve_new_resources(session=session, libraryId=1)
        assert len(res) == 3
        assert res[0].nid == 2
        assert res[1].nid == 4
        assert res[2].nid == 3


def test_retrieve_open_matched_resources_without_full_bib(db_provider, test_data_core):
    some_date = datetime.now(timezone.utc).date()
    with db_provider.session_scope() as session:
        # BPL resources
        session.add(
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
        session.add(
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
        session.add(
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
        session.add(
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
        session.add(
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
        session.add(
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

    with db_provider.session_scope() as session:
        res1 = retrieve_open_matched_resources_without_full_bib(session, libraryId=2)
        assert len(res1) == 0

        res2 = retrieve_open_matched_resources_without_full_bib(session, libraryId=1)
        assert len(res2) == 3
        assert res2[0].nid == 4
        assert res2[1].nid == 5
        assert res2[2].nid == 3


@pytest.mark.parametrize(
    "libraryId,expectation", [(1, ["foo1.mrc"]), (2, ["foo2.mrc"])]
)
def test_retrieve_processed_files(db_provider, test_data_rich, libraryId, expectation):
    with db_provider.session_scope() as session:
        results = retrieve_processed_files(session, libraryId)
        assert results == expectation


def test_retrieve_rotten_apples(db_provider, test_data_core):
    with db_provider.session_scope() as session:
        session.add(RottenApple(code="FOO"))

    with db_provider.session_scope() as session:
        session.add(RottenAppleResource(resourceCategoryId=1, rottenAppleId=3))

    with db_provider.session_scope() as session:
        orgs = retrieve_rotten_apples(session)
        assert orgs == {1: ["UKAHL", "UAH", "FOO"], 2: ["UKAHL"], 3: ["UKAHL"]}


@pytest.mark.parametrize(
    "day_offset, expected_count, status",
    [(1, 1, "expired"), (-1, 0, "open")],
    ids=["expired", "not_expired"],
)
def test_set_resources_to_expired(
    db_provider, test_data_rich, day_offset, expected_count, status
):
    nid = RESOURCE_CATEGORIES["ebook"]["nid"]
    last_period = RESOURCE_CATEGORIES["ebook"]["queryDays"].split(",")[-1]
    last_day = int(last_period.split("-")[-1])

    with db_provider.session_scope() as session:
        session.add(
            Resource(
                sierraId=22222222,
                libraryId=1,
                sourceId=1,
                resourceCategoryId=1,
                status="open",
                bibDate=datetime.now(timezone.utc)
                - timedelta(days=last_day + day_offset),
            )
        )

    with db_provider.session_scope() as session:
        result = set_resources_to_expired(session, nid, last_day)
        assert result == expected_count

    with db_provider.session_scope() as session:
        resource_not_changed = session.scalars(
            select(Resource).filter_by(sierraId=11111111)
        ).one()
        resource_set_to_expired = session.scalars(
            select(Resource).filter_by(sierraId=22222222)
        ).one()
        assert resource_not_changed.status == "bot_enhanced"
        assert resource_set_to_expired.status == status


def test_update_resource(db_provider):
    with db_provider.session_scope() as session:
        lib_rec = insert_or_ignore(session, Library, code="NYP")
        cat_rec = insert_or_ignore(
            session,
            ResourceCategory,
            name="ebook",
            sierraBibFormatBpl="x",
            sierraBibFormatNyp="z",
            queryDays="1-5",
        )
        src_rec = insert_or_ignore(
            session, SourceFile, libraryId=lib_rec.nid, handle="foo"
        )
        session.flush()

        sierraId = 22222222
        rec1 = insert_or_ignore(
            session,
            Resource,
            sierraId=sierraId,
            libraryId=lib_rec.nid,
            resourceCategoryId=cat_rec.nid,
            bibDate=datetime.now(timezone.utc).date(),
            title="TEST TITLE",
            sourceId=src_rec.nid,
            status="open",
        )
        assert rec1.status == "open"

        rec2 = update_resource(
            session, sierraId=sierraId, libraryId=lib_rec.nid, status="expired"
        )
        assert rec2.nid == rec1.nid
        assert rec1.status == "expired"


def test_update_resource_instance_does_not_exist(db_provider):
    with pytest.raises(NoResultFound):
        with db_provider.session_scope() as session:
            update_resource(session, sierraId=22222222, libraryId=1, status="expired")
