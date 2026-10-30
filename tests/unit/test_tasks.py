import os
from datetime import date, datetime, timezone

import pytest
from pymarc import MARCReader
from sqlalchemy import select

from nightshift.comms.storage import Drive, get_credentials
from nightshift.datastore import Event, OutputFile, Resource, SourceFile
from nightshift.datastore_transactions import ResCatById, ResCatByName
from nightshift.tasks import Tasks


def test_create_resource_category_idx(db_provider, stub_res_cat_by_name):
    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks._res_cat = dict(
        ebook=ResCatByName(9, "a", "b", ["999"], ["099", "199"], [(15, 30), (30, 60)])
    )
    res = tasks._create_resource_category_idx()
    assert isinstance(res, dict)
    assert len(res) == 1
    assert isinstance(res[9], ResCatById)
    assert res[9].name == "ebook"
    assert res[9].sierraBibFormatBpl == "a"
    assert res[9].sierraBibFormatNyp == "b"
    assert res[9].srcTags2Keep == ["999"]
    assert res[9].dstTags2Delete == ["099", "199"]
    assert res[9].queryDays == [(15, 30), (30, 60)]


def test_create_rotten_apples_idx(db_provider, test_data_core, stub_res_cat_by_name):
    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    res = tasks._create_rotten_apples_idx()
    assert res == {1: ["UKAHL", "UAH"], 2: ["UKAHL"], 3: ["UKAHL"]}


def test_check_resources_sierra_state_nyp_platform(
    db_provider,
    test_data_rich,
    stub_res_cat_by_name,
    mock_env_vars,
    mock_token,
    mock_successful_platform_session_response,
):
    with db_provider.session_scope() as session:
        stub_resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        stub_resource.suppressed = True
        stub_resource.status = "open"

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([stub_resource])

    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        assert resource.suppressed is False
        assert resource.status == "staff_enhanced"

        event = session.scalars(select(Event)).one_or_none()
        assert isinstance(event, Event)
        assert isinstance(event.timestamp, datetime)
        assert event.libraryId == 1
        assert event.sierraId == 11111111
        assert isinstance(event.bibDate, date)
        assert event.resourceCategoryId == 1
        assert event.status == "staff_enhanced"


def test_check_resources_sierra_state_nyp_platform_deleted_record(
    db_provider,
    test_data_rich,
    stub_res_cat_by_name,
    mock_env_vars,
    mock_token,
    mock_successful_platform_session_response_deleted_record,
):
    with db_provider.session_scope() as session:
        stub_resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        stub_resource.suppressed = True
        stub_resource.status = "open"

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([stub_resource])

    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        assert resource.suppressed is False
        assert resource.status == "staff_deleted"

        event = session.scalars(select(Event)).one_or_none()
        assert isinstance(event, Event)
        assert isinstance(event.timestamp, datetime)
        assert event.libraryId == 1
        assert event.sierraId == 11111111
        assert isinstance(event.bibDate, date)
        assert event.resourceCategoryId == 1
        assert event.status == "staff_deleted"


def test_check_resources_sierra_state_bpl_solr(
    db_provider,
    test_data_rich,
    stub_res_cat_by_name,
    mock_env_vars,
    mock_successful_solr_session_response,
):
    with db_provider.session_scope() as session:
        stub_resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        stub_resource.suppressed = False
        stub_resource.status = "open"

    tasks = Tasks(db_provider, "BPL", 2, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([stub_resource])

    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        assert resource.suppressed
        assert resource.status == "open"


def test_check_resources_sierra_state_bpl_solr_no_record(
    db_provider,
    test_data_rich,
    stub_res_cat_by_name,
    mock_env_vars,
    mock_failed_solr_session_response,
):
    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        resource.libraryId = 2
        resource.status = "open"

    tasks = Tasks(db_provider, "BPL", 2, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([resource])

    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource).filter_by(nid=1)).one()
        assert resource.status == "staff_deleted"

        event = session.scalars(select(Event)).one_or_none()
        assert event is not None
        assert isinstance(event.timestamp, datetime)
        assert event.libraryId == 2
        assert event.sierraId == 11111111
        assert isinstance(event.bibDate, date)
        assert event.resourceCategoryId == 1
        assert event.status == "staff_deleted"


def test_check_resources_sierra_state_invalid_library_arg(caplog):
    with pytest.raises(ValueError):
        tasks = Tasks(None, "QPL", 3, {})
        tasks.check_resources_sierra_state([])

    assert (
        "Invalid library argument passed: 'QPL'. Must be 'NYP' or 'BPL'" in caplog.text
    )


def test_enhance_and_output_bibs(
    caplog, db_provider, test_data_rich, stub_res_cat_by_name, mock_drive
):
    remote_file = f"{datetime.now().date():%y%m%d}-NYP-ebook-01.mrc"
    with db_provider.session_scope() as session:
        resources = list(
            session.scalars(select(Resource).where(Resource.nid == 1)).all()
        )

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.enhance_and_output_bibs("ebook", resources)

    assert "NYP b11111111a has been output to 'temp.mrc'." in caplog.text

    with db_provider.session_scope() as session:
        output_record = session.scalars(
            select(OutputFile).where(OutputFile.nid == 2)
        ).one_or_none()
        assert output_record is not None
        assert output_record.libraryId == 1
        assert output_record.handle == remote_file
        assert output_record.timestamp is not None

        resource = session.scalars(select(Resource).where(Resource.nid == 1)).one()
        assert resource.status == "bot_enhanced"
        assert resource.outputId is not None
        assert resource.enhanceTimestamp is not None


def test_get_worldcat_brief_bib_matches_success(
    db_provider,
    test_data_core,
    stub_res_cat_by_name,
    mock_env_vars,
    mock_token,
    mock_worldcat_response,
):
    with db_provider.session_scope() as session:
        session.add(
            Resource(
                nid=1,
                sierraId=11111111,
                libraryId=1,
                resourceCategoryId=1,
                sourceId=1,
                bibDate=datetime.now(timezone.utc).date(),
                title="Pride and prejudice.",
                distributorNumber="123",
                status="open",
            )
        )

    with db_provider.session_scope() as session:
        resources = list(session.scalars(select(Resource).filter_by(nid=1)).all())

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.get_worldcat_brief_bib_matches(resources)

    with db_provider.session_scope() as session:
        res = session.scalars(select(Resource).filter_by(nid=1)).first()
        assert res is not None
        query = res.queries[0]
        assert query.nid == 1
        assert query.match
        assert query.response == mock_worldcat_response.json()
        assert res.oclcMatchNumber == "44959645"
        assert res.status == "open"

        event = session.scalars(select(Event)).one_or_none()
        assert event is not None
        assert isinstance(event.timestamp, datetime)
        assert event.libraryId == 1
        assert event.sierraId == 11111111
        assert isinstance(event.bibDate, date)
        assert event.resourceCategoryId == 1
        assert event.status == "worldcat_hit"


@pytest.mark.parametrize("library,library_id", [("NYP", 1), ("BPL", 2)])
def test_get_worldcat_brief_bib_matches_failed(
    library,
    library_id,
    db_provider,
    test_data_core,
    stub_res_cat_by_name,
    mock_env_vars,
    mock_token,
    mock_worldcat_response_no_matches,
):
    with db_provider.session_scope() as session:
        session.add(
            Resource(
                nid=1,
                sierraId=11111111,
                libraryId=library_id,
                resourceCategoryId=1,
                sourceId=1,
                bibDate=datetime.now(timezone.utc).date(),
                title="Pride and prejudice.",
                distributorNumber="123",
                status="open",
            )
        )

    with db_provider.session_scope() as session:
        resources = list(session.scalars(select(Resource).filter_by(nid=1)).all())

    tasks = Tasks(db_provider, library, library_id, stub_res_cat_by_name)
    tasks.get_worldcat_brief_bib_matches(resources)

    with db_provider.session_scope() as session:
        res = session.scalars(select(Resource).filter_by(nid=1)).first()
        assert res is not None
        query = res.queries[0]
        assert query.nid == 1
        assert query.resourceId == 1
        assert query.match is False
        assert query.response == mock_worldcat_response_no_matches.json()
        assert res.oclcMatchNumber is None
        assert res.status == "open"

        event = session.scalars(select(Event)).one_or_none()
        assert event is not None
        assert isinstance(event.timestamp, datetime)
        assert event.libraryId == library_id
        assert event.sierraId == 11111111
        assert isinstance(event.bibDate, date)
        assert event.resourceCategoryId == 1
        assert event.status == "worldcat_miss"


def test_get_worldcat_full_bibs(
    db_provider,
    test_data_core,
    stub_res_cat_by_name,
    mock_env_vars,
    mock_token,
    mock_worldcat_response,
):
    with db_provider.session_scope() as session:
        session.add(
            Resource(
                nid=1,
                sierraId=11111111,
                libraryId=1,
                resourceCategoryId=1,
                sourceId=1,
                bibDate=datetime.now(timezone.utc).date(),
                title="Pride and prejudice.",
                distributorNumber="123",
                status="open",
                oclcMatchNumber="44959645",
            )
        )

    with db_provider.session_scope() as session:
        resources = list(session.scalars(select(Resource).filter_by(nid=1)).all())

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.get_worldcat_full_bibs(resources)

    with db_provider.session_scope() as session:
        res = session.scalars(select(Resource).filter_by(nid=1)).first()
        assert res is not None
        assert res.fullBib == mock_worldcat_response.content


def test_ingest_new_files(
    db_provider, test_data_core, stub_res_cat_by_name, mock_drive
):
    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.ingest_new_files()

    with db_provider.session_scope() as session:
        src_file_rec = session.scalars(
            select(SourceFile).where(
                SourceFile.libraryId == 1, SourceFile.handle == "NYP-bar-pout"
            )
        ).one_or_none()
        assert src_file_rec is not None
        assert src_file_rec.libraryId == 1
        assert src_file_rec.handle == "NYP-bar-pout"

        resources = list(
            session.scalars(
                select(Resource).where(Resource.sourceId == src_file_rec.nid)
            ).all()
        )
        assert resources == []


@pytest.mark.parametrize(
    "library, lib_id, expected_files, log_snippet",
    [
        (
            "NYP",
            1,
            ["NYP-bar-pout"],
            "Found following remote files for NYP: ['NYP-bar-pout'].",
        ),
        ("BPL", 2, [], "Found following remote files for BPL: []."),
    ],
    ids=["NYP", "BPL"],
)
def test_isolate_unprocessed_files(
    caplog,
    stub_res_cat_by_name,
    db_provider,
    test_data_core,
    mock_drive,
    library,
    lib_id,
    expected_files,
    log_snippet,
):
    tasks = Tasks(db_provider, library, lib_id, stub_res_cat_by_name)
    with Drive(*get_credentials()) as drive:
        results = tasks.isolate_unprocessed_files(drive)
    assert log_snippet in caplog.text
    assert results == expected_files


def test_manipulate_and_serialize_bibs_default_outfile(
    caplog, stub_res_cat_by_name, db_provider, test_data_rich
):
    with db_provider.session_scope() as session:
        resources = list(session.scalars(select(Resource)).all())

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    file, res_list = tasks.manipulate_and_serialize_bibs("ebook", resources)
    assert "NYP b11111111a has been output to 'temp.mrc'." in caplog.text
    assert "Enhanced and serialized 1 and skipped 0 NYP ebook record(s)." in caplog.text
    assert file == "temp.mrc"
    assert len(res_list) == 1
    with open("temp.mrc", "rb") as f:
        reader = MARCReader(f)
        bib = next(reader)
    assert bib["091"].value() == "eNYPL Book"
    assert bib["945"].value() == ".b11111111a"
    assert bib["949"].value() == "*b2=z;bn=ia;"
    assert bib["901"].value() == "NightShift/0.6.2"

    if os.path.exists("temp.mrc"):
        try:
            os.remove("temp.mrc")
        except OSError:
            raise


def test_manipulate_and_serialize_bibs_custom_outfile(
    caplog, tmpdir, db_provider, test_data_rich, stub_res_cat_by_name
):
    outfile = tmpdir.join("custom_file.mrc")
    with db_provider.session_scope() as session:
        resources = list(session.scalars(select(Resource)).all())

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    file, res_list = tasks.manipulate_and_serialize_bibs(
        "ebook", resources, str(outfile)
    )

    assert f"NYP b11111111a has been output to '{outfile}'." in caplog.text
    assert "Enhanced and serialized 1 and skipped 0 NYP ebook record(s)." in caplog.text
    assert file == str(outfile)
    assert len(res_list) == 1
    with open(outfile, "rb") as f:
        reader = MARCReader(f)
        bib = next(reader)

    assert bib["091"].value() == "eNYPL Book"
    assert bib["945"].value() == ".b11111111a"
    assert bib["949"].value() == "*b2=z;bn=ia;"
    assert bib["901"].value() == "NightShift/0.6.2"


def test_manipulate_and_serialize_bibs_failed(
    caplog, db_provider, test_data_rich, stub_res_cat_by_name
):
    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource)).one_or_none()
        assert resource is not None
        resource.resourceCategoryId = 5

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    file, res_list = tasks.manipulate_and_serialize_bibs("ebook", [resource])

    assert "NYP b11111111a enhancement incomplete. Skipping." in caplog.text
    assert file is None
    assert len(res_list) == 0


def test_manipulate_and_serialize_bibs_os_error_on_temp_file_removal(
    caplog, db_provider, test_data_rich, stub_res_cat_by_name, mock_os_error_on_remove
):
    with db_provider.session_scope() as session:
        resource = session.scalars(select(Resource)).one()

    with pytest.raises(OSError):
        tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
        tasks.manipulate_and_serialize_bibs("ebook", [resource])

    assert "Unable to empty temp file 'temp.mrc'" in caplog.text


def test_transfer_to_drive(tmpdir, mock_drive, stub_res_cat_by_name):
    base_name = f"{datetime.now().date():%y%m%d}-NYP-ebook"
    tmpfile = tmpdir.join("temp.mrc")
    tmpfile.write("spam")
    tasks = Tasks(None, "NYP", 1, stub_res_cat_by_name)
    res = tasks.transfer_to_drive("ebook", str(tmpfile))
    assert res == f"{base_name}-01.mrc" or res is not None


def test_transfer_to_drive_temp_file_not_created(
    caplog, mock_drive, stub_res_cat_by_name
):
    tasks = Tasks(None, "NYP", 1, stub_res_cat_by_name)
    remote_file = tasks.transfer_to_drive("ebook", None)
    assert remote_file is None
    assert "No source file to output to SFTP" in caplog.text


def test_update_status_to_upgraded(
    caplog, db_provider, test_data_rich, stub_res_cat_by_name
):
    with db_provider.session_scope() as session:
        resources = list(session.scalars(select(Resource)).all())

    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.update_status_to_upgraded("foo3.mrc", resources)

    assert "Updating 1 resources status to 'bot_enhanced'." in caplog.text

    with db_provider.session_scope() as session:
        out_file_record = session.scalars(
            select(OutputFile).where(
                OutputFile.handle == "foo3.mrc", OutputFile.libraryId == 1
            )
        ).one_or_none()
        assert out_file_record is not None

        results = list(session.scalars(select(Resource)).all())
        assert len(results) > 0
        for resource in results:
            assert resource.status == "bot_enhanced"
            assert resource.outputId == 2
            assert resource.enhanceTimestamp is not None

        event = session.scalars(select(Event)).one_or_none()
        assert event is not None
        assert isinstance(event.timestamp, datetime)
        assert event.libraryId == 1
        assert event.sierraId == 11111111
        assert isinstance(event.bibDate, date)
        assert event.resourceCategoryId == 1
        assert event.status == "bot_enhanced"


def test_update_status_to_upgraded_no_new_file(
    caplog, db_provider, stub_res_cat_by_name
):
    tasks = Tasks(db_provider, "NYP", 1, stub_res_cat_by_name)
    tasks.update_status_to_upgraded(None, [])
    assert (
        "Skipping resources enhancement status update. No SFTP output file this time."
        in caplog.text
    )

    with db_provider.session_scope() as session:
        event = session.scalars(select(Event)).one_or_none()
        assert event is None
