# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
from datetime import datetime, date
import logging
import os

from pymarc import MARCReader
import pytest

from nightshift.constants import ROTTEN_APPLES
from nightshift.datastore import Event, Resource, OutputFile, SourceFile
from nightshift.datastore_transactions import ResCatByName, ResCatById
from nightshift.ns_exceptions import DriveError
from nightshift.tasks import Tasks

from ..conftest import (
    MockSuccessfulHTTP200SessionResponse,
    MockSuccessfulHTTP200SessionResponseNoMatches,
)


def test_create_resource_category_idx(test_session, stub_res_cat_by_name):
    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks._res_cat = dict(
        ebook=ResCatByName(9, "a", "b", ["999"], ["099", "199"], [(15, 30), (30, 60)]),
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


def test_create_rotten_apples_idx(test_session, test_data_core, stub_res_cat_by_name):
    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    res = tasks._create_rotten_apples_idx()
    assert res == {1: ["UKAHL", "UAH"], 2: ["UKAHL"], 3: ["UKAHL"]}


def test_check_resources_sierra_state_nyp_platform(
    test_session,
    test_data_rich,
    stub_resource,
    stub_res_cat_by_name,
    mock_platform_env,
    mock_successful_platform_post_token_response,
    mock_successful_platform_session_response,
):
    stub_resource = test_session.query(Resource).filter_by(nid=1).one()
    stub_resource.suppressed = True
    stub_resource.status = "open"

    test_session.commit()

    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([stub_resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.suppressed is False
    assert resource.status == "staff_enhanced"

    # check if event recorded
    event = test_session.query(Event).one_or_none()
    assert isinstance(event, Event)
    assert isinstance(event.timestamp, datetime)
    assert event.libraryId == 1
    assert event.sierraId == 11111111
    assert isinstance(event.bibDate, date)
    assert event.resourceCategoryId == 1
    assert event.status == "staff_enhanced"


def test_check_resources_sierra_state_nyp_platform_deleted_record(
    test_session,
    test_data_rich,
    stub_resource,
    stub_res_cat_by_name,
    mock_platform_env,
    mock_successful_platform_post_token_response,
    mock_successful_platform_session_response_deleted_record,
):
    stub_resource = test_session.query(Resource).filter_by(nid=1).one()
    stub_resource.suppressed = True
    stub_resource.status = "open"

    test_session.commit()

    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([stub_resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.suppressed is False
    assert resource.status == "staff_deleted"

    # check if event recorded
    event = test_session.query(Event).one_or_none()
    assert isinstance(event, Event)
    assert isinstance(event.timestamp, datetime)
    assert event.libraryId == 1
    assert event.sierraId == 11111111
    assert isinstance(event.bibDate, date)
    assert event.resourceCategoryId == 1
    assert event.status == "staff_deleted"


def test_check_resources_sierra_state_bpl_solr(
    test_session,
    test_data_rich,
    stub_resource,
    stub_res_cat_by_name,
    mock_solr_env,
    mock_successful_solr_session_response,
):
    stub_resource = test_session.query(Resource).filter_by(nid=1).one()
    stub_resource.suppressed = False
    stub_resource.status = "open"
    test_session.commit()

    tasks = Tasks(test_session, "BPL", 2, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([stub_resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.suppressed
    assert resource.status == "open"


def test_check_resources_sierra_state_bpl_solr_no_record(
    test_session,
    test_data_rich,
    stub_res_cat_by_name,
    mock_solr_env,
    mock_failed_solr_session_response,
):
    resource = test_session.query(Resource).filter_by(nid=1).one()
    resource.libraryId = 2
    resource.status = "open"
    test_session.commit()

    tasks = Tasks(test_session, "BPL", 2, stub_res_cat_by_name)
    tasks.check_resources_sierra_state([resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.status == "staff_deleted"

    # check if even recorded
    event = test_session.query(Event).one_or_none()
    assert event is not None
    assert isinstance(event.timestamp, datetime)
    assert event.libraryId == 2
    assert event.sierraId == 11111111
    assert isinstance(event.bibDate, date)
    assert event.resourceCategoryId == 1
    assert event.status == "staff_deleted"


def test_check_resources_sierra_state_invalid_library_arg(caplog):
    with pytest.raises(ValueError):
        with caplog.at_level(logging.ERROR):
            tasks = Tasks(None, "QPL", 3, {})
            tasks.check_resources_sierra_state([])

    assert "Invalid library argument passed: 'QPL'. Must be 'NYP' or 'BPL'"


def test_enhance_and_output_bibs(
    caplog,
    sftpserver,
    test_session,
    test_data_rich,
    stub_res_cat_by_name,
    mock_drive,
    mock_sftp_env,
):
    remote_file = f"{datetime.now().date():%y%m%d}-NYP-ebook-01.mrc"
    resources = test_session.query(Resource).where(Resource.nid == 1).all()

    with caplog.at_level(logging.DEBUG):
        with sftpserver.serve_content({"load_dir": {}}):
            tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
            tasks.enhance_and_output_bibs("ebook", resources)

    assert "NYP b11111111a has been output to 'temp.mrc'." in caplog.text

    # check database state
    output_record = (
        test_session.query(OutputFile).where(OutputFile.nid == 2).one_or_none()
    )
    assert output_record.libraryId == 1
    assert output_record.handle == remote_file
    assert output_record.timestamp is not None

    resource = test_session.query(Resource).where(Resource.nid == 1).one()
    assert resource.status == "bot_enhanced"
    assert resource.outputId is not None
    assert resource.enhanceTimestamp is not None


def test_get_worldcat_brief_bib_matches_success(
    test_session,
    test_data_core,
    stub_res_cat_by_name,
    mock_worldcat_creds,
    mock_successful_post_token_response,
    mock_successful_session_get_request,
):
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=1,
            resourceCategoryId=1,
            sourceId=1,
            bibDate=datetime.utcnow().date(),
            title="Pride and prejudice.",
            distributorNumber="123",
            status="open",
        )
    )
    test_session.commit()
    resources = test_session.query(Resource).filter_by(nid=1).all()
    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks.get_worldcat_brief_bib_matches(resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query = res.queries[0]
    assert query.nid == 1
    assert query.match
    assert query.response == MockSuccessfulHTTP200SessionResponse().json()
    assert res.oclcMatchNumber == "44959645"
    assert res.status == "open"

    # check if correct event recorded
    event = test_session.query(Event).one_or_none()
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
    test_session,
    test_data_core,
    stub_res_cat_by_name,
    mock_worldcat_creds,
    mock_successful_post_token_response,
    mock_successful_session_get_request_no_matches,
):
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=library_id,
            resourceCategoryId=1,
            sourceId=1,
            bibDate=datetime.utcnow().date(),
            title="Pride and prejudice.",
            distributorNumber="123",
            status="open",
        )
    )
    test_session.commit()
    resources = test_session.query(Resource).filter_by(nid=1).all()
    tasks = Tasks(test_session, library, library_id, stub_res_cat_by_name)
    tasks.get_worldcat_brief_bib_matches(resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query = res.queries[0]
    assert query.nid == 1
    assert query.resourceId == 1
    assert query.match is False
    assert query.response == MockSuccessfulHTTP200SessionResponseNoMatches().json()
    assert res.oclcMatchNumber is None
    assert res.status == "open"

    # check if correct event recorded
    event = test_session.query(Event).one_or_none()
    assert event is not None
    assert isinstance(event.timestamp, datetime)
    assert event.libraryId == library_id
    assert event.sierraId == 11111111
    assert isinstance(event.bibDate, date)
    assert event.resourceCategoryId == 1
    assert event.status == "worldcat_miss"


def test_get_worldcat_full_bibs(
    test_session,
    test_data_core,
    stub_res_cat_by_name,
    mock_worldcat_creds,
    mock_successful_post_token_response,
    mock_successful_session_get_request,
):
    test_session.add(
        Resource(
            nid=1,
            sierraId=11111111,
            libraryId=1,
            resourceCategoryId=1,
            sourceId=1,
            bibDate=datetime.utcnow().date(),
            title="Pride and prejudice.",
            distributorNumber="123",
            status="open",
            oclcMatchNumber="44959645",
        )
    )
    test_session.commit()
    resources = test_session.query(Resource).filter_by(nid=1).all()
    tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
    tasks.get_worldcat_full_bibs(resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    assert res.fullBib == MockSuccessfulHTTP200SessionResponse().content


def test_ingest_new_files(
    sftpserver, test_session, test_data_core, stub_res_cat_by_name, mock_sftp_env
):
    with open("tests/nyp-ebook-sample.mrc", "rb") as test_file:
        marc_data = test_file.read()

    with sftpserver.serve_content(
        {"sierra_dumps_dir": {"foo1-pout": b"foo", "NYP-bar-pout": marc_data}}
    ):
        tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
        tasks.ingest_new_files()

    # verify source file has been added to db
    src_file_rec = (
        test_session.query(SourceFile)
        .where(SourceFile.libraryId == 1, SourceFile.handle == "NYP-bar-pout")
        .one_or_none()
    )
    assert src_file_rec.libraryId == 1
    assert src_file_rec.handle == "NYP-bar-pout"

    resources = (
        test_session.query(Resource).where(Resource.sourceId == src_file_rec.nid).all()
    )
    assert len(resources) == 2


def test_ingest_new_files_empty_file(
    sftpserver, test_session, test_data_core, stub_res_cat_by_name, mock_sftp_env
):

    with sftpserver.serve_content(
        {"sierra_dumps_dir": {"foo1-pout": b"foo", "NYP-bar-pout": ""}}
    ):
        tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
        tasks.ingest_new_files()

    # verify source file has been added to db
    src_file_rec = (
        test_session.query(SourceFile)
        .where(SourceFile.libraryId == 1, SourceFile.handle == "NYP-bar-pout")
        .one_or_none()
    )
    assert src_file_rec.libraryId == 1
    assert src_file_rec.handle == "NYP-bar-pout"

    resources = (
        test_session.query(Resource).where(Resource.sourceId == src_file_rec.nid).all()
    )
    assert resources == []


def test_isolate_unprocessed_nyp_files(
    caplog, sftpserver, stub_res_cat_by_name, test_session, test_data_core, mock_drive
):
    with sftpserver.serve_content(
        {"sierra_dumps_dir": {"foo1-pout": b"spam", "NYP-bar-pout": b"spam"}}
    ):
        with caplog.at_level(logging.DEBUG):
            tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
            results = tasks.isolate_unprocessed_files(mock_drive)

        assert "Found following remote files for NYP: ['NYP-bar-pout']." in caplog.text

    assert results == ["NYP-bar-pout"]


def test_isolate_unprocessed_bpl_files(
    caplog, sftpserver, stub_res_cat_by_name, test_session, test_data_core, mock_drive
):
    with sftpserver.serve_content(
        {"sierra_dumps_dir": {"foo1-pout": b"spam", "NYP-bar-pout": b"spam"}}
    ):
        with caplog.at_level(logging.DEBUG):
            tasks = Tasks(test_session, "BPL", 2, stub_res_cat_by_name)
            results = tasks.isolate_unprocessed_files(mock_drive)
        assert "Found following remote files for BPL: []." in caplog.text

    assert results == []


def test_manipulate_and_serialize_bibs_default_outfile(
    caplog,
    stub_res_cat_by_name,
    test_session,
    test_data_rich,
):

    resources = test_session.query(Resource).all()
    with caplog.at_level(logging.DEBUG):
        tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
        file, resources = tasks.manipulate_and_serialize_bibs("ebook", resources)

    assert "NYP b11111111a has been output to 'temp.mrc'." in caplog.text
    assert (
        f"Enhanced and serialized 1 and skipped 0 NYP ebook record(s)." in caplog.text
    )

    assert file == "temp.mrc"
    assert len(resources) == 1

    with open("temp.mrc", "rb") as f:
        reader = MARCReader(f)
        bib = next(reader)

    assert bib["091"].value() == "eNYPL Book"
    assert bib["945"].value() == ".b11111111a"
    assert bib["949"].value() == "*b2=z;bn=ia;"
    assert bib["901"].value() == "NightShift/0.5.0"

    if os.path.exists("temp.mrc"):
        try:
            os.remove("temp.mrc")
        except:
            raise


def test_manipulate_and_serialize_bibs_custom_outfile(
    caplog,
    tmpdir,
    test_session,
    test_data_rich,
    stub_res_cat_by_name,
):
    outfile = tmpdir.join("custom_file.mrc")
    resources = test_session.query(Resource).all()
    with caplog.at_level(logging.DEBUG):
        tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
        file, resources = tasks.manipulate_and_serialize_bibs(
            "ebook", resources, outfile
        )

    assert f"NYP b11111111a has been output to '{outfile}'." in caplog.text
    assert (
        f"Enhanced and serialized 1 and skipped 0 NYP ebook record(s)." in caplog.text
    )

    assert file == outfile
    assert len(resources) == 1

    with open(outfile, "rb") as f:
        reader = MARCReader(f)
        bib = next(reader)

    assert bib["091"].value() == "eNYPL Book"
    assert bib["945"].value() == ".b11111111a"
    assert bib["949"].value() == "*b2=z;bn=ia;"
    assert bib["901"].value() == "NightShift/0.5.0"


def test_manipulate_and_serialize_bibs_failed(
    caplog, test_session, test_data_rich, stub_res_cat_by_name
):

    resource = test_session.query(Resource).one_or_none()
    resource.resourceCategoryId = 5

    with caplog.at_level(logging.WARNING):
        tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
        file, resources = tasks.manipulate_and_serialize_bibs("ebook", [resource])

    assert "NYP b11111111a enhancement incomplete. Skipping." in caplog.text

    assert file is None
    assert len(resources) == 0


def test_manipulate_and_serialize_bibs_os_error_on_temp_file_removal(
    caplog, test_session, test_data_rich, stub_res_cat_by_name, mock_os_error_on_remove
):
    resource = test_session.query(Resource).one()
    with pytest.raises(OSError):
        with caplog.at_level(logging.ERROR):
            tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
            tasks.manipulate_and_serialize_bibs("ebook", [resource])

    assert "Unable to empty temp file 'temp.mrc'" in caplog.text


def test_transfer_to_drive(
    caplog, sftpserver, tmpdir, mock_drive, stub_res_cat_by_name
):
    base_name = f"{datetime.now().date():%y%m%d}-NYP-ebook"
    tmpfile = tmpdir.join("temp.mrc")
    tmpfile.write("spam")
    with sftpserver.serve_content({"load_dir": {f"{base_name}-01.mrc": "spam"}}):
        with caplog.at_level(logging.INFO):
            tasks = Tasks(None, "NYP", 1, stub_res_cat_by_name)
            tasks.transfer_to_drive("ebook", str(tmpfile))
            assert mock_drive.sftp.listdir("load_dir") == [
                f"{base_name}-01.mrc",
                f"{base_name}-02.mrc",
            ]
    assert f"NYP ebook records have been output to remote '{base_name}-02.mrc'"


def test_transfer_to_drive_temp_file_not_created(
    caplog, sftpserver, mock_sftp_env, stub_res_cat_by_name
):
    with caplog.at_level(logging.INFO):
        tasks = Tasks(None, "NYP", 1, stub_res_cat_by_name)
        remote_file = tasks.transfer_to_drive("ebook", None)
    assert remote_file is None
    assert "No source file to output to SFTP" in caplog.text


def test_transfer_to_drive_sftp_error(
    tmpdir, sftpserver, stub_res_cat_by_name, mock_sftp_env, mock_io_error
):
    tmpfile = tmpdir.join("temp.mrc")
    tmpfile.write("spam")
    with sftpserver.serve_content({"laod_dir": {}}):
        with pytest.raises(DriveError):
            tasks = Tasks(None, "NYP", 1, stub_res_cat_by_name)
            tasks.transfer_to_drive("ebook", str(tmpfile))


def test_update_status_to_upgraded(
    caplog, test_session, test_data_rich, stub_res_cat_by_name
):
    resources = test_session.query(Resource).all()
    with does_not_raise():
        with caplog.at_level(logging.INFO):
            tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
            tasks.update_status_to_upgraded("foo3.mrc", resources)

        assert "Updating 1 resources status to 'bot_enhanced'." in caplog.text

    # check if output file record has been created
    out_file_record = (
        test_session.query(OutputFile)
        .where(OutputFile.handle == "foo3.mrc", OutputFile.libraryId == 1)
        .one_or_none()
    )
    assert out_file_record is not None

    results = test_session.query(Resource).all()
    assert len(results) > 0
    for resource in results:
        assert resource.status == "bot_enhanced"
        assert resource.outputId == 2
        assert resource.enhanceTimestamp is not None

    # check if correct event recorded
    event = test_session.query(Event).one_or_none()
    assert event is not None
    assert isinstance(event.timestamp, datetime)
    assert event.libraryId == 1
    assert event.sierraId == 11111111
    assert isinstance(event.bibDate, date)
    assert event.resourceCategoryId == 1
    assert event.status == "bot_enhanced"


def test_update_status_to_upgraded_no_new_file(
    caplog, test_session, stub_res_cat_by_name
):
    with caplog.at_level(logging.INFO):
        tasks = Tasks(test_session, "NYP", 1, stub_res_cat_by_name)
        tasks.update_status_to_upgraded(None, [])

    assert (
        "Skipping resources enhancement status update. No SFTP output file this time."
        in caplog.text
    )

    # check if correct event recorded
    event = test_session.query(Event).one_or_none()
    assert event is None
