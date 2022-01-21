# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
from datetime import datetime
import logging
import os

from pymarc import MARCReader
import pytest

from nightshift.datastore import Resource, OutputFile, SourceFile
from nightshift.ns_exceptions import DriveError
from nightshift.tasks import (
    check_resources_sierra_state,
    enhance_and_output_bibs,
    get_worldcat_brief_bib_matches,
    get_worldcat_full_bibs,
    ingest_new_files,
    isolate_unprocessed_files,
    manipulate_and_serialize_bibs,
    transfer_to_drive,
    update_status_to_upgraded,
)

from ..conftest import (
    MockSuccessfulHTTP200SessionResponse,
    MockSuccessfulHTTP200SessionResponseNoMatches,
)


def test_check_resources_sierra_state_nyp_platform(
    test_session,
    test_data_rich,
    stub_resource,
    mock_platform_env,
    mock_successful_platform_post_token_response,
    mock_successful_platform_session_response,
):
    stub_resource = test_session.query(Resource).filter_by(nid=1).one()
    stub_resource.suppressed = True
    stub_resource.status = "open"

    test_session.commit()

    check_resources_sierra_state(test_session, "NYP", [stub_resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.suppressed is False
    assert resource.status == "staff_enhanced"


def test_check_resources_sierra_state_bpl_solr(
    test_session,
    test_data_rich,
    stub_resource,
    mock_solr_env,
    mock_successful_solr_session_response,
):
    stub_resource = test_session.query(Resource).filter_by(nid=1).one()
    stub_resource.suppressed = False
    stub_resource.status = "expired"
    test_session.commit()

    check_resources_sierra_state(test_session, "BPL", [stub_resource])

    resource = test_session.query(Resource).filter_by(nid=1).one()
    assert resource.suppressed
    assert resource.status == "open"


def test_check_resources_sierra_state_invalid_library_arg(caplog):
    with pytest.raises(ValueError):
        with caplog.at_level(logging.ERROR):
            check_resources_sierra_state(None, "QPL", [])

    assert "Invalid library argument passed: 'QPL'. Must be 'NYP' or 'BPL'"


def test_enhance_and_output_bibs(
    caplog, test_session, test_data_rich, sftpserver, mock_drive, mock_sftp_env
):
    remote_file = f"{datetime.now().date():%y%m%d}-NYP-ebook-01.mrc"
    resources = test_session.query(Resource).where(Resource.nid == 1).all()

    with caplog.at_level(logging.DEBUG):
        with sftpserver.serve_content({"load_dir": {}}):
            enhance_and_output_bibs(test_session, "NYP", 1, "ebook", resources)

    assert "NYP b11111111a has been output to 'temp.mrc'." in caplog.text

    # make sure temp file cleaned up
    assert not os.path.exists("temp.mrc")
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
    get_worldcat_brief_bib_matches(test_session, "NYP", resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query = res.queries[0]
    assert query.nid == 1
    assert query.match
    assert query.response == MockSuccessfulHTTP200SessionResponse().json()
    assert res.oclcMatchNumber == "44959645"
    assert res.status == "open"


@pytest.mark.parametrize("library,library_id", [("NYP", 1), ("BPL", 2)])
def test_get_worldcat_brief_bib_matches_failed(
    library,
    library_id,
    test_session,
    test_data_core,
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
    get_worldcat_brief_bib_matches(test_session, library, resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    query = res.queries[0]
    assert query.nid == 1
    assert query.resourceId == 1
    assert query.match is False
    assert query.response == MockSuccessfulHTTP200SessionResponseNoMatches().json()
    assert res.oclcMatchNumber is None
    assert res.status == "open"


def test_get_worldcat_full_bibs(
    test_session,
    test_data_core,
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
    get_worldcat_full_bibs(test_session, "NYP", resources)

    res = test_session.query(Resource).filter_by(nid=1).all()[0]
    assert res.fullBib == MockSuccessfulHTTP200SessionResponse().content


def test_ingest_new_files(test_session, test_data_core, sftpserver, mock_sftp_env):
    with open("tests/nyp-ebook-sample.mrc", "rb") as test_file:
        marc_data = test_file.read()

    with sftpserver.serve_content(
        {"sierra_dumps_dir": {"foo1.pout": b"foo", "NYP-bar.pout": marc_data}}
    ):
        ingest_new_files(test_session, "NYP", 1)

    # verify source file has been added to db
    src_file_rec = (
        test_session.query(SourceFile)
        .where(SourceFile.libraryId == 1, SourceFile.handle == "NYP-bar.pout")
        .one_or_none()
    )
    assert src_file_rec.libraryId == 1
    assert src_file_rec.handle == "NYP-bar.pout"

    resources = (
        test_session.query(Resource).where(Resource.sourceId == src_file_rec.nid).all()
    )
    assert len(resources) == 2


def test_isolate_unprocessed_nyp_files(
    test_session, test_data_core, mock_drive, sftpserver, caplog
):
    with sftpserver.serve_content(
        {"sierra_dumps_dir": {"foo1.pout": b"spam", "NYP-bar.pout": b"spam"}}
    ):
        with caplog.at_level(logging.DEBUG):
            results = isolate_unprocessed_files(test_session, mock_drive, "NYP", 1)

        assert "Found following remote files for NYP: ['NYP-bar.pout']." in caplog.text

    assert results == ["NYP-bar.pout"]


def test_isolate_unprocessed_bpl_files(
    test_session, test_data_core, mock_drive, sftpserver, caplog
):
    with sftpserver.serve_content(
        {"sierra_dumps_dir": {"foo1.pout": b"spam", "NYP-bar.pout": b"spam"}}
    ):
        with caplog.at_level(logging.DEBUG):
            results = isolate_unprocessed_files(test_session, mock_drive, "BPL", 2)
        assert "Found following remote files for BPL: []." in caplog.text

    assert results == []


def test_manipulate_and_serialize_bibs_default_outfile(
    test_session, test_data_rich, caplog
):

    resources = test_session.query(Resource).all()
    with caplog.at_level(logging.DEBUG):
        file, resources = manipulate_and_serialize_bibs(
            test_session, "NYP", "ebook", resources
        )

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
    assert bib["949"].value() == "*b2=z;"
    assert bib["901"].value() == "NightShift/0.1.0"

    if os.path.exists("temp.mrc"):
        try:
            os.remove("temp.mrc")
        except:
            raise


def test_manipulate_and_serialize_bibs_custom_outfile(
    test_session, test_data_rich, caplog, tmpdir
):
    outfile = tmpdir.join("custom_file.mrc")
    resources = test_session.query(Resource).all()
    with caplog.at_level(logging.DEBUG):
        file, resources = manipulate_and_serialize_bibs(
            test_session, "NYP", "ebook", resources, outfile
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
    assert bib["949"].value() == "*b2=z;"
    assert bib["901"].value() == "NightShift/0.1.0"


def test_manipulate_and_serialize_bibs_failed(test_session, test_data_rich, caplog):

    resource = test_session.query(Resource).one_or_none()
    resource.resourceCategoryId = 5

    with caplog.at_level(logging.WARNING):
        file, resources = manipulate_and_serialize_bibs(
            test_session, "NYP", "ebook", [resource]
        )

    assert "NYP b11111111a enhancement incomplete. Skipping." in caplog.text

    assert file is None
    assert len(resources) == 0


def test_transfer_to_drive(mock_drive, caplog, sftpserver, tmpdir):
    base_name = f"{datetime.now().date():%y%m%d}-NYP-ebook"
    tmpfile = tmpdir.join("temp.mrc")
    tmpfile.write("spam")
    with sftpserver.serve_content({"load_dir": {f"{base_name}-01.mrc": "spam"}}):
        with caplog.at_level(logging.INFO):
            transfer_to_drive("NYP", "ebook", str(tmpfile))
            assert mock_drive.sftp.listdir("load_dir") == [
                f"{base_name}-01.mrc",
                f"{base_name}-02.mrc",
            ]
    assert f"NYP ebook records have been output to remote '{base_name}-02.mrc'"

    # make sure temp file cleanup
    assert not os.path.exists(tmpfile)


def test_transfer_to_drive_temp_file_not_created(mock_sftp_env, sftpserver, caplog):
    with caplog.at_level(logging.INFO):
        remote_file = transfer_to_drive("NYP", "ebook", None)
    assert remote_file is None
    assert "No source file to output to SFTP" in caplog.text


def test_transfer_to_drive_unable_to_del_temp_file_exception(
    caplog, mock_sftp_env, sftpserver, mock_os_error_on_remove, tmpdir
):
    tmpfile = tmpdir.join("temp.mrc")
    tmpfile.write("spam")
    with sftpserver.serve_content({"load_dir": {"foo.mrc": "spam"}}):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(OSError):
                transfer_to_drive("NYP", "ebook", str(tmpfile))

    assert (
        f"Unable to delete '{str(tmpfile)}' file after completing the job. Error "
        in caplog.text
    )


def test_transfer_to_drive_sftp_error(mock_sftp_env, sftpserver, mock_io_error, tmpdir):
    tmpfile = tmpdir.join("temp.mrc")
    tmpfile.write("spam")
    with sftpserver.serve_content({"laod_dir": {}}):
        with pytest.raises(DriveError):
            transfer_to_drive("NYP", "ebook", str(tmpfile))


def test_update_status_to_upgraded(test_session, test_data_rich, caplog):
    resources = test_session.query(Resource).all()
    with does_not_raise():
        with caplog.at_level(logging.INFO):
            update_status_to_upgraded(test_session, 1, "foo3.mrc", resources)

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


def test_update_status_to_upgraded_no_new_file(caplog, test_session):
    with caplog.at_level(logging.INFO):
        update_status_to_upgraded(test_session, 1, None, [])

    assert (
        "Skipping resources enhancement status update. No SFTP output file this time."
        in caplog.text
    )
