# -*- coding: utf-8 -*-

"""
worker module tests

"""
import datetime

import pytest

from nightshift.datastore import Resource, ExportFile
from nightshift.workers import (
    import_sierra_data,
    import_export_file_data,
    import_platform_data,
    retrieve_bibnos_for_enhancement,
    retrieve_eresource_records_for_worldcat_queries,
)


def test_import_export_file_data(init_dataset):
    session = init_dataset
    fh = "tests/files/bpl-ere-export-sample.txt"
    rec = import_export_file_data(fh, session)
    assert rec.efid == 1
    assert rec.handle == "bpl-ere-export-sample.txt"


def test_import_sierra_data(init_dataset):
    session = init_dataset
    import_sierra_data("tests/files/bpl-ere-export-sample.txt", session)

    assert len(session.query(Resource).all()) == 4
    assert len(session.query(ExportFile).all()) == 1


@pytest.mark.parametrize(
    "lib_sys,bib_cat,expectation",
    [
        ("nyp", "ere", [22259002, 22259003]),
        ("nyp", "pre", [12345670, 12345671]),
        ("bpl", "ere", [22345678, 22345679]),
        ("bpl", "pre", [22345670, 22345671]),
    ],
)
def test_retrieve_bibnos_for_enhancement(
    lib_sys, bib_cat, expectation, brief_bib_dataset
):
    session = brief_bib_dataset
    assert retrieve_bibnos_for_enhancement(lib_sys, bib_cat, session) == expectation


def test_import_platform_data(
    brief_bib_dataset,
    mock_keys,
    mock_successful_platform_post_token_response,
    mock_successful_platform_session_get_request,
):
    session = brief_bib_dataset
    bibnos = [22259002, 22259003, 19099433]
    import_platform_data(bibnos, session)
    rec1 = session.query(Resource).filter_by(sbid=bibnos[0], librarySystemId=1).one()
    rec2 = session.query(Resource).filter_by(sbid=bibnos[1], librarySystemId=1).one()
    rec3 = session.query(Resource).filter_by(sbid=bibnos[2], librarySystemId=1).one()

    # record 1
    assert rec1.bibDate == datetime.date(2020, 9, 30)
    assert rec1.sbn == "9781631491719,1631491719"
    assert rec1.lcn == "2017022370"
    assert rec1.did == "40CC3B3F-4C30-4685-B391-DB7B2EA91455"
    assert rec1.sid == "1111111111,2222222222"
    assert rec1.wcn is None
    assert rec1.deleted is False
    assert rec1.title == "saddest words electronic resource william faulkners civil war"
    assert rec1.pubDate == "2020"
    assert rec1.author == "gorra michael"
    assert rec1.upgradeStamp is None
    assert rec1.upgraded is False
    assert rec1.upgradeSourceId is None
    assert (
        str(rec1.urls)
        == "[<UrlField(ufid=1, sBibId=22259002, librarySystemId=1, uTypeId=1, url='http://link.overdrive.com/?content')>, <UrlField(ufid=2, sBibId=22259002, librarySystemId=1, uTypeId=2, url='https://samples.overdrive.com/?sample_url')>, <UrlField(ufid=3, sBibId=22259002, librarySystemId=1, uTypeId=3, url='https://img1.od-cdn.com/ImageType-100/image.jpg')>, <UrlField(ufid=4, sBibId=22259002, librarySystemId=1, uTypeId=4, url='https://img1.od-cdn.com/ImageType-200/thumbnail.jpg')>]"
    )
    assert rec1.wqueries == []

    # record 2
    assert rec2.bibDate == datetime.date(2020, 9, 30)
    assert rec2.sbn == "9780307804525"
    assert rec2.lcn is None
    assert rec2.did == "4E7547BF-6D42-43F4-9180-8FCF302497F3"
    assert rec2.sid is None
    assert rec2.wcn is None
    assert rec2.deleted is False
    assert (
        rec2.title
        == "passion of the western mind electronic resource understanding the ideas that have shaped our world view"
    )
    assert rec2.pubDate == "2011"
    assert rec2.author == "tarnas richard"
    assert rec2.upgradeStamp is None
    assert rec2.upgraded is False
    assert rec2.upgradeSourceId is None
    assert (
        str(rec2.urls)
        == "[<UrlField(ufid=5, sBibId=22259003, librarySystemId=1, uTypeId=1, url='http://link.overdrive.com/?websiteID=37&titleID=647341')>, <UrlField(ufid=6, sBibId=22259003, librarySystemId=1, uTypeId=2, url='https://samples.overdrive.com/?crid=4E7547BF-6D42-43F4-9180-8FCF302497F3&.epub-sample.overdrive.com')>, <UrlField(ufid=7, sBibId=22259003, librarySystemId=1, uTypeId=3, url='https://img1.od-cdn.com/ImageType-100/0111-1/%7B4E7547BF-6D42-43F4-9180-8FCF302497F3%7DImg100.jpg')>, <UrlField(ufid=8, sBibId=22259003, librarySystemId=1, uTypeId=4, url='https://img1.od-cdn.com/ImageType-200/0111-1/%7B4E7547BF-6D42-43F4-9180-8FCF302497F3%7DImg200.jpg')>]"
    )
    assert rec2.wqueries == []

    # record 3 (deleted)
    assert rec3.sbid == 19099433
    assert rec3.bibDate == datetime.date(2020, 9, 30)
    assert rec3.sbn is None
    assert rec3.lcn is None
    assert rec3.did is None
    assert rec3.sid is None
    assert rec3.wcn is None
    assert rec3.deleted is True
    assert rec3.title is None
    assert rec3.pubDate is None
    assert rec3.author is None
    assert rec3.upgradeStamp is None
    assert rec3.upgraded is False
    assert rec3.upgradeSourceId is None
    assert rec3.urls == []
    assert rec3.wqueries == []


def test_retrieve_eresource_records_for_worldcat_queries(
    mixed_dataset,
    mock_date_today,
    mock_keys,
    mock_successful_platform_post_token_response,
    mock_successful_platform_session_get_request,
):
    session = mixed_dataset
    records = retrieve_eresource_records_for_worldcat_queries("nyp", session)

    exp1 = (1, "reserve-id-1")  # never queried
    exp2 = (2, "reserve-id-2")  # month old not queried in the last week
    exp3 = (5, "reserve-id-5")  # 2-5 months old not queried in the last month
    records = [x for x in records]
    assert records[0] == exp1
    assert records[1] == exp2
    assert records[2] == exp3
