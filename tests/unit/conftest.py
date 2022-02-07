# -*- coding: utf-8 -*-
from datetime import datetime
from io import BytesIO
import os

from bookops_marc import Bib
from pymarc import Field
import pytest
from sqlalchemy.exc import IntegrityError

from nightshift import bot, datastore_transactions, manager, tasks, constants
from nightshift.comms.storage import Drive
from nightshift.datastore import OutputFile, WorldcatQuery
from nightshift.marc.marc_parser import BibReader


class MockOSError:
    def __init__(self, *args, **kwargs):
        raise OSError


@pytest.fixture
def mock_os_error(monkeypatch):
    monkeypatch.setattr("builtins.open", MockOSError)


@pytest.fixture
def mock_os_error_on_remove(monkeypatch):
    monkeypatch.setattr("os.remove", MockOSError)


@pytest.fixture
def mock_log_env(monkeypatch):
    monkeypatch.setenv("LOGGLY_TOKEN", "ns_token_here")
    monkeypatch.setenv("LOG_HANDLERS", "console,file,loggly")


@pytest.fixture(scope="function")
def test_log(monkeypatch, local_test_config):
    if not os.getenv("TRAVIS"):
        monkeypatch.setenv("LOGGLY_TOKEN", local_test_config["LOGGLY_TOKEN"])
        monkeypatch.setenv("LOG_HANDLERS", local_test_config["LOG_HANDLERS"])


@pytest.fixture
def patch_config_local_env_variables(monkeypatch):
    def _patch(*args, **kwargs):
        return

    monkeypatch.setattr(bot, "config_local_env_variables", _patch)


# DB fixtures ############


@pytest.fixture
def test_data_rich(stub_resource, test_session, test_data_core):
    test_session.add(OutputFile(libraryId=1, handle="spam.mrc"))
    test_session.commit()

    test_session.add(stub_resource)
    test_session.commit()


@pytest.fixture
def patch_init_db(monkeypatch):
    def _patch(*args, **kwargs):
        return

    monkeypatch.setattr(datastore_transactions, "init_db", _patch)


@pytest.fixture
def mock_init_db_integrity_error(monkeypatch):
    def _patch(*args, **kwargs):
        raise IntegrityError("err", "params", "orig")

    monkeypatch.setattr(datastore_transactions, "init_db", _patch)


@pytest.fixture
def mock_init_db_value_error(monkeypatch):
    def _patch(*args, **kwargs):
        raise ValueError

    monkeypatch.setattr(datastore_transactions, "init_db", _patch)


@pytest.fixture
def mock_init_libraries(monkeypatch):
    monkeypatch.setattr(constants, "LIBRARIES", {"NYP": {"nid": 1}})


# Bibs fixtures ##############


@pytest.fixture
def stub_marc():
    bib = Bib()
    bib.leader = "02866pam  2200517 i 4500"
    bib.add_field(Field(tag="008", data="190306s2017    ht a   j      000 1 hat d"))
    bib.add_field(
        Field(
            tag="100",
            indicators=["1", " "],
            subfields=["a", "Adams, John,", "e", "author."],
        )
    )
    bib.add_field(
        Field(
            tag="245",
            indicators=["1", "4"],
            subfields=["a", "The foo /", "c", "by John Adams."],
        )
    )
    bib.add_field(
        Field(
            tag="264",
            indicators=[" ", "1"],
            subfields=["a", "Bar :", "b", "New York,", "c", "2021"],
        )
    )
    bib.add_field(
        Field(
            tag="907",
            indicators=[" ", " "],
            subfields=["a", ".b22222222x", "b", "07-01-21", "c", "07-01-2021 19:07"],
        )
    )

    return bib


@pytest.fixture
def fake_BibReader():
    return BibReader(BytesIO(b"some records"), "NYP")


@pytest.fixture
def patch_process_resources(monkeypatch):
    def _patch(*args, **kwargs):
        return

    monkeypatch.setattr(manager, "process_resources", _patch)


@pytest.fixture
def patch_perform_db_maintenance(monkeypatch):
    def _patch(*args, **kwargs):
        return

    monkeypatch.setattr(manager, "perform_db_maintenance", _patch)


@pytest.fixture
def mock_drive_unprocessed_files(monkeypatch):
    """
    mocks workflow only for NYP
    """

    def _files(*args):
        if args[2] == "NYP":
            return ["NYP-bar.pout"]
        else:
            return []

    monkeypatch.setattr(tasks, "isolate_unprocessed_files", _files)


@pytest.fixture
def mock_drive_unprocessed_files_empty(monkeypatch):
    """
    mocks workflow only for NYP
    """

    def _files(*args):
        if args[2] == "NYP":
            return []
        else:
            return []

    monkeypatch.setattr(tasks, "isolate_unprocessed_files", _files)


@pytest.fixture
def mock_drive_fetch_file(monkeypatch):
    def _fetch(*args):
        with open("tests/nyp-ebook-sample.mrc", "rb") as test_file:
            data = test_file.read()
            marc_target = BytesIO(data)
            return marc_target

    monkeypatch.setattr(Drive, "fetch_file", _fetch)


@pytest.fixture
def mock_worldcat_brief_bib_matches(monkeypatch):
    def _patch(*args):
        session = args[0]
        resources = args[2]
        n = 0
        for res in resources:
            n += 1
            instance = datastore_transactions.update_resource(
                session, res.sierraId, res.libraryId, oclcMatchNumber=str(n)
            )
            instance.queries.append(WorldcatQuery(resourceId=res.nid, match=True))
        session.commit()

    monkeypatch.setattr(tasks, "get_worldcat_brief_bib_matches", _patch)


@pytest.fixture
def mock_check_resources_sierra_state_open(monkeypatch):
    def _patch(*args):
        session = args[0]
        resources = args[2]
        for res in resources:
            res.suppressed = False
            res.status = "open"
        session.commit()

    monkeypatch.setattr(tasks, "check_resources_sierra_state", _patch)


@pytest.fixture
def mock_get_worldcat_full_bibs(monkeypatch):
    def _patch(*args):
        session = args[0]
        resources = args[2]
        for res in resources:
            datastore_transactions.update_resource(
                session,
                res.sierraId,
                res.libraryId,
                fullBib=b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<entry xmlns="http://www.w3.org/2005/Atom">\n  <content type="application/xml">\n    <response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n      <record xmlns="http://www.loc.gov/MARC21/slim">\n        <leader>00000cam a2200000Ia 4500</leader>\n        <controlfield tag="001">ocn850939580</controlfield>\n        <controlfield tag="003">OCoLC</controlfield>\n        <controlfield tag="005">20190426152409.0</controlfield>\n        <controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n        <datafield tag="040" ind1=" " ind2=" ">\n          <subfield code="a">OCPSB</subfield>\n          <subfield code="b">eng</subfield>\n          <subfield code="c">OCPSB</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">NYP</subfield>\n    </datafield>\n        <datafield tag="035" ind1=" " ind2=" ">\n          <subfield code="a">(OCoLC)850939580</subfield>\n    </datafield>\n        <datafield tag="020" ind1=" " ind2=" ">\n          <subfield code="a">some isbn</subfield>\n    </datafield>\n        <datafield tag="049" ind1=" " ind2=" ">\n          <subfield code="a">NYPP</subfield>\n    </datafield>\n        <datafield tag="100" ind1="0" ind2=" ">\n          <subfield code="a">OCLC RecordBuilder.</subfield>\n    </datafield>\n        <datafield tag="245" ind1="1" ind2="0">\n          <subfield code="a">Record Builder Added This Test Record On 06/26/2013 13:06:26.</subfield>\n    </datafield>\n        <datafield tag="336" ind1=" " ind2=" ">\n          <subfield code="a">text</subfield>\n          <subfield code="b">txt</subfield>\n          <subfield code="2">rdacontent</subfield>\n    </datafield>\n        <datafield tag="337" ind1=" " ind2=" ">\n          <subfield code="a">unmediated</subfield>\n          <subfield code="b">n</subfield>\n          <subfield code="2">rdamedia</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">TEST RECORD -- DO NOT USE.</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">Added Field by MarcEdit.</subfield>\n    </datafield>\n  </record>\n    </response>\n  </content>\n  <id>http://worldcat.org/oclc/850939580</id>\n  <link href="http://worldcat.org/oclc/850939580"/>\n</entry>',
            )
        session.commit()

    monkeypatch.setattr(tasks, "get_worldcat_full_bibs", _patch)


@pytest.fixture
def mock_transfer_to_drive(monkeypatch):
    def _patch(*args):
        today = datetime.now().date()
        library = args[0]
        res_cat = args[1]
        return f"{today:%y%m%d}-{library}-{res_cat}.mrc"

    monkeypatch.setattr(tasks, "transfer_to_drive", _patch)

    yield

    try:
        os.remove("temp.mrc")
    except:
        pass
