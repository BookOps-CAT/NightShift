import datetime
import os
from io import BytesIO

import pytest
import yaml
from sqlalchemy.exc import IntegrityError

from nightshift import datastore_transactions, manager
from nightshift.comms.storage import Drive
from nightshift.datastore import WorldcatQuery
from nightshift.tasks import Tasks


@pytest.fixture
def mock_init_db(monkeypatch, db_provider):
    def mock_db_provider(*args, **kwargs):
        return db_provider

    def mock_init(*args, **kwargs):
        pass

    monkeypatch.setattr(datastore_transactions, "create_db_provider", mock_db_provider)
    monkeypatch.setattr(datastore_transactions, "init_db", mock_init)


@pytest.fixture
def mock_envar_yaml(monkeypatch, mock_db_env):
    def mock_data(*args, **kwargs):
        return {}

    monkeypatch.setattr(yaml, "safe_load", mock_data)


@pytest.fixture
def mock_db_integrity_error(monkeypatch, mock_init_db):
    def mock_integrity_error(*args, **kwargs):
        raise IntegrityError("err", "params", "orig")

    monkeypatch.setattr(datastore_transactions, "init_db", mock_integrity_error)


@pytest.fixture
def mock_manager(monkeypatch):
    def null_return(*args, **kwargs):
        pass

    monkeypatch.setattr(manager, "process_resources", null_return)
    monkeypatch.setattr(manager, "perform_db_maintenance", null_return)


@pytest.fixture
def mock_drive_unprocessed_files(monkeypatch):
    """Mocks workflow only for NYP."""

    def _files(*args):
        if args[0].library == "NYP":
            return ["NYP-bar-pout.mrc"]
        return []

    monkeypatch.setattr(Tasks, "isolate_unprocessed_files", _files)


@pytest.fixture
def mock_drive_unprocessed_files_empty(monkeypatch):
    def _files(*args):
        return []

    monkeypatch.setattr(Tasks, "isolate_unprocessed_files", _files)


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
        task_instance = args[0]
        resources = args[1]
        with task_instance.db_provider.session_scope() as session:
            n = 0
            for res in resources:
                n += 1
                instance = datastore_transactions.update_resource(
                    session, res.sierraId, res.libraryId, oclcMatchNumber=str(n)
                )
                instance.queries.append(WorldcatQuery(resourceId=res.nid, match=True))

    monkeypatch.setattr(Tasks, "get_worldcat_brief_bib_matches", _patch)


@pytest.fixture
def mock_check_resources_sierra_state_open(monkeypatch):
    def _patch(*args):
        task_instance = args[0]
        resources = args[1]
        with task_instance.db_provider.session_scope() as session:
            for res in resources:
                res.suppressed = False
                res.status = "open"
                datastore_transactions.update_resource(
                    session,
                    res.sierraId,
                    res.libraryId,
                    suppressed=False,
                    status="open",
                )

    monkeypatch.setattr(Tasks, "check_resources_sierra_state", _patch)


@pytest.fixture
def mock_get_worldcat_full_bibs(monkeypatch):
    def _patch(*args):
        task_instance = args[0]
        resources = args[1]
        fullBib = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n<entry xmlns=\"http://www.w3.org/2005/Atom\">\n"
            b'  <content type="application/xml">\n    <response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n'
            b'      <record xmlns="http://www.loc.gov/MARC21/slim">\n        <leader>00000cam a2200000Ia 4500</leader>\n'
            b'        <controlfield tag="001">ocn850939580</controlfield>\n        <controlfield tag="003">OCoLC</controlfield>\n'
            b'        <controlfield tag="005">20190426152409.0</controlfield>\n        <controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n'
            b'        <datafield tag="040" ind1=" " ind2=" ">\n          <subfield code="a">OCPSB</subfield>\n          <subfield code="b">eng</subfield>\n'
            b'          <subfield code="c">OCPSB</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n'
            b'          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">NYP</subfield>\n'
            b'        </datafield>\n        <datafield tag="035" ind1=" " ind2=" ">\n          <subfield code="a">(OCoLC)850939580</subfield>\n'
            b'        </datafield>\n        <datafield tag="020" ind1=" " ind2=" ">\n          <subfield code="a">some isbn</subfield>\n'
            b'        </datafield>\n        <datafield tag="049" ind1=" " ind2=" ">\n          <subfield code="a">NYPP</subfield>\n'
            b'        </datafield>\n        <datafield tag="100" ind1="0" ind2=" ">\n          <subfield code="a">OCLC RecordBuilder.</subfield>\n'
            b'        </datafield>\n        <datafield tag="245" ind1="1" ind2="0">\n          <subfield code="a">Record Builder Added This Test Record</subfield>\n'
            b'          <subfield code="c">spam.</subfield>\n        </datafield>\n        <datafield tag="300" ind1=" " ind2=" ">\n'
            b'          <subfield code="a">1 online resource</subfield>\n        </datafield>\n        <datafield tag="336" ind1=" " ind2=" ">\n'
            b'          <subfield code="a">text</subfield>\n          <subfield code="b">txt</subfield>\n          <subfield code="2">rdacontent</subfield>\n'
            b'        </datafield>\n        <datafield tag="337" ind1=" " ind2=" ">\n          <subfield code="a">unmediated</subfield>\n'
            b'          <subfield code="b">n</subfield>\n          <subfield code="2">rdamedia</subfield>\n        </datafield>\n'
            b'        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">TEST RECORD -- DO NOT USE.</subfield>\n        </datafield>\n'
            b'        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">Added Field by MarcEdit.</subfield>\n        </datafield>\n'
            b'        <datafield tag="650" ind1=" " ind2="0">\n          <subfield code="a">Test.</subfield>\n        </datafield>\n'
            b'      </record>\n    </response>\n  </content>\n  <id>http://worldcat.org/oclc/850939580</id>\n  <link href="http://worldcat.org/oclc/850939580"/>\n</entry>'
        )
        with task_instance.db_provider.session_scope() as session:
            for res in resources:
                datastore_transactions.update_resource(
                    session, res.sierraId, res.libraryId, fullBib=fullBib
                )

    monkeypatch.setattr(Tasks, "get_worldcat_full_bibs", _patch)


@pytest.fixture
def mock_transfer_to_drive(monkeypatch):
    def _patch(*args):
        today = datetime.datetime.now().date()
        library = args[0].library
        res_cat = args[1]
        return f"{today:%y%m%d}-{library}-{res_cat}.mrc"

    monkeypatch.setattr(Tasks, "transfer_to_drive", _patch)

    yield

    try:
        os.remove("temp.mrc")
    except OSError:
        pass
