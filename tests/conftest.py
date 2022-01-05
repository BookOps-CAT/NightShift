# -*- coding: utf-8 -*-
import datetime
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml


from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import Base, Library, Resource, ResourceCategory, SourceFile


class MockIOError:
    def __init__(self, *args, **kwargs):
        raise IOError


@pytest.fixture
def mock_io_error(monkeypatch):
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.put", MockIOError)
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.listdir", MockIOError)
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.file", MockIOError)


@pytest.fixture
def local_test_config():
    """
    Requires yaml file with local logging configuration
    example:
    ---
    NS_DBHOST: localhost
    NS_DBUSER: ns_test
    NS_DBPASSW: some_password
    NS_DBPORT: 5432
    NS_DBNAME: ns_db
    WCNYP_KEY: nypl_worldcat_key
    WCNYP_SECRET: nypl_worldcat_secret
    WCNYP_PRINCIPALID: nypl_principal_id
    WCNYP_PRINCIPALIDNS: nypl_principal_idns
    WCBPL_KEY: bpl_worldcat_key
    WCBPL_SECRET: bpl_worlcat_secret
    WCBPL_PRINCIPALID: bpl_worlcat_principal_id
    WCBPL_PRINCIPALIDNS: bpl_worlcat_principal_idns
    LOGGLY_TOKEN: loggly_token
    LOG_HANDLERS: "loggly,file,console"
    SFTP_HOST: sftp_host
    SFTP_USER: sftp_user
    SFTP_PASSW: sfpt_password
    SFTP_NS_SRC: "/NSDROP/TEST/sierra_dumps/nightshift"
    SFTP_NS_DST:  "/NSDROP/TEST/load"
    NYPL_PLATFORM_CLIENT: nypl_platform_client
    NYPL_PLATFORM_SECRET: nypl_platform_secret
    NYPL_PLATFORM_OAUTH: nypl_platform_oauth_server
    NYPL_PLATFORM_ENV: prod
    BPL_SOLR_CLIENT_KEY: bpl_solr_client_key
    BPL_SOLR_ENDPOINT: bpl_solr_endpoint
    """
    with open("tests/envar.yaml", "r") as f:
        data = yaml.safe_load(f)
        return data


@pytest.fixture
def stub_resource():
    return Resource(
        sierraId=11111111,
        libraryId=1,
        resourceCategoryId=1,
        sourceId=1,
        bibDate=datetime.datetime.utcnow().date() - datetime.timedelta(days=31),
        title="TITLE 1",
        status="open",
        fullBib=b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<entry xmlns="http://www.w3.org/2005/Atom">\n  <content type="application/xml">\n    <response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n      <record xmlns="http://www.loc.gov/MARC21/slim">\n        <leader>00000cam a2200000Ia 4500</leader>\n        <controlfield tag="001">ocn850939580</controlfield>\n        <controlfield tag="003">OCoLC</controlfield>\n        <controlfield tag="005">20190426152409.0</controlfield>\n        <controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n        <datafield tag="040" ind1=" " ind2=" ">\n          <subfield code="a">OCPSB</subfield>\n          <subfield code="b">eng</subfield>\n          <subfield code="c">OCPSB</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">NYP</subfield>\n    </datafield>\n        <datafield tag="035" ind1=" " ind2=" ">\n          <subfield code="a">(OCoLC)850939580</subfield>\n    </datafield>\n        <datafield tag="020" ind1=" " ind2=" ">\n          <subfield code="a">some isbn</subfield>\n    </datafield>\n        <datafield tag="049" ind1=" " ind2=" ">\n          <subfield code="a">NYPP</subfield>\n    </datafield>\n        <datafield tag="100" ind1="0" ind2=" ">\n          <subfield code="a">OCLC RecordBuilder.</subfield>\n    </datafield>\n        <datafield tag="245" ind1="1" ind2="0">\n          <subfield code="a">Record Builder Added This Test Record On 06/26/2013 13:06:26.</subfield>\n    </datafield>\n        <datafield tag="336" ind1=" " ind2=" ">\n          <subfield code="a">text</subfield>\n          <subfield code="b">txt</subfield>\n          <subfield code="2">rdacontent</subfield>\n    </datafield>\n        <datafield tag="337" ind1=" " ind2=" ">\n          <subfield code="a">unmediated</subfield>\n          <subfield code="b">n</subfield>\n          <subfield code="2">rdamedia</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">TEST RECORD -- DO NOT USE.</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">Added Field by MarcEdit.</subfield>\n    </datafield>\n  </record>\n    </response>\n  </content>\n  <id>http://worldcat.org/oclc/850939580</id>\n  <link href="http://worldcat.org/oclc/850939580"/>\n</entry>',
        oclcMatchNumber="850939580",
    )


@pytest.fixture(scope="function")
def mock_db_env(monkeypatch, local_test_config):
    if os.getenv("TRAVIS"):
        data = dict(
            NS_DBUSER="postgres",
            NS_DBPASSW="",
            NS_DBHOST="127.0.0.1",
            NS_DBPORT="5433",
            NS_DBNAME="ns_db",
        )
    else:
        data = local_test_config

    monkeypatch.setenv("NS_DBUSER", data["NS_DBUSER"])
    monkeypatch.setenv("NS_DBPASSW", data["NS_DBPASSW"])
    monkeypatch.setenv("NS_DBHOST", data["NS_DBHOST"])
    monkeypatch.setenv("NS_DBPORT", data["NS_DBPORT"])
    monkeypatch.setenv("NS_DBNAME", data["NS_DBNAME"])


@pytest.fixture(scope="function")
def test_connection(mock_db_env):
    # create db engine differently on local machine or Travis
    if os.getenv("TRAVIS"):
        conn = f"postgresql://{os.getenv('NS_DBUSER')}@{os.getenv('NS_DBHOST')}:{os.getenv('NS_DBPORT')}/{os.getenv('NS_DBNAME')}"
    else:
        conn = f"postgresql://{os.getenv('NS_DBUSER')}:{os.getenv('NS_DBPASSW')}@{os.getenv('NS_DBHOST')}:{os.getenv('NS_DBPORT')}/{os.getenv('NS_DBNAME')}"
    return conn


@pytest.fixture
def test_session(test_connection):
    engine = create_engine(test_connection)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session
    session.close()

    # teardown
    Base.metadata.drop_all(engine)


@pytest.fixture
def test_data_core(test_session):
    for k, v in LIBRARIES.items():
        test_session.add(Library(nid=v["nid"], code=k))
    for k, v in RESOURCE_CATEGORIES.items():
        test_session.add(
            ResourceCategory(nid=v["nid"], name=k, description=v["description"])
        )
    test_session.add(SourceFile(libraryId=1, handle="foo1.mrc"))
    test_session.add(SourceFile(libraryId=2, handle="foo2.mrc"))
    test_session.commit()
