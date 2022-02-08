# -*- coding: utf-8 -*-
import datetime
import os

import paramiko
import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatSessionError
import yaml


from nightshift.comms.storage import get_credentials, Drive
from nightshift.comms.worldcat import Worldcat
from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import (
    Base,
    Library,
    OutputFile,
    Resource,
    ResourceCategory,
    SourceFile,
    WorldcatQuery,
)


class FakeUtcNow(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return cls(2021, 1, 1, 17, 0, 0, 0)


@pytest.fixture
def mock_utcnow(monkeypatch):
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)


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
    POSTGRES_HOST: postgres_host
    POSTGRES_USER: postgres_user
    POSTGRES_PASSWORD: postgres_password
    POSTGRES_PORT: postgres_port
    POSTGRES_DB: postgred_db_name
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
def env_var(monkeypatch):
    if os.getenv("GITHUB_ACTIONS"):
        data = dict(
            POSTGRES_HOST="127.0.0.1",
            POSTGRES_USER="postgres",
            POSTGRES_PASSWORD="postgres",
            POSTGRES_PORT="5432",
            POSTGRES_DB="ns_db",
        )
    else:
        # local and firewalled tests
        with open("tests/envar.yaml", "r") as f:
            data = yaml.safe_load(f)

    for k, v in data.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def stub_resource():
    return Resource(
        sierraId=11111111,
        libraryId=1,
        resourceCategoryId=1,
        sourceId=1,
        bibDate=datetime.datetime.utcnow().date() - datetime.timedelta(days=31),
        title="TITLE 1",
        status="bot_enhanced",
        fullBib=b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<entry xmlns="http://www.w3.org/2005/Atom">\n  <content type="application/xml">\n    <response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n      <record xmlns="http://www.loc.gov/MARC21/slim">\n        <leader>00000cam a2200000Ia 4500</leader>\n        <controlfield tag="001">ocn850939580</controlfield>\n        <controlfield tag="003">OCoLC</controlfield>\n        <controlfield tag="005">20190426152409.0</controlfield>\n        <controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n        <datafield tag="040" ind1=" " ind2=" ">\n          <subfield code="a">OCPSB</subfield>\n          <subfield code="b">eng</subfield>\n          <subfield code="c">OCPSB</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">NYP</subfield>\n    </datafield>\n        <datafield tag="035" ind1=" " ind2=" ">\n          <subfield code="a">(OCoLC)850939580</subfield>\n    </datafield>\n        <datafield tag="020" ind1=" " ind2=" ">\n          <subfield code="a">some isbn</subfield>\n    </datafield>\n        <datafield tag="049" ind1=" " ind2=" ">\n          <subfield code="a">NYPP</subfield>\n    </datafield>\n        <datafield tag="100" ind1="0" ind2=" ">\n          <subfield code="a">OCLC RecordBuilder.</subfield>\n    </datafield>\n        <datafield tag="245" ind1="1" ind2="0">\n          <subfield code="a">Record Builder Added This Test Record On 06/26/2013 13:06:26.</subfield>\n    </datafield>\n        <datafield tag="336" ind1=" " ind2=" ">\n          <subfield code="a">text</subfield>\n          <subfield code="b">txt</subfield>\n          <subfield code="2">rdacontent</subfield>\n    </datafield>\n        <datafield tag="337" ind1=" " ind2=" ">\n          <subfield code="a">unmediated</subfield>\n          <subfield code="b">n</subfield>\n          <subfield code="2">rdamedia</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">TEST RECORD -- DO NOT USE.</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">Added Field by MarcEdit.</subfield>\n    </datafield>\n  </record>\n    </response>\n  </content>\n  <id>http://worldcat.org/oclc/850939580</id>\n  <link href="http://worldcat.org/oclc/850939580"/>\n</entry>',
        oclcMatchNumber="850939580",
        enhanceTimestamp=datetime.datetime.utcnow().date()
        - datetime.timedelta(days=15),
        queries=[WorldcatQuery(match=True)],
        outputId=1,
    )


@pytest.fixture(scope="function")
def mock_db_env(monkeypatch):
    if os.getenv("GITHUB_ACTIONS"):
        data = dict(
            POSTGRES_HOST="127.0.0.1",
            POSTGRES_USER="postgres",
            POSTGRES_PASSWORD="postgres",
            POSTGRES_PORT="5432",
            POSTGRES_DB="ns_db",
        )
    else:
        with open("tests/envar.yaml", "r") as f:
            data = yaml.safe_load(f)

    monkeypatch.setenv("POSTGRES_USER", data["POSTGRES_USER"])
    monkeypatch.setenv("POSTGRES_PASSWORD", data["POSTGRES_PASSWORD"])
    monkeypatch.setenv("POSTGRES_HOST", data["POSTGRES_HOST"])
    monkeypatch.setenv("POSTGRES_PORT", data["POSTGRES_PORT"])
    monkeypatch.setenv("POSTGRES_DB", data["POSTGRES_DB"])


@pytest.fixture(scope="function")
def test_connection(mock_db_env):
    # create db engine differently on local machine or Github Actions
    conn = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
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


@pytest.fixture
def test_data_rich(stub_resource, test_session, test_data_core):
    test_session.add(OutputFile(libraryId=1, handle="spam.mrc"))
    test_session.commit()

    test_session.add(stub_resource)
    test_session.commit()


# SFTP / newtowrked drive #############


@pytest.fixture
def mock_sftp_env(monkeypatch, sftpserver):
    monkeypatch.setenv("SFTP_HOST", sftpserver.host)
    monkeypatch.setenv("SFTP_PORT", str(sftpserver.port))
    monkeypatch.setenv("SFTP_USER", "nightshift")
    monkeypatch.setenv("SFTP_PASSW", "sftp_password")
    monkeypatch.setenv("SFTP_NS_SRC", "sierra_dumps_dir")
    monkeypatch.setenv("SFTP_NS_DST", "load_dir")


class MockSSHException:
    def __init__(self, *args, **kwargs):
        raise paramiko.ssh_exception.SSHException


@pytest.fixture
def mock_ssh_exception(monkeypatch):
    monkeypatch.setattr("paramiko.transport.Transport.connect", MockSSHException)


@pytest.fixture
def mock_drive(mock_sftp_env):
    creds = get_credentials()
    with Drive(*creds) as drive:
        yield drive


# Worldcat fixtures ########


class MockAuthServerResponseFailure:
    """Simulates auth server response to failed token request"""

    def __init__(self):
        self.status_code = 401

    def json(self):
        return {
            "code": 401,
            "message": "Basic Authorization Header - Missing or Invalid WSKey and/or Secret",
        }


class MockAuthServerResponseSuccess:
    """Simulates auth server response to successful token request"""

    def __init__(self):
        self.status_code = 200

    def json(self):
        expires_at = datetime.datetime.strftime(
            datetime.datetime.utcnow() + datetime.timedelta(0, 1199),
            "%Y-%m-%d %H:%M:%SZ",
        )

        return {
            "access_token": "tk_Yebz4BpEp9dAsghA7KpWx6dYD1OZKWBlHjqW",
            "token_type": "bearer",
            "expires_in": "1199",
            "principalID": "",
            "principalIDNS": "",
            "scopes": "scope1",
            "contextInstitutionId": "00001",
            "expires_at": expires_at,
        }


class MockSuccessfulHTTP200SessionResponse:
    def __init__(self):
        self.status_code = 200
        self.content = b"some content here"
        self.url = "request_url_here"

    def json(self):
        return {
            "numberOfRecords": 1,
            "briefRecords": [
                {
                    "oclcNumber": "44959645",
                    "title": "Pride and prejudice.",
                    "creator": "Jane Austen",
                    "date": "199u",
                    "language": "eng",
                    "generalFormat": "Book",
                    "specificFormat": "Digital",
                    "publisher": "Project Gutenberg",
                    "publicationPlace": "Champaign, Ill.",
                    "isbns": [
                        "9780585013367",
                    ],
                    "mergedOclcNumbers": ["818363152"],
                    "catalogingInfo": {
                        "catalogingAgency": "DLC",
                        "transcribingAgency": "DLC",
                        "catalogingLanguage": "eng",
                        "levelOfCataloging": "8",
                    },
                }
            ],
        }


class MockSuccessfulHTTP200SessionResponseNoMatches:
    def __init__(self):
        self.status_code = 200
        self.url = "request_url_here"

    def json(self):
        return {
            "numberOfRecords": 0,
        }


class MockSessionError:
    def __init__(self, *args, **kwargs):
        raise WorldcatSessionError("Timeout error")


@pytest.fixture
def mock_worldcat_creds(monkeypatch):
    for lib in ("NYP", "BPL"):
        monkeypatch.setenv(f"WC{lib}_KEY", "lib_key")
        monkeypatch.setenv(f"WC{lib}_SECRET", "lib_secret")
        monkeypatch.setenv(f"WC{lib}_SCOPE", "WorldCatMetadataAPI")
        monkeypatch.setenv(f"WC{lib}_PRINCIPALID", "lib_principal_id")
        monkeypatch.setenv(f"WC{lib}_PRINCIPALIDNS", "lib_principal_idns")


@pytest.fixture
def mock_failed_post_token_response(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockAuthServerResponseFailure()

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_successful_post_token_response(mock_utcnow, monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockAuthServerResponseSuccess()

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_successful_session_get_request(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockSuccessfulHTTP200SessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_successful_session_get_request_no_matches(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockSuccessfulHTTP200SessionResponseNoMatches()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_session_error(monkeypatch):
    monkeypatch.setattr("requests.Session.get", MockSessionError)


@pytest.fixture
def mock_token(mock_worldcat_creds, mock_successful_post_token_response):
    mock_credentials = dict(
        key=os.getenv("WCNYP_KEY"),
        secret=os.getenv("WCNYP_SECRET"),
        scopes="WorldCatMetadataAPI",
        principal_id=os.getenv("WCNYP_PRINCIPALID"),
        principal_idns=os.getenv("WCNYP_PRINCIPALIDNS"),
    )
    return WorldcatAccessToken(**mock_credentials)


@pytest.fixture
def mock_worldcat_session(mock_token):
    with MetadataSession(authorization=mock_token) as session:
        yield session


@pytest.fixture
def mock_Worldcat(mock_worldcat_creds, mock_successful_post_token_response):
    return Worldcat("NYP")


# NYPL Platform & BPL Solr fixtures ###


class MockSearchSessionHTTPError:
    """Mocks 500 HTTP error responses for both platforms"""

    def __init__(self, code=500):
        self.status_code = code
        self.url = "request_url_here"

    def json(self):
        return {
            "statusCode": self.status_code,
            "type": "exception",
            "message": "error message",
            "error": [],
            "debugInfo": [],
        }


class MockPlatformAuthServerResponseSuccess:
    """Simulates oauth server response to successful token request"""

    def __init__(self):
        self.status_code = 200

    def json(self):
        return {
            "access_token": "token_string_here",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "scopes_here",
            "id_token": "token_string_here",
        }


class MockPlatformAuthServerResponseFailure:
    """Simulates oauth server response to successful token request"""

    def __init__(self):
        self.status_code = 400

    def json(self):
        return {"error": "No grant_type specified", "error_description": None}


class MockPlatformSessionResponseSuccess:
    """Simulates NYPL Platform query successful response"""

    def __init__(self):
        self.status_code = 200
        self.url = "request_url_here"

    def json(self):
        return {
            "data": {
                "id": "18578797",
                "deleted": False,
                "suppressed": False,
                "title": "Zendegi",
                "author": "Egan, Greg, 1961-",
                "standardNumbers": ["9781597801744", "1597801747"],
                "controlNumber": "2010074825",
                "fixedFields": {
                    "24": {"label": "Language", "value": "eng", "display": "English"},
                    "107": {"label": "MARC Type", "value": " ", "display": None},
                },
                "varFields": [
                    {
                        "fieldTag": "c",
                        "marcTag": "091",
                        "ind1": " ",
                        "ind2": " ",
                        "content": None,
                        "subfields": [
                            {"tag": "a", "content": "SCI-FI"},
                            {"tag": "c", "content": "EGAN"},
                        ],
                    },
                    {
                        "fieldTag": "o",
                        "marcTag": "001",
                        "ind1": " ",
                        "ind2": " ",
                        "content": "2010074825",
                        "subfields": None,
                    },
                    {
                        "fieldTag": "t",
                        "marcTag": "245",
                        "ind1": "1",
                        "ind2": "0",
                        "content": None,
                        "subfields": [
                            {"tag": "a", "content": "Zendegi /"},
                            {"tag": "c", "content": "Greg Egan."},
                        ],
                    },
                    {
                        "fieldTag": "y",
                        "marcTag": "003",
                        "ind1": " ",
                        "ind2": " ",
                        "content": "OCoLC",
                        "subfields": None,
                    },
                ],
            },
            "count": 1,
            "totalCount": 0,
            "statusCode": 200,
            "debugInfo": [],
        }


class MockPlatformSessionResponseDeletedRecord:
    """Simulates NYPL Platform query response for deleted record"""

    def __init__(self):
        self.status_code = 200
        self.url = "request_url_here"

    def json(self):
        return {
            "data": {
                "id": "19099433",
                "nyplSource": "sierra-nypl",
                "nyplType": "bib",
                "updatedDate": None,
                "createdDate": "2017-08-23T17:59:35-04:00",
                "deletedDate": "2011-09-15",
                "deleted": True,
                "locations": [],
                "suppressed": None,
                "lang": None,
                "title": None,
                "author": None,
                "materialType": None,
                "bibLevel": None,
                "publishYear": None,
                "catalogDate": None,
                "country": None,
                "normTitle": None,
                "normAuthor": None,
                "standardNumbers": [],
                "controlNumber": "",
                "fixedFields": [],
                "varFields": [],
            },
            "count": 1,
            "totalCount": 0,
            "statusCode": 200,
            "debugInfo": [],
        }


class MockPlatformSessionResponseNotFound:
    """Simulates NYPL Platform failed query response"""

    def __init__(self):
        self.status_code = 404
        self.url = "query_url_here"

    def json(self):
        return {
            "statusCode": 404,
            "type": "exception",
            "message": "No record found",
            "error": [],
            "debugInfo": [],
        }


class MockSolrSessionResponseSuccess:
    def __init__(self):
        self.status_code = 200
        self.url = "query_url_here"

    def json(self):
        return {
            "response": {
                "numFound": 1,
                "start": 0,
                "numFoundExact": True,
                "docs": [
                    {
                        "id": "12234255",
                        "suppressed": True,
                        "deleted": False,
                        "call_number": "eBOOK",
                    }
                ],
            }
        }


class MockSolrSessionResponseNotFound:
    def __init__(self):
        self.status_code = 200
        self.url = "query_url_here"

    def json(self):
        return {
            "response": {"numFound": 0, "start": 0, "numFoundExact": True, "docs": []}
        }


@pytest.fixture
def mock_successful_platform_post_token_response(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockPlatformAuthServerResponseSuccess()

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_failed_platform_post_token_response(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockPlatformAuthServerResponseFailure

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_successful_platform_session_response(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockPlatformSessionResponseSuccess()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_successful_platform_session_response_deleted_record(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockPlatformSessionResponseDeletedRecord()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_failed_platform_session_response(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockPlatformSessionResponseNotFound()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_successful_solr_session_response(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockSolrSessionResponseSuccess()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_failed_solr_session_response(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockSolrSessionResponseNotFound()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_platform_env(monkeypatch):
    monkeypatch.setenv("NYPL_PLATFORM_CLIENT", "app_client_id")
    monkeypatch.setenv("NYPL_PLATFORM_SECRET", "app_secret")
    monkeypatch.setenv("NYPL_PLATFORM_OAUTH", "outh_server")
    monkeypatch.setenv("NYPL_PLATFORM_ENV", "prod")


@pytest.fixture
def mock_solr_env(monkeypatch):
    monkeypatch.setenv("BPL_SOLR_CLIENT_KEY", "solr_key")
    monkeypatch.setenv("BPL_SOLR_ENDPOINT", "solr_endpoint")
