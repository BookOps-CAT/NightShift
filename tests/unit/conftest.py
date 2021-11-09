# -*- coding: utf-8 -*-

import datetime
from io import BytesIO
import os

from bookops_marc import Bib
from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatSessionError
import paramiko
from pymarc import Field
import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

from nightshift.comms.storage import get_credentials, Drive
from nightshift.comms.worldcat import Worldcat
from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import Base, Library, Resource, ResourceCategory, SourceFile
from nightshift.marc.marc_parser import BibReader


class FakeUtcNow(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return cls(2021, 1, 1, 17, 0, 0, 0)


@pytest.fixture
def mock_utcnow(monkeypatch):
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)


@pytest.fixture
def mock_log_env(monkeypatch):
    monkeypatch.setenv("LOGGLY_TOKEN", "ns_token_here")
    monkeypatch.setenv("LOG_HANDLERS", "console,file,loggly")


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
    LOGGLY_TOKEN: app_token
    LOG_HANDLERS: "console,file,loggly"
    """
    with open("tests/envar.yaml", "r") as f:
        data = yaml.safe_load(f)
        return data


@pytest.fixture(scope="function")
def test_log(monkeypatch):
    if not os.getenv("TRAVIS"):
        data = local_test_config()
        monkeypatch.setenv("LOGGLY_TOKEN", data["LOGGLY_TOKEN"])
        monkeypatch.setenv("LOG_HANDLERS", data["LOG_HANDLERS"])


# DB fixtures ############


@pytest.fixture(scope="function")
def mock_db_env(monkeypatch):
    if os.getenv("TRAVIS"):
        data = dict(
            NS_DBUSER="postgres",
            NS_DBPASSW="",
            NS_DBHOST="127.0.0.1",
            NS_DBPORT="5433",
            NS_DBNAME="ns_db",
        )
    else:
        data = local_test_config()

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


@pytest.fixture(scope="function")
def test_session(test_connection):

    # setup
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
    test_session.add(SourceFile(nid=1, libraryId=1, handle="foo1.mrc"))
    test_session.add(SourceFile(nid=2, libraryId=2, handle="foo2.mrc"))
    test_session.commit()


@pytest.fixture
def test_data_rich(test_session, test_data_core):
    test_session.add(
        Resource(
            nid=1,
            sierraId=1111111,
            libraryId=1,
            resourceCategoryId=1,
            sourceId=1,
            bibDate=datetime.datetime.utcnow().date() - datetime.timedelta(days=31),
            title="TITLE 1",
            status="open",
        )
    )
    test_session.commit()


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

    return bib


@pytest.fixture
def fake_BibReader():
    return BibReader(BytesIO(b"some records"), "nyp")


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


# SFTP / newtowrked drive #############


@pytest.fixture
def mock_sftp_env(monkeypatch, sftpserver):
    monkeypatch.setenv("SFTP_HOST", sftpserver.host)
    monkeypatch.setenv("SFTP_PORT", str(sftpserver.port))
    monkeypatch.setenv("SFTP_USER", "nightshift")
    monkeypatch.setenv("SFTP_PASSW", "sftp_password")
    monkeypatch.setenv("SFTP_NS_SRC", "sierra_dumps_dir")
    monkeypatch.setenv("SFTP_NS_DST", "load_dir")


class MockIOError:
    def __init__(self, *args, **kwargs):
        raise IOError


class MockSSHException:
    def __init__(self, *args, **kwargs):
        raise paramiko.ssh_exception.SSHException


@pytest.fixture
def mock_io_error(monkeypatch):
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.put", MockIOError)
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.listdir", MockIOError)
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.file", MockIOError)


@pytest.fixture
def mock_ssh_exception(monkeypatch):
    monkeypatch.setattr("paramiko.transport.Transport.connect", MockSSHException)


@pytest.fixture
def mock_drive(mock_sftp_env):
    creds = get_credentials()
    with Drive(*creds) as drive:
        yield drive


# NYPL Platform & BPL Solr fixtures ###


class MockSearchSession500HTTPError:
    """Mocks 500 HTTP error responses for both platforms"""

    def __init__(self):
        self.status_code = 500
        self.url = "query_url_here"

    def json(self):
        return {
            "statusCode": 500,
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


class MockPlatformSessionResponseNotFound:
    """Simulates NYPL Platform failed query response"""

    def __init__(self):
        self.status_code = 404

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
def mock_platform_env(monkeypatch):
    monkeypatch.setenv("NYPL_PLATFORM_CLIENT", "app_client_id")
    monkeypatch.setenv("NYPL_PLATFORM_SECRET", "app_secret")
    monkeypatch.setenv("NYPL_PLATFORM_OAUTH", "outh_server")
    monkeypatch.setenv("NYPL_PLATFORM_ENV", "prod")


@pytest.fixture
def mock_solr_env(monkeypatch):
    monkeypatch.setenv("BPL_SOLR_CLIENT_KEY", "solr_key")
    monkeypatch.setenv("BPL_SOLR_ENDPOINT", "solr_endpoint")
