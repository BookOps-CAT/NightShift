import datetime
import os
from typing import Any

import pytest
import requests
import yaml
from bookops_worldcat.errors import WorldcatRequestError
from paramiko import SFTPClient
from paramiko.ssh_exception import SSHException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nightshift import constants
from nightshift.comms.worldcat import Worldcat
from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES, ROTTEN_APPLES
from nightshift.datastore import (
    Base,
    Library,
    OutputFile,
    Resource,
    ResourceCategory,
    RottenApple,
    RottenAppleResource,
    SourceFile,
    WorldcatQuery,
)
from nightshift.datastore_transactions import ResCatById, ResCatByName, parse_query_days


class FakeUtcNow(datetime.datetime):
    @classmethod
    def now(cls, tzinfo=datetime.timezone.utc):
        return cls(2021, 1, 1, 17, 0, 0, 0, tzinfo=datetime.timezone.utc)


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch, caplog):
    caplog.set_level("DEBUG")
    monkeypatch.setenv("NYPL_PLATFORM_CLIENT", "app_client_id")
    monkeypatch.setenv("NYPL_PLATFORM_SECRET", "app_secret")
    monkeypatch.setenv("NYPL_PLATFORM_OAUTH", "outh_server")
    monkeypatch.setenv("NYPL_PLATFORM_ENV", "prod")
    monkeypatch.setenv("BPL_SOLR_CLIENT_KEY", "solr_key")
    monkeypatch.setenv("BPL_SOLR_ENDPOINT", "solr_endpoint")
    monkeypatch.setenv("WCNYP_KEY", "lib_key")
    monkeypatch.setenv("WCNYP_SECRET", "lib_secret")
    monkeypatch.setenv("WCNYP_SCOPE", "WorldCatMetadataAPI")
    monkeypatch.setenv("WCBPL_KEY", "lib_key")
    monkeypatch.setenv("WCBPL_SECRET", "lib_secret")
    monkeypatch.setenv("WCBPL_SCOPE", "WorldCatMetadataAPI")
    monkeypatch.setenv("SFTP_HOST", "sftp.fakehost.com")
    monkeypatch.setenv("SFTP_PORT", "22")
    monkeypatch.setenv("SFTP_USER", "nightshift")
    monkeypatch.setenv("SFTP_PASSW", "sftp_password")
    monkeypatch.setenv("SFTP_NS_SRC", "sierra_dumps_dir")
    monkeypatch.setenv("SFTP_NS_DST", "load_dir")
    monkeypatch.setenv("LOGGLY_TOKEN", "ns_token_here")
    monkeypatch.setenv("LOG_HANDLERS", "console,file,loggly")


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
    WCBPL_KEY: bpl_worldcat_key
    WCBPL_SECRET: bpl_worlcat_secret
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
        bibDate=datetime.datetime.now(datetime.timezone.utc).date()
        - datetime.timedelta(days=31),
        title="TITLE 1",
        status="bot_enhanced",
        fullBib=b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<entry xmlns="http://www.w3.org/2005/Atom">\n  <content type="application/xml">\n    <response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n      <record xmlns="http://www.loc.gov/MARC21/slim">\n        <leader>00000cam a2200000Ia 4500</leader>\n        <controlfield tag="001">ocn850939580</controlfield>\n        <controlfield tag="003">OCoLC</controlfield>\n        <controlfield tag="005">20190426152409.0</controlfield>\n        <controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n        <datafield tag="040" ind1=" " ind2=" ">\n          <subfield code="a">OCPSB</subfield>\n          <subfield code="b">eng</subfield>\n          <subfield code="c">OCPSB</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">NYP</subfield>\n    </datafield>\n        <datafield tag="035" ind1=" " ind2=" ">\n          <subfield code="a">(OCoLC)850939580</subfield>\n    </datafield>\n        <datafield tag="020" ind1=" " ind2=" ">\n          <subfield code="a">some isbn</subfield>\n    </datafield>\n        <datafield tag="049" ind1=" " ind2=" ">\n          <subfield code="a">NYPP</subfield>\n    </datafield>\n        <datafield tag="100" ind1="0" ind2=" ">\n          <subfield code="a">OCLC RecordBuilder.</subfield>\n    </datafield>\n        <datafield tag="245" ind1="1" ind2="0">\n          <subfield code="a">Record Builder Added This Test Record</subfield>\n    <subfield code="c">spam.</subfield>\n    </datafield>\n        <datafield tag="300" ind1=" " ind2=" ">\n          <subfield code="a">1 online resource</subfield>\n    </datafield>\n        <datafield tag="336" ind1=" " ind2=" ">\n          <subfield code="a">text</subfield>\n          <subfield code="b">txt</subfield>\n          <subfield code="2">rdacontent</subfield>\n    </datafield>\n        <datafield tag="337" ind1=" " ind2=" ">\n          <subfield code="a">unmediated</subfield>\n          <subfield code="b">n</subfield>\n          <subfield code="2">rdamedia</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">TEST RECORD -- DO NOT USE.</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">Added Field by MarcEdit.</subfield>\n    </datafield>\n  <datafield tag="650" ind1=" " ind2="0">\n          <subfield code="a">Test.</subfield>\n    </datafield>\n        </record>\n    </response>\n  </content>\n  <id>http://worldcat.org/oclc/850939580</id>\n  <link href="http://worldcat.org/oclc/850939580"/>\n</entry>',
        oclcMatchNumber="850939580",
        enhanceTimestamp=datetime.datetime.now(datetime.timezone.utc).date()
        - datetime.timedelta(days=15),
        queries=[WorldcatQuery(match=True)],
        outputId=1,
    )


@pytest.fixture
def stub_res_cat_by_name():
    data = dict()
    for k, v in RESOURCE_CATEGORIES.items():
        data[k] = ResCatByName(
            v["nid"],
            v["sierraBibFormatBpl"],
            v["sierraBibFormatNyp"],
            v["srcTags2Keep"].split(","),
            v["dstTags2Delete"].split(","),
            parse_query_days(v["queryDays"]),
        )
    return data


@pytest.fixture
def stub_res_cat_by_id():
    data = dict()
    for k, v in RESOURCE_CATEGORIES.items():
        data[v["nid"]] = ResCatById(
            k,
            v["sierraBibFormatBpl"],
            v["sierraBibFormatNyp"],
            v["srcTags2Keep"].split(","),
            v["dstTags2Delete"].split(","),
            parse_query_days(v["queryDays"]),
        )
    return data


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
            ResourceCategory(
                nid=v["nid"],
                name=k,
                description=v["description"],
                sierraBibFormatBpl=v["sierraBibFormatBpl"],
                sierraBibFormatNyp=v["sierraBibFormatNyp"],
                srcTags2Keep=v["srcTags2Keep"],
                dstTags2Delete=v["dstTags2Delete"],
                queryDays=v["queryDays"],
            )
        )
    for code, resource_cat_ids in ROTTEN_APPLES.items():
        ids = [
            RottenAppleResource(resourceCategoryId=RESOURCE_CATEGORIES[n]["nid"])
            for n in resource_cat_ids
        ]
        test_session.add(RottenApple(code=code, applicableResourceIds=ids))
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


class MockStat:
    def __init__(self):
        self.st_size = 1


class MockFile:
    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def prefetch(self, *args, **kwargs):
        pass

    def read(self, *args, **kwargs):
        return b""

    def stat(self, *args, **kwargs):
        return MockStat()


@pytest.fixture
def mock_drive(monkeypatch, mocker, mock_env_vars):
    mock_obj = mocker.MagicMock()

    def mock_file(*args, **kwargs):
        return MockFile()

    def mock_listdir(*args, **kwargs):
        return ["NYP-bar-pout"]

    def mock_stat(*args, **kwargs):
        all_args = [i for i in args] + [i for i in kwargs.values()]
        if "foo.mrc" in all_args or "load_dir/foo-01.mrc" in all_args:
            pass
        else:
            raise FileNotFoundError

    monkeypatch.setattr(SFTPClient, "_send_version", lambda *args, **kwargs: None)
    monkeypatch.setattr(SFTPClient, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(SFTPClient, "file", mock_file)
    monkeypatch.setattr(SFTPClient, "close", lambda *args, **kwargs: None)
    monkeypatch.setattr(SFTPClient, "listdir", mock_listdir)
    monkeypatch.setattr(SFTPClient, "put", lambda *args, **kwargs: None)
    monkeypatch.setattr(SFTPClient, "stat", mock_stat)
    monkeypatch.setattr("nightshift.comms.storage.Transport", mock_obj)


@pytest.fixture
def mock_empty_sftp_client(monkeypatch, mock_drive):
    monkeypatch.setattr(SFTPClient, "from_transport", lambda *args, **kwargs: None)


@pytest.fixture
def mock_ssh_exception(monkeypatch, mock_drive):
    def mock_ssh_exception(self, *args, **kwargs):
        raise SSHException

    monkeypatch.setattr("nightshift.comms.storage.Transport", mock_ssh_exception)


@pytest.fixture
def mock_io_error(monkeypatch, mock_drive):
    def io_error(self, *args, **kwargs):
        raise IOError

    monkeypatch.setattr(SFTPClient, "put", io_error)
    monkeypatch.setattr(SFTPClient, "listdir", io_error)
    monkeypatch.setattr(SFTPClient, "file", io_error)


class MockHTTPResponse:
    def __init__(
        self,
        _content: bytes = b"some content here",
        _json: dict[str, Any] = {},
        reason: str = "foo",
        status_code: int = 200,
        url: str = "request_url_here",
    ):
        self._content = _content
        self._json = _json
        self.reason = reason
        self.status_code = status_code
        self.url = url

    @property
    def content(self) -> bytes:
        return self._content

    def json(self) -> dict[str, Any]:
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError


@pytest.fixture
def mock_400_response():
    return MockHTTPResponse(status_code=400)


# Worldcat fixtures ########


@pytest.fixture
def mock_token_error(monkeypatch, mock_400_response):
    monkeypatch.setattr(requests, "post", mock_400_response)


@pytest.fixture
def mock_token(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        expires_at = datetime.datetime.strftime(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(0, 1199),
            "%Y-%m-%d %H:%M:%SZ",
        )
        json_response = {
            "access_token": "token_string_here",
            "token_type": "bearer",
            "expires_at": expires_at,
            "scope": "scopes_here",
            "id_token": "token_string_here",
            "expires_in": 3600,
        }
        return MockHTTPResponse(status_code=200, _json=json_response)

    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)
    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_worldcat_response(monkeypatch):
    json_response = {
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
                "isbns": ["9780585013367"],
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
    response = MockHTTPResponse(status_code=200, _json=json_response)

    def mock_api_response(*args, **kwargs):
        return response

    monkeypatch.setattr(requests.Session, "send", mock_api_response)
    return response


@pytest.fixture
def mock_worldcat_response_no_matches(monkeypatch):
    response = MockHTTPResponse(status_code=200, _json={"numberOfRecords": 0})

    def mock_api_response(*args, **kwargs):
        return response

    monkeypatch.setattr(requests.Session, "send", mock_api_response)
    return response


@pytest.fixture
def mock_session_error(monkeypatch):
    def mock_error(self, *args, **kwargs):
        raise WorldcatRequestError("Timeout error")

    monkeypatch.setattr("requests.Session.send", mock_error)


@pytest.fixture
def stub_worldcat_session(mock_env_vars, mock_token, mock_worldcat_response):
    with Worldcat("NYP") as session:
        yield session


# NYPL Platform & BPL Solr fixtures ###


@pytest.fixture
def mock_platform_response():
    return MockHTTPResponse(
        status_code=200,
        url="request_url_here",
        _json={
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
                ],
            },
            "count": 1,
            "totalCount": 0,
            "statusCode": 200,
            "debugInfo": [],
        },
    )


@pytest.fixture
def mock_platform_response_oclc_origin(mock_platform_response):
    json_response = mock_platform_response.json()
    json_response["data"]["varFields"] = [{"marcTag": "003", "content": "OCoLC"}]
    return MockHTTPResponse(_json=json_response)


@pytest.fixture
def mock_platform_response_deleted_record(mock_platform_response):
    json_response = mock_platform_response.json()
    json_response["data"]["deleted"] = True
    return MockHTTPResponse(_json=json_response)


@pytest.fixture
def mock_platform_response_not_found():
    return MockHTTPResponse(status_code=404)


@pytest.fixture
def mock_platform_response_suppressed(mock_platform_response):
    json_response = mock_platform_response.json()
    json_response["data"]["suppressed"] = True
    json_response["data"]["varFields"] = [
        {"marcTag": "001", "content": "2010074825"},
        {"marcTag": "091", "subfields": [{"tag": "a", "content": "eNYPL"}]},
    ]
    return MockHTTPResponse(
        status_code=200, url="request_url_here", _json=json_response
    )


@pytest.fixture
def mock_solr_response():
    return MockHTTPResponse(
        status_code=200,
        url="request_url_here",
        _json={
            "response": {
                "numFound": 1,
                "start": 0,
                "numFoundExact": True,
                "docs": [
                    {
                        "id": "12234255",
                        "suppressed": False,
                        "deleted": False,
                        "call_number": "FOO",
                    }
                ],
            }
        },
    )


@pytest.fixture
def mock_solr_response_oclc_origin(mock_solr_response):
    json_response = mock_solr_response.json()
    json_response["response"]["docs"][0]["ss_marc_tag_003"] = "OCoLC"
    return MockHTTPResponse(
        status_code=200, url="request_url_here", _json=json_response
    )


@pytest.fixture
def mock_solr_response_suppressed(mock_solr_response):
    json_response = mock_solr_response.json()
    json_response["response"]["docs"][0]["suppressed"] = True
    json_response["response"]["docs"][0]["call_number"] = "eBOOK"
    return MockHTTPResponse(
        status_code=200, url="request_url_here", _json=json_response
    )


@pytest.fixture
def mock_solr_response_record_not_found():
    return MockHTTPResponse(
        status_code=200,
        url="request_url_here",
        _json={
            "response": {"numFound": 0, "start": 0, "numFoundExact": True, "docs": []}
        },
    )


@pytest.fixture
def mock_successful_platform_session_response(monkeypatch, mock_platform_response):
    def mock_api_response(*args, **kwargs):
        return mock_platform_response

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_successful_platform_session_response_deleted_record(
    monkeypatch, mock_platform_response_deleted_record
):
    def mock_api_response(*args, **kwargs):
        return mock_platform_response_deleted_record

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_successful_solr_session_response(monkeypatch, mock_solr_response_suppressed):
    def mock_api_response(*args, **kwargs):
        return mock_solr_response_suppressed

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_failed_solr_session_response(monkeypatch, mock_solr_response_record_not_found):
    def mock_api_response(*args, **kwargs):
        return mock_solr_response_record_not_found

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_os_error(monkeypatch):
    def os_error(self, *args, **kwargs):
        raise OSError

    monkeypatch.setattr("builtins.open", os_error)


@pytest.fixture
def mock_os_error_on_remove(monkeypatch):
    def os_error(self, *args, **kwargs):
        raise OSError

    monkeypatch.setattr("os.remove", os_error)


@pytest.fixture
def mock_init_libraries(monkeypatch):
    monkeypatch.setattr(constants, "LIBRARIES", {"NYP": {"nid": 1}})
