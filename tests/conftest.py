import datetime
import json
import os
import pickle
import xml.etree.ElementTree as ET


from pymarc import Record, Field
import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatAuthorizationError, WorldcatSessionError
from bookops_nypl_platform.errors import BookopsPlatformError
from bookops_bpl_solr.session import BookopsSolrError

from nightshift.datastore import (
    Base,
    LibrarySystem,
    BibCategory,
    ExportFile,
    OutputFile,
    UpgradeSource,
    Resource,
    SierraFormat,
    UrlType,
    UrlField,
    WorldcatQuery,
)
import nightshift
from nightshift import __title__, __version__
from nightshift.datastore_values import (
    LIB_SYS,
    BIB_CAT,
    UPGRADE_SRC,
    URL_TYPE,
    SIERRA_FORMAT,
)

# from nightshift.errors import NightShiftError
from .service_responses import NPRESP, BSRESP, WFRESP


"""
=========================================================
General fixtures
=========================================================
"""


class FakeDateTime(datetime.datetime):
    @classmethod
    def now(cls):
        return cls(2019, 1, 1, 17, 0, 0, 0)


class FakeDate(datetime.date):
    @classmethod
    def today(cls):
        return cls(2019, 1, 1)


@pytest.fixture
def mock_datetime_now(monkeypatch):
    monkeypatch.setattr(datetime, "datetime", FakeDateTime)


@pytest.fixture
def mock_date_today(monkeypatch):
    monkeypatch.setattr(datetime, "date", FakeDate)


class MockTimeout:
    def __init__(self, *args, **kwargs):
        raise requests.exceptions.Timeout


@pytest.fixture
def mock_timeout(monkeypatch):
    monkeypatch.setattr("requests.Session.get", MockTimeout)


@pytest.fixture
def mock_keys():
    os.environ["platform-client-id"] = "app_client_id"
    os.environ["platform-client-secret"] = "app_client_secret"
    os.environ["platform-oauth-server"] = "app_oauth-server"
    os.environ["nyp-worldcat-key"] = "app_worldcat_key"
    os.environ["nyp-worldcat-secret"] = "app_worldcat_secret"
    os.environ["nyp-worldcat-scopes"] = "app_worldcat_scopes"
    os.environ["nyp-worldcat-principal-id"] = "app_worldcat_principal_id"
    os.environ["nyp-worldcat-principal-idns"] = "app_worldcat_principal_idns"
    os.environ["bpl-worldcat-key"] = "app_worldcat_key"
    os.environ["bpl-worldcat-secret"] = "app_worldcat_secret"
    os.environ["bpl-worldcat-scopes"] = "app_worldcat_scopes"
    os.environ["bpl-worldcat-principal-id"] = "app_worldcat_principal_id"
    os.environ["bpl-worldcat-principal-idns"] = "app_worldcat_principal_idns"
    os.environ["solr-client-key"] = "app_client_key"
    os.environ["solr-endpoint"] = "sorl_endpoint"


"""
============================================================
Datastore fixtures
============================================================
"""


@pytest.fixture(scope="function")
def db_setup():
    """
    Sets up in-memory datastore and yield session
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture(scope="function")
def init_dataset(db_setup):
    """
    Populates datastore with initial data
    """
    session = db_setup

    for values in LIB_SYS.values():
        rec = LibrarySystem(**values)
        session.add(rec)
        session.commit()

    for values in BIB_CAT.values():
        rec = BibCategory(**values)
        session.add(rec)
        session.commit()

    for values in UPGRADE_SRC.values():
        rec = UpgradeSource(**values)
        session.add(rec)

    for values in URL_TYPE.values():
        rec = UrlType(**values)
        session.add(rec)

    for values in SIERRA_FORMAT.values():
        rec = SierraFormat(**values)
        session.add(rec)

    yield session


@pytest.fixture(scope="function")
def brief_bib_dataset(init_dataset):
    """
    Populates datastore with brief data from Sierra export
    """
    session = init_dataset

    # export files
    session.add(ExportFile(handle="nyp-ere-20200930.txt"))
    session.add(ExportFile(handle="nyp-pre-20200929.txt"))
    session.add(ExportFile(handle="bpl-ere-20200930.txt"))
    session.add(ExportFile(handle="bpl-ere-20200929.txt"))
    session.commit()

    # two nypl eresources
    session.add(
        Resource(
            sbid=22259002,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN123456789",
            did="reserve-id-1",
            sierraFormatId=1,
            bibDate=datetime.date(2020, 9, 30),
        )
    )
    session.add(
        Resource(
            sbid=22259003,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN123456780",
            did="reserve-id-2",
            sierraFormatId=2,
            bibDate=datetime.date(2020, 9, 30),
        )
    )
    # deleted record
    session.add(
        Resource(
            sbid=19099433,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN123456783",
            did="reserve-id-5",
            deleted=True,
            sierraFormatId=1,
            bibDate=datetime.date(2020, 9, 30),
        )
    )

    # two nypl English print
    session.add(
        Resource(
            sbid=12345670,
            librarySystemId=1,
            bibCategoryId=2,
            exportFileId=2,
            cno="ODN123456781",
            sierraFormatId=4,
            bibDate=datetime.date(2020, 9, 29),
        )
    )
    session.add(
        Resource(
            sbid=12345671,
            librarySystemId=1,
            bibCategoryId=2,
            exportFileId=2,
            cno="ODN123456782",
            sierraFormatId=4,
            bibDate=datetime.date(2020, 9, 29),
        )
    )

    # two bpl eresources
    session.add(
        Resource(
            sbid=12014671,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=3,
            cno="ODN223456789",
            did="reserve-id-3",
            sierraFormatId=1,
            bibDate=datetime.date(2020, 9, 30),
        )
    )
    session.add(
        Resource(
            sbid=22345679,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=3,
            cno="ODN223456780",
            did="reserve-id-4",
            sierraFormatId=2,
            bibDate=datetime.date(2020, 9, 30),
        )
    )
    # deleted record
    session.add(
        Resource(
            sbid=29099433,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN123456784",
            did="reserve-id-6",
            deleted=True,
            sierraFormatId=1,
            bibDate=datetime.date(2020, 9, 30),
        )
    )
    # two bpl English print
    session.add(
        Resource(
            sbid=22345670,
            librarySystemId=2,
            bibCategoryId=2,
            exportFileId=4,
            cno="bt223456789",
            sierraFormatId=4,
            bibDate=datetime.date(2020, 9, 29),
        )
    )
    session.add(
        Resource(
            sbid=22345671,
            librarySystemId=2,
            bibCategoryId=2,
            exportFileId=4,
            cno="bt223456780",
            sierraFormatId=4,
            bibDate=datetime.date(2020, 9, 29),
        )
    )
    session.commit()

    yield session


@pytest.fixture(scope="function")
def mixed_dataset(init_dataset, mock_datetime_now, fake_xml_response):
    """
    Populate database with mixed of brief, enhanced, and
    full records.
    """
    session = init_dataset

    # export files
    session.add(ExportFile(handle="nyp-ere-20200930.txt"))
    session.commit()

    session.add(
        Resource(
            sbid=1,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN1",
            did="reserve-id-1",
            sierraFormatId=1,
            bibDate=datetime.date(2018, 12, 31),
        )
    )

    # one month record
    session.add(
        Resource(
            sbid=2,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="OD2",
            did="reserve-id-2",
            sierraFormatId=1,
            bibDate=datetime.date(2018, 12, 15),
            title="test title 1",
            wqueries=[
                WorldcatQuery(
                    sBibId=2,
                    found=False,
                    queryStamp=datetime.datetime.now() - datetime.timedelta(days=7),
                ),
            ],
        )
    )

    # found full bib (generic)
    session.add(
        Resource(
            sbid=3,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            wcn="001",
            cno="ODN-3",
            sierraFormatId=1,
            bibDate=datetime.date(2018, 11, 1),
            title="test title 2",
            upgraded=True,
            upgradeSourceId=1,
            upgradeStamp=datetime.datetime(2019, 1, 1, 17, 0, 0, 0),
            wqueries=[
                WorldcatQuery(
                    sBibId=3,
                    found=True,
                    queryStamp=datetime.datetime.now() - datetime.timedelta(days=7),
                    record=fake_xml_response,
                )
            ],
            urls=[
                UrlField(
                    ufid=1, sBibId=3, librarySystemId=1, uTypeId=1, url="content_url"
                ),
                UrlField(
                    ufid=2, sBibId=3, librarySystemId=1, uTypeId=2, url="excerpt_url"
                ),
                UrlField(
                    ufid=3, sBibId=3, librarySystemId=1, uTypeId=3, url="image_url"
                ),
                UrlField(
                    ufid=4, sBibId=3, librarySystemId=1, uTypeId=4, url="thumbnail_url"
                ),
            ],
        )
    )

    # deleted record
    session.add(
        Resource(
            sbid=4,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN4",
            did="reserve-id-4",
            deleted=True,
            sierraFormatId=1,
            bibDate=datetime.date(2018, 12, 15),
        )
    )

    # enhanced record between 2-6 months old not queried in the last month
    session.add(
        Resource(
            sbid=5,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="OD5",
            did="reserve-id-5",
            sierraFormatId=1,
            bibDate=datetime.date(2018, 8, 1),
            title="test title 1",
            wqueries=[
                WorldcatQuery(
                    found=False,
                    queryStamp=datetime.datetime.now() - datetime.timedelta(days=35),
                ),
            ],
        )
    )

    session.commit()

    yield session


@pytest.fixture(scope="function")
def full_resource_dataset(init_dataset, fake_xml_response_content):
    session = init_dataset

    # export files
    session.add(ExportFile(handle="nyp-ere-20200930.txt"))
    session.add(ExportFile(handle="bpl-ere-20200930.txt"))
    session.commit()

    # nypl eBook record
    session.add(
        Resource(
            sbid=1,
            librarySystemId=1,
            bibCategoryId=1,  # eResource
            exportFileId=1,
            cno="ODN1",
            sbn="9781",
            did="reserve-id-1",
            wcn="773692015",
            sierraFormatId=2,  # ebook
            bibDate=datetime.date(2020, 9, 30),
            title="Zendegi",
            author="Egan, Greg",
            upgradeStamp=datetime.date(2020, 9, 30),
            upgradeSourceId=1,
            urls=[
                UrlField(librarySystemId=1, uTypeId=1, url="https://content_url"),
                UrlField(librarySystemId=1, uTypeId=2, url="https://excerpt_url"),
                UrlField(librarySystemId=1, uTypeId=3, url="https://image_url"),
                UrlField(librarySystemId=1, uTypeId=4, url="https://thumbnail_url"),
            ],
            wqueries=[
                WorldcatQuery(
                    queryStamp=datetime.datetime(2020, 9, 30, 15, 0, 0),
                    found=True,
                    record=fake_xml_response_content,
                )
            ],
        )
    )

    # bpl eBook record
    session.add(
        Resource(
            sbid=2,
            librarySystemId=2,
            bibCategoryId=1,  # eResource
            exportFileId=2,
            cno="ODN2",
            sbn="9782",
            did="reserve-id-2",
            wcn="773692015",
            sierraFormatId=2,  # ebook
            bibDate=datetime.date(2020, 9, 30),
            title="Zendegi",
            author="Egan, Greg",
            upgradeStamp=datetime.date(2020, 9, 30),
            upgradeSourceId=1,
            urls=[
                UrlField(librarySystemId=2, uTypeId=1, url="https://content_url"),
                UrlField(librarySystemId=2, uTypeId=2, url="https://excerpt_url"),
                UrlField(librarySystemId=2, uTypeId=3, url="https://image_url"),
                UrlField(librarySystemId=2, uTypeId=4, url="https://thumbnail_url"),
            ],
            wqueries=[
                WorldcatQuery(
                    queryStamp=datetime.datetime(2020, 9, 30, 15, 0, 0),
                    found=True,
                    record=fake_xml_response_content,
                )
            ],
        )
    )
    session.commit()

    yield session


"""
====================================================
NYPL Platform fixtures
====================================================
"""


class FakePlatformHTTP200SessionResponse:
    def __init__(self):
        self.status_code = 200

    def json(self):
        return NPRESP


class FakePlatformHTTP404SessionResponse:
    def __init__(self):
        self.status_code = 404

    def json(self):
        return {
            "statusCode": 404,
            "type": "exception",
            "message": "No records found",
            "error": [],
            "debugInfo": [],
        }


class FakePlatformHTTP401SessionResponse:
    def __init__(self):
        self.status_code = 401

    def json(self):
        return {"statusCode": 401, "type": "unauthorized", "message": "Unauthorized"}


class MockBookopsPlatformError:
    def __init__(self, *args, **kwargs):
        raise BookopsPlatformError


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


@pytest.fixture
def mock_successful_platform_post_token_response(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockPlatformAuthServerResponseSuccess()

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_failed_platform_post_token_response(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockBookopsPlatformError()

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_successful_platform_session_get_request(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return FakePlatformHTTP200SessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_bookops_platform_error(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockBookopsPlatformError()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_platform_401_error_response(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return FakePlatformHTTP401SessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def stub_nyp_platform_200_response():
    return FakePlatformHTTP200SessionResponse()


@pytest.fixture
def stub_nyp_platform_401_response():
    return FakePlatformHTTP401SessionResponse()


@pytest.fixture
def stub_nyp_platform_404_response():
    return FakePlatformHTTP404SessionResponse()


@pytest.fixture
def live_keys():
    if os.name == "nt":
        fh = os.path.join(os.environ["USERPROFILE"], ".platform/tomasz_platform.json")
        with open(fh, "r") as file:
            data = json.load(file)
            os.environ["platform-client-id"] = data["client-id"]
            os.environ["platform-client-secret"] = data["client-secret"]
            os.environ["platform-oauth-server"] = data["oauth-server"]
    else:
        # Travis env variables defined in the repository settings
        pass


@pytest.fixture
def stub_nyp_responses():
    return NPRESP


@pytest.fixture
def stub_platform_record():
    return NPRESP["data"][0]


@pytest.fixture
def stub_platform_record_missing():
    # dict is mutable so make a copy
    data = NPRESP["data"][0].copy()

    # remove isbn tags
    new_fields = []
    for field in data["varFields"]:
        if field["marcTag"] == "020":
            pass
        elif field["marcTag"] == "010":
            pass
        elif field["marcTag"] == "037":
            pass
        elif field["marcTag"] == "100":
            pass
        elif field["marcTag"] == "024":
            pass
        elif field["marcTag"] == "856":
            pass
        else:
            new_fields.append(field)

    data["varFields"] = new_fields
    return data


"""
================================================================
Worldcat fixtures
================================================================
"""


class MockWorldcatAuthServerResponseSuccess:
    """Simulates oauth server response to successful token request"""

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


class MockWorldcatAuthorizatonError:
    def __init__(self, *args, **kwargs):
        raise WorldcatAuthorizationError


class MockWorldcatSessionError:
    def __init__(self, *args, **kwargs):
        raise WorldcatSessionError


@pytest.fixture
def fake_successful_worldcat_metadata_search_response():
    with open("tests/wcsearch_resp.pickle", "rb") as f:
        response = pickle.load(f)
        return response


@pytest.fixture
def fake_no_match_worldcat_metadata_search_response():
    with open("tests/wcsearch_fail_resp.pickle", "rb") as f:
        response = pickle.load(f)
        return response


@pytest.fixture
def fake_successful_worldcat_full_bib_response():
    with open("tests/wcbib_resp.pickle", "rb") as f:
        response = pickle.load(f)
        return response


@pytest.fixture
def mock_successful_worldcat_post_token_response(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockWorldcatAuthServerResponseSuccess()

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_failed_worldcat_post_token_response(monkeypatch):
    def mock_oauth_server_response(*args, **kwargs):
        return MockWorldcatAuthorizatonError()

    monkeypatch.setattr(requests, "post", mock_oauth_server_response)


@pytest.fixture
def mock_failed_worldcat_metadata_search_response(monkeypatch):
    def mock_worldcat_metadata_server_response(*args, **kwargs):
        return MockWorldcatSessionError()

    monkeypatch.setattr(requests.Session, "get", mock_worldcat_metadata_server_response)


@pytest.fixture
def mock_successful_worldcat_metadata_search_response(
    monkeypatch, fake_successful_worldcat_metadata_search_response
):
    def mock_worldcat_metadata_server_response(*args, **kwargs):
        return fake_successful_worldcat_metadata_search_response

    monkeypatch.setattr(requests.Session, "get", mock_worldcat_metadata_server_response)


@pytest.fixture
def mock_worldcat_metadata_search_response_no_results(
    monkeypatch, fake_worldcat_metadata_search_response
):
    def mock_worldcat_metadata_server_response(*args, **kwargs):
        return fake_no_match_worldcat_metadata_search_response

    monkeypatch.setattr(requests.Session, "get", mock_worldcat_metadata_server_response)


@pytest.fixture
def mock_successful_worldcat_metadata_full_bib_response(
    monkeypatch, fake_successful_worldcat_full_bib_response
):
    def mock_worldcat_metadata_server_response(*args, **kwargs):
        return fake_successful_worldcat_full_bib_response

    monkeypatch.setattr(requests.Session, "get", mock_worldcat_metadata_server_response)


@pytest.fixture
def fake_xml_response():
    return WFRESP


@pytest.fixture
def fake_xml_response_content(fake_successful_worldcat_full_bib_response):
    return fake_successful_worldcat_full_bib_response.content


@pytest.fixture
def fake_metadata_session(mock_successful_worldcat_post_token_response):
    token = WorldcatAccessToken(
        key="app_worldcat-key",
        secret="app_worldcat-secret",
        scopes="app_worldcat-scopes",
        principal_id="app_worldcat-principal-id",
        principal_idns="app_worldcat-principal-idns",
        agent=f"{__title__}/{__version__}",
    )
    with MetadataSession(authorization=token) as session:
        return session


@pytest.fixture
def mock_search_for_brief_eresource_success(
    monkeypatch, fake_successful_worldcat_metadata_search_response
):
    def mock_response(
        *args,
        **kwargs,
    ):
        return fake_successful_worldcat_metadata_search_response

    monkeypatch.setattr(
        nightshift.api_worldcat, "search_for_brief_eresource", mock_response
    )


@pytest.fixture
def mock_get_full_bib(monkeypatch, fake_successful_worldcat_full_bib_response):
    def mock_response(*args, **kwargs):
        return fake_successful_worldcat_full_bib_response

    monkeypatch.setattr(nightshift.api_worldcat, "get_full_bib", mock_response)


@pytest.fixture
def mock_search_for_brief_eresource_fail(
    monkeypatch, fake_no_match_worldcat_metadata_search_response
):
    def mock_response(*arg, **kwargs):
        return fake_no_match_worldcat_metadata_search_response

    monkeypatch.setattr(
        nightshift.api_worldcat, "search_for_brief_eresource", mock_response
    )


@pytest.fixture
def fake_xml_bib(fake_successful_worldcat_full_bib_response):
    return ET.fromstring((fake_successful_worldcat_full_bib_response.content))


@pytest.fixture
def mock_find_matching_eresource_hit(fake_xml_response, monkeypatch):
    def func_output(*arg, **kwargs):
        return ("773692015", fake_xml_response)

    monkeypatch.setattr("nightshift.api_worldcat.find_matching_eresource", func_output)


@pytest.fixture
def mock_find_matching_eresource_no_hit(monkeypatch):
    def func_output(*arg, **kwargs):
        return None

    monkeypatch.setattr("nightshift.api_worldcat.find_matching_eresource", func_output)


"""
=====================================================
BPL Solr fixtures
=====================================================
"""


class FakeSolrHTTP200SessionResponse:
    def __init__(self):
        self.status_code = 200

    def json(self):
        return BSRESP


class FakeSolrHTTP200NoHitsSessionResponse:
    def __init__(self):
        self.status_code = 200

    def json(self):
        return {
            "response": {"numFound": 0, "start": 0, "numFoundExact": True, "docs": []}
        }


class FakeSolrHTTP401SessionResponse:
    def __init__(self):
        self.status_code = 401

    def json(self):
        return {"statusCode": 401, "type": "unauthorized", "message": "Unauthorized"}


class MockBookopsSolrError:
    def __init__(self, *args, **kwargs):
        raise BookopsSolrError


@pytest.fixture
def stub_bpl_solr_200_response():
    return FakeSolrHTTP200SessionResponse()


@pytest.fixture
def stub_bpl_solr_401_response():
    return FakeSolrHTTP401SessionResponse()


@pytest.fixture
def stub_bpl_solr_no_hits_response():
    return FakeSolrHTTP200NoHitsSessionResponse()


@pytest.fixture
def stub_solr_response():
    return BSRESP


@pytest.fixture
def mock_successful_solr_session_get_request(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return FakeSolrHTTP200SessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_no_hits_solr_session_get_request(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return FakeSolrHTTP200NoHitsSessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_401_error_solr_session_get_request(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return FakeSolrHTTP401SessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


"""
======================================================================
Pymarc
======================================================================
"""


@pytest.fixture
def stub_marc_bib():
    tags = []
    marc_bib = Record()
    marc_bib.leader = "00000nam a2200000u  4500"
    tags.append(Field(tag="001", data="ocm0001"))
    tags.append(Field(tag="245", indicators=["0", "0"], subfields=["a", "Test title"]))
    tags.append(Field(tag="001", data="ocn1111"))
    tags.append(
        Field(
            tag="019",
            indicators=[" ", " "],
            subfields=["a", "some-id-001"],
        )
    )
    tags.append(
        Field(
            tag="020",
            indicators=[" ", " "],
            subfields=["a", "isbn001", "b", "isbn002"],
        )
    )
    tags.append(
        Field(
            tag="024",
            indicators=[" ", " "],
            subfields=["a", "upc001"],
        )
    )
    tags.append(
        Field(
            tag="037",
            indicators=[" ", " "],
            subfields=["a", "some-id-0001", "b", "test-distributor"],
        )
    )
    tags.append(
        Field(
            tag="037",
            indicators=[" ", " "],
            subfields=["a", "some-id-0002", "b", "Overdrive, Inc."],
        )
    )
    tags.append(
        Field(
            tag="084",
            indicators=[" ", " "],
            subfields=["a", "some-classification", "2", "test-thesaurus"],
        )
    )
    tags.append(
        Field(
            tag="091",
            indicators=[" ", " "],
            subfields=["a", "some-callnumber"],
        )
    )
    tags.append(
        Field(
            tag="099",
            indicators=[" ", " "],
            subfields=["a", "some-callnumber"],
        )
    )
    tags.append(
        Field(
            tag="263",
            indicators=[" ", " "],
            subfields=["a", "some-date"],
        )
    )
    tags.append(
        Field(
            tag="856",
            indicators=[" ", "3"],
            subfields=["u", "url1", "3", "public-note-1"],
        )
    )
    tags.append(
        Field(
            tag="856",
            indicators=[" ", "3"],
            subfields=["u", "url2", "3", "public-note-2"],
        )
    )
    tags.append(
        Field(
            tag="856",
            indicators=[" ", "3"],
            subfields=["u", "url3", "3", "public-note-3"],
        )
    )
    tags.append(
        Field(
            tag="838",
            indicators=[" ", " "],
            subfields=["u", "EBSCOhost", "b", "EBSC", "n", "11111"],
        )
    )
    for tag in tags:
        marc_bib.add_ordered_field(tag)

    return marc_bib
