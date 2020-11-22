import datetime
import os
import json

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bookops_worldcat.errors import WorldcatAuthorizationError

from bookops_nypl_platform.errors import BookopsPlatformError
from nightshift.datastore import (
    Base,
    LibrarySystem,
    BibCategory,
    ExportFile,
    OutputFile,
    UpgradeSource,
    Resource,
    UrlType,
    UrlField,
    WorldcatQuery,
)
from nightshift.datastore_values import LIB_SYS, BIB_CAT, UPGRADE_SRC, URL_TYPE
from nightshift.errors import NightShiftError


from .service_responses import NPRESP, WSRESP


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
            bibDate=datetime.date(2020, 9, 29),
        )
    )

    # two bpl eresources
    session.add(
        Resource(
            sbid=22345678,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=3,
            cno="ODN223456789",
            did="reserve-id-3",
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
            bibDate=datetime.date(2020, 9, 29),
        )
    )
    session.commit()

    yield session


@pytest.fixture(scope="function")
def mixed_dataset(init_dataset, mock_datetime_now):
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
            bibDate=datetime.date(2018, 12, 15),
            title="test title 1",
            wqueries=[
                WorldcatQuery(
                    sBibId=2,
                    found=False,
                    httpCode=404,
                    queryStamp=datetime.datetime.now() - datetime.timedelta(days=7),
                ),
            ],
        )
    )

    # found full bib
    session.add(
        Resource(
            sbid=3,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            wcn="003",
            cno="ODN-3",
            bibDate=datetime.date(2018, 11, 1),
            title="test title 2",
            wqueries=[
                WorldcatQuery(
                    sBibId=3,
                    found=True,
                    httpCode=200,
                    queryStamp=datetime.datetime.now() - datetime.timedelta(days=7),
                )
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
            bibDate=datetime.date(2018, 8, 1),
            title="test title 1",
            wqueries=[
                WorldcatQuery(
                    found=False,
                    httpCode=404,
                    queryStamp=datetime.datetime.now() - datetime.timedelta(days=35),
                ),
            ],
        )
    )

    session.commit()

    yield session


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
def mock_keys():
    os.environ["platform-client-id"] = "app_client_id"
    os.environ["platform-client-secret"] = "app_client_secret"
    os.environ["platform-oauth-server"] = "app_oauth-server"
    os.environ["worldcat-key"] = "app_worldcat_key"
    os.environ["worldcat-secret"] = "app_worldcat_secret"
    os.environ["worldcat-scopes"] = "app_worldcat_scopes"
    os.environ["worldcat-principal-id"] = "app_worldcat_principal_id"
    os.environ["worldcat-principal-idns"] = "app_worldcat_principal_idns"


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
