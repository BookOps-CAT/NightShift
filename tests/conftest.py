import datetime
import os
import json

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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


from .nyp_resp import RESP


class FakeDate(datetime.datetime):
    @classmethod
    def now(cls):
        return cls(2019, 1, 1, 17, 0, 0)


@pytest.fixture
def mock_datetime_now(monkeypatch):
    monkeypatch.setattr(datetime, "datetime", FakeDate)


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
            sbid=19099433,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN123456784",
            did="reserve-id-6",
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


class FakeHTTP200SessionResponse:
    def __init__(self):
        self.status_code = 200

    def json(self):
        return RESP


class FakeHTTP404SessionResponse:
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


class FakeHTTP401SessionResponse:
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
        return FakeHTTP200SessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_bookops_platform_error(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return MockBookopsPlatformError()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def mock_platform_401_error_response(monkeypatch):
    def mock_api_response(*args, **kwargs):
        return FakeHTTP401SessionResponse()

    monkeypatch.setattr(requests.Session, "get", mock_api_response)


@pytest.fixture
def stub_nyp_platform_200_response():
    return FakeHTTP200SessionResponse()


@pytest.fixture
def stub_nyp_platform_401_response():
    return FakeHTTP401SessionResponse()


@pytest.fixture
def stub_nyp_platform_404_response():
    return FakeHTTP404SessionResponse()


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


@pytest.fixture
def stub_nyp_responses():
    return RESP


@pytest.fixture
def stub_platform_record():
    return RESP["data"][0]


@pytest.fixture
def stub_platform_record_missing():
    # dict is mutable so make a copy
    data = RESP["data"][0].copy()

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
