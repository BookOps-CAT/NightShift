# -*- coding: utf-8 -*-

import datetime
import os

from bookops_marc import Bib
from pymarc import Field
import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

from nightshift.datastore import Base


class FakeUtcNow(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return cls(2021, 1, 1, 17, 0, 0, 0)


@pytest.fixture
def mock_utcnow(monkeypatch):
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)


# DB fixtures ############


def local_test_db_config():
    """
    requires yaml file with local postgres config

    example:
    ---
    NS_DBHOST: localhost
    NS_DBUSER: ns_test
    NS_DBPASSW: some_password
    NS_DBPORT: 5432
    NS_DBNAME: ns_db
    """
    with open("tests/confdatabase.yaml", "r") as f:
        data = yaml.safe_load(f)
        return data


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
        data = local_test_db_config()

    monkeypatch.setenv("NS_DBUSER", data["NS_DBUSER"])
    monkeypatch.setenv("NS_DBPASSW", data["NS_DBPASSW"])
    monkeypatch.setenv("NS_DBHOST", data["NS_DBHOST"])
    monkeypatch.setenv("NS_DBPORT", data["NS_DBPORT"])
    monkeypatch.setenv("NS_DBNAME", data["NS_DBNAME"])


@pytest.fixture
def mock_worldcat_creds(monkeypatch):
    for lib in ("NYP", "BPL"):
        monkeypatch.setenv(f"WC{lib}_KEY", "lib_key")
        monkeypatch.setenv(f"WC{lib}_SECRET", "lib_secret")
        monkeypatch.setenv(f"WC{lib}_SCOPE", "WorldCatMetadataAPI")
        monkeypatch.setenv(f"WC{lib}_PRINCIPALID", "lib_principal_id")
        monkeypatch.setenv(f"WC{lib}_PRINCIPALIDNS", "lib_principal_idns")


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
