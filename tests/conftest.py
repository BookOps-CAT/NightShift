# -*- coding: utf-8 -*-

import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

from nightshift.datastore import Base


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


@pytest.fixture
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


@pytest.fixture(scope="function")
def test_connection(mock_db_env):
    # create db engine differently on local machine or Travis
    if os.getenv("TRAVIS"):
        conn = f"postgresql+psycopg2://{os.getenv('NS_DBUSER')}@{os.getenv('NS_DBHOST')}:{os.getenv('NS_DBPORT')}/{os.getenv('NS_DBNAME')}"
    else:
        conn = f"postgresql+psycopg2://{os.getenv('NS_DBUSER')}:{os.getenv('NS_DBPASSW')}@{os.getenv('NS_DBHOST')}:{os.getenv('NS_DBPORT')}/{os.getenv('NS_DBNAME')}"
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
