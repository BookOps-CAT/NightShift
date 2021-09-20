# -*- coding: utf-8 -*-

import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

from nightshift.datastore import Base


def db_config():
    """
    requires yaml file with local postgres config

    example:
    ---
    host: localhost
    user:
    passw:
    port: 5432
    name: nightshiftTestDB
    """
    with open("tests/confdatabase.yaml", "r") as f:
        data = yaml.safe_load(f)
        return data


@pytest.fixture(scope="function")
def test_engine():
    # create db engine differently on local machine or Travis
    if os.getenv("TRAVIS"):
        engine = create_engine(
            "postgresql+psycopg2://postgres@127.0.0.1:5433/ns_db")
    else:
        c = db_config()
        conn = f"postgresql+psycopg2://{c['user']}:{c['passw']}@{c['host']}:{c['port']}/{c['name']}"
        engine = create_engine(conn)
    return engine


@pytest.fixture(scope="function")
def test_session(test_engine):

    Base.metadata.create_all(test_engine)
    Session = sessionmaker(bind=test_engine)
    session = Session()

    yield session
    session.close()
    Base.metadata.drop_all(test_engine)
