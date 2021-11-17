# -*- coding: utf-8 -*-
import os

import pytest
from sqlalchemy import create_engine
import yaml


from nightshift.datastore import Base
from nightshift.datastore_transactions import init_db


@pytest.fixture
def env_var(monkeypatch):
    if not os.getenv("TRAVIS"):
        with open("tests/envar.yaml", "r") as f:
            data = yaml.safe_load(f)
            for k, v in data.items():
                monkeypatch.setenv(k, v)


@pytest.fixture
def test_db(env_var):
    # setup
    init_db()
    yield

    # teardown
    test_connection = f"postgresql://{os.getenv('NS_DBUSER')}:{os.getenv('NS_DBPASSW')}@{os.getenv('NS_DBHOST')}:{os.getenv('NS_DBPORT')}/{os.getenv('NS_DBNAME')}"
    engine = create_engine(test_connection)
    Base.metadata.drop_all(engine)
