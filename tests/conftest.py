# -*- coding: utf-8 -*-

import sys


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
def test_session():
    # open test db differently on Win an Linux
    if sys.platform == "win32":
        c = db_config()
        conn = f"postgresql+psycopg2://{c['user']}:{c['passw']}@{c['host']}:{c['port']}/{c['name']}"
        engine = create_engine(conn)
    elif sys.platform == "linux":
        engine = create_engine(
            "postgresql+psycopg2://postgres@127.0.0.1:5433/nightshiftTestDB"
        )
        pass

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session
    session.close()
    Base.metadata.drop_all(engine)
