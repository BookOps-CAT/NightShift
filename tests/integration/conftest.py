# -*- coding: utf-8 -*-
import datetime
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml


from nightshift.comms.worldcat import Worldcat
from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import Base, Library, Resource, ResourceCategory, SourceFile


class MockIOError:
    def __init__(self, *args, **kwargs):
        raise IOError


@pytest.fixture
def mock_io_error(monkeypatch):
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.put", MockIOError)
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.listdir", MockIOError)
    monkeypatch.setattr("paramiko.sftp_client.SFTPClient.file", MockIOError)


@pytest.fixture
def env_var(monkeypatch):
    if not os.getenv("TRAVIS"):
        with open("tests/envar.yaml", "r") as f:
            data = yaml.safe_load(f)
            for k, v in data.items():
                monkeypatch.setenv(k, v)


@pytest.fixture
def local_connection(env_var):
    return f"postgresql://{os.getenv('NS_DBUSER')}:{os.getenv('NS_DBPASSW')}@{os.getenv('NS_DBHOST')}:{os.getenv('NS_DBPORT')}/{os.getenv('NS_DBNAME')}"


@pytest.fixture
def local_db(local_connection):
    engine = create_engine(local_connection)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session
    session.close()

    # teardown
    Base.metadata.drop_all(engine)


@pytest.fixture
def test_data(local_db, stub_resource):
    for k, v in LIBRARIES.items():
        local_db.add(Library(nid=v["nid"], code=k))

    for k, v in RESOURCE_CATEGORIES.items():
        local_db.add(
            ResourceCategory(nid=v["nid"], name=k, description=v["description"]),
        )
    local_db.add(SourceFile(libraryId=1, handle="foo1.mrc"))
    local_db.add(SourceFile(libraryId=2, handle="foo2.mrc"))
    local_db.commit()
    local_db.add(stub_resource)
    local_db.commit()


@pytest.fixture
def stub_resource():
    return Resource(
        sierraId=11111111,
        libraryId=1,
        resourceCategoryId=1,
        sourceId=1,
        bibDate=datetime.datetime.utcnow().date() - datetime.timedelta(days=31),
        title="Harry potter and the sorcerer's stone",
        status="open",
    )


# @pytest.fixture
# def mock_drive():
#     with Drive(*creds) as drive:
#         yield drive
