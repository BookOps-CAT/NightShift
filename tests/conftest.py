import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nightshift.datastore import (
    Base,
    LibrarySystem,
    BibCategory,
    ExportFile,
    OutputFile,
    UpgradeSource,
    Resource,
    UrlField,
    WorldcatQuery,
)
from nightshift.datastore_values import LIB_SYS, BIB_CAT


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

    #
    for values in LIB_SYS.values():
        lib = LibrarySystem(**values)
        session.add(lib)
        session.commit()

    for values in BIB_CAT.values():
        cat = BibCategory(**values)
        session.add(cat)
        session.commit()

    yield session
