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
from nightshift.datastore_values import LIB_SYS, BIB_CAT, UPGRADE_SRC


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

    yield session
