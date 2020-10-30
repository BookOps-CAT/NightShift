from datetime import date

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
            sbid=12345678,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN123456789",
            bibDate=date(2020, 9, 30),
        )
    )
    session.add(
        Resource(
            sbid=12345679,
            librarySystemId=1,
            bibCategoryId=1,
            exportFileId=1,
            cno="ODN123456780",
            bibDate=date(2020, 9, 30),
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
            bibDate=date(2020, 9, 29),
        )
    )
    session.add(
        Resource(
            sbid=12345671,
            librarySystemId=1,
            bibCategoryId=2,
            exportFileId=2,
            cno="ODN123456782",
            bibDate=date(2020, 9, 29),
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
            bibDate=date(2020, 9, 30),
        )
    )
    session.add(
        Resource(
            sbid=22345679,
            librarySystemId=2,
            bibCategoryId=1,
            exportFileId=3,
            cno="ODN223456780",
            bibDate=date(2020, 9, 30),
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
            bibDate=date(2020, 9, 29),
        )
    )
    session.add(
        Resource(
            sbid=22345671,
            librarySystemId=2,
            bibCategoryId=2,
            exportFileId=4,
            cno="bt223456780",
            bibDate=date(2020, 9, 29),
        )
    )
    session.commit()

    yield session
