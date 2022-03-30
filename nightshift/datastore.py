# -*- coding: utf-8 -*-

"""
NightShift's database schema.
"""
from contextlib import contextmanager
from datetime import datetime
import os

from sqlalchemy import (
    Boolean,
    Column,
    create_engine,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    PickleType,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ENUM, JSONB, BYTEA
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


Base = declarative_base()


STATUS = ENUM(
    "bot_enhanced",
    "expired",
    "open",
    "staff_deleted",
    "staff_enhanced",
    "worldcat_miss",
    "worldcat_hit",
    name="status",
    metadata=Base.metadata,
)


def conf_db():
    """
    Retrieves db configuation from env variables

    Returns:
        db settings as dictionary
    """
    return dict(
        POSTGRES_USER=os.getenv("POSTGRES_USER"),
        POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD"),
        POSTGRES_HOST=os.getenv("POSTGRES_HOST"),
        POSTGRES_PORT=os.getenv("POSTGRES_PORT"),
        POSTGRES_DB=os.getenv("POSTGRES_DB"),
    )


class DataAccessLayer:
    def __init__(self):
        db = conf_db()
        self.conn = f"postgresql://{db['POSTGRES_USER']}:{db['POSTGRES_PASSWORD']}@{db['POSTGRES_HOST']}:{db['POSTGRES_PORT']}/{db['POSTGRES_DB']}"
        self.engine = None

    def connect(self):
        self.engine = create_engine(self.conn)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)


@contextmanager
def session_scope():
    """
    Provides a transactional scope around series of operations.
    """
    dal = DataAccessLayer()
    dal.connect()
    session = dal.Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class Event(Base):
    """
    Statistics table.
    Stores information about transactions affecting resources, such as
    WorldCat matches/upgrades, resources being dropped out from the process because
    they were cataloged or deleted by cataloging staff, finally, marks resources
    that expired from the process because of they age.
    """

    __tablename__ = "event"

    nid = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow())
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    sierraId = Column(Integer, nullable=False)
    bibDate = Column(Date, nullable=False)
    resourceCategoryId = Column(
        Integer, ForeignKey("resource_category.nid"), nullable=False
    )
    status = Column(STATUS)

    def __repr__(self):
        return (
            f"<Event(nid='{self.nid}', timestamp='{self.timestamp}', "
            f"libraryId='{self.libraryId}', sierraId='{self.sierraId}', "
            f"bibDate='{self.bibDate}', "
            f"resourceCategoryId='{self.resourceCategoryId}', "
            f"status='{self.status}')>"
        )


class Library(Base):
    """
    Library system.
    """

    __tablename__ = "library"

    nid = Column(Integer, primary_key=True)
    code = Column(String(3), unique=True)

    def __repr__(self):
        return f"<Library(nid='{self.nid}', code='{self.code}')>"


class OutputFile(Base):
    """
    Output MARC file info.
    """

    __tablename__ = "output_file"
    __table_args__ = (UniqueConstraint("handle", "libraryId"),)

    nid = Column(Integer, primary_key=True)
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    handle = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow())

    def __repr__(self):
        return (
            f"<OutputFile(nid='{self.nid}', "
            f"libraryId='{self.libraryId}', "
            f"handle='{self.handle}', "
            f"timestamp='{self.timestamp}')>"
        )


class Resource(Base):
    """
    Resource to be upgraded.
    """

    __tablename__ = "resource"
    __table_args__ = (UniqueConstraint("sierraId", "libraryId"),)

    nid = Column(Integer, primary_key=True)
    sierraId = Column(Integer, nullable=False)
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    resourceCategoryId = Column(
        Integer, ForeignKey("resource_category.nid"), nullable=False
    )

    bibDate = Column(Date, nullable=False)
    author = Column(String)
    title = Column(String)
    pubDate = Column(String)

    congressNumber = Column(String)
    controlNumber = Column(String)
    distributorNumber = Column(String)
    otherNumber = Column(String)
    sourceId = Column(Integer, ForeignKey("source_file.nid"), nullable=False)
    srcFieldsToKeep = Column(PickleType)
    standardNumber = Column(String)
    suppressed = Column(Boolean, nullable=False, default=False)

    oclcMatchNumber = Column(String)
    fullBib = Column(BYTEA)
    outputId = Column(Integer, ForeignKey("output_file.nid"))
    status = Column(STATUS)
    enhanceTimestamp = Column(DateTime)

    queries = relationship("WorldcatQuery", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"<Resource(nid='{self.nid}', "
            f"sierraId='{self.sierraId}', libraryId='{self.libraryId}', "
            f"sourceId='{self.sourceId}', "
            f"resourceCategoryId='{self.resourceCategoryId}', "
            f"bibDate='{self.bibDate}', "
            f"author='{self.author}', "
            f"title='{self.title}', "
            f"pubDate='{self.pubDate}', "
            f"controlNumber='{self.controlNumber}', "
            f"congressNumber='{self.congressNumber}', "
            f"standardNumber='{self.standardNumber}', "
            f"distributorNumber='{self.distributorNumber}', "
            f"suppressed='{self.suppressed}', "
            f"status='{self.status}', "
            f"outputId='{self.outputId}', "
            f"oclcMatchNumber='{self.oclcMatchNumber}', "
            f"enhanceTimestamp='{self.enhanceTimestamp}')>"
        )


class ResourceCategory(Base):
    """
    Resource Category. Example: ebook, fiction-gen, fiction-mystery.
    """

    __tablename__ = "resource_category"

    nid = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)
    sierraBibFormatBpl = Column(String, nullable=False)
    sierraBibFormatNyp = Column(String, nullable=False)
    srcTags2Keep = Column(String)
    dstTags2Delete = Column(String)
    queryDays = Column(String, nullable=False)

    def __repr__(self):
        return (
            f"<ResourceCategory(nid='{self.nid}', "
            f"name='{self.name}', "
            f"description='{self.description}', "
            f"sierraBibFormatBpl='{self.sierraBibFormatBpl}', "
            f"sierraBibFormatNyp='{self.sierraBibFormatNyp}', "
            f"srcTags2Keep='{self.srcTags2Keep}', "
            f"dstTags2Delete='{self.dstTags2Delete}', "
            f"queryDays='{self.queryDays}')>"
        )


class RottenApple(Base):
    """
    List of OCLC organization codes which records
    should be rejected because of poor quality
    """

    __tablename__ = "rotten_apple"

    nid = Column(Integer, primary_key=True)
    code = Column(String, nullable=False, unique=True)

    applicableResourceIds = relationship(
        "RottenAppleResource", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<RottenApple(nid='{self.nid}', code='{self.code}')>"


class RottenAppleResource(Base):
    """
    Specifies applicable `ResourceCategory` instances for each
    RottenApple
    """

    __tablename__ = "rotten_apple_resource"

    rottenAppleId = Column(Integer, ForeignKey("rotten_apple.nid"), primary_key=True)
    resourceCategoryId = Column(
        Integer, ForeignKey("resource_category.nid"), primary_key=True
    )

    def __repr__(self):
        return (
            f"<RottenAppleResource(rottenAppleId='{self.rottenAppleId}', "
            f"resourceCategoryId='{self.resourceCategoryId}')>"
        )


class SourceFile(Base):
    """
    Source MARC file info.
    """

    __tablename__ = "source_file"
    __table_args__ = (UniqueConstraint("handle", "libraryId"),)

    nid = Column(Integer, primary_key=True)
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    handle = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow())

    def __repr__(self):
        return (
            f"<SourceFile(nid='{self.nid}', "
            f"libraryId='{self.libraryId}', "
            f"handle='{self.handle}', "
            f"timestamp='{self.timestamp}')>"
        )


class WorldcatQuery(Base):
    """
    Metadata API responses.
    """

    __tablename__ = "worldcat_query"

    nid = Column(Integer, primary_key=True)
    resourceId = Column(
        Integer, ForeignKey("resource.nid", ondelete="CASCADE"), nullable=False
    )
    match = Column(Boolean, nullable=False)
    response = Column(JSONB)
    timestamp = Column(DateTime, default=datetime.utcnow(), nullable=False)

    def __repr__(self):
        return (
            f"<WorldcatQuery(nid='{self.nid}', "
            f"resourceId='{self.resourceId}', "
            f"match='{self.match}', "
            f"timestamp='{self.timestamp}')>"
        )
