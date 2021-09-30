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
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


Base = declarative_base()


def conf_db():
    """
    Retrieves db configuation from env variables

    Returns:
        db settings as dictionary
    """
    return dict(
        NS_DBUSER=os.getenv("NS_DBUSER"),
        NS_DBPASSW=os.getenv("NS_DBPASSW"),
        NS_DBHOST=os.getenv("NS_DBHOST"),
        NS_DBPORT=os.getenv("NS_DBPORT"),
        NS_DBNAME=os.getenv("NS_DBNAME"),
    )


class DataAccessLayer:
    def __init__(self):
        db = conf_db()
        self.conn = f"postgresql://{db['NS_DBUSER']}:{db['NS_DBPASSW']}@{db['NS_DBHOST']}:{db['NS_DBPORT']}/{db['NS_DBNAME']}"
        self.engine = None

    def connect(self):
        self.engine = create_engine(self.conn)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)


dal = DataAccessLayer()


@contextmanager
def session_scope():
    """
    Provides a transactional scope around series of operations.
    """
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
    Resource to be upgraded info.
    """

    __tablename__ = "resource"
    __table_args__ = (UniqueConstraint("sierraId", "libraryId"),)

    nid = Column(Integer, primary_key=True)
    sierraId = Column(Integer, nullable=False)
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    resourceCategoryId = Column(
        Integer, ForeignKey("resource_category.nid"), nullable=False
    )

    bibDate = Column(Date)
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

    deleted = Column(Boolean, nullable=False, default=False)
    deletedTimestamp = Column(DateTime)
    oclcMatchNumber = Column(Integer)
    outputId = Column(Integer, ForeignKey("output_file.nid"))
    status = Column(
        ENUM(
            "open",
            "expired",
            "deleted_staff",
            "upgraded_bot",
            "upgraded_staff",
            name="status",
        )
    )
    upgradeTimestamp = Column(DateTime)

    queries = relationship("WorldcatQuery", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"<Resource(sierraId='{self.sierraId}', libraryId='{self.libraryId}', "
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
            f"status='{self.status}', "
            f"deleted='{self.deleted}', "
            f"deletedTimestamp='{self.deletedTimestamp}', "
            f"outputId='{self.outputId}', "
            f"oclcMatchNumber='{self.oclcMatchNumber}', "
            f"upgradeTimestamp='{self.upgradeTimestamp}')>"
        )


class ResourceCategory(Base):
    """
    Resource Category. Example: ebook, fiction-gen, fiction-mystery.
    """

    __tablename__ = "resource_category"

    nid = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)

    def __repr__(self):
        return (
            f"<ResourceCategory(nid='{self.nid}', "
            f"name='{self.name}', "
            f"description='{self.description}')>"
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
    resourceId = Column(Integer, ForeignKey("resource.nid"), nullable=False)
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    match = Column(Boolean, nullable=False)
    response = Column(PickleType)  # save as requests.Response object?
    timestamp = Column(DateTime, default=datetime.now(), nullable=False)

    def __repr__(self):
        return (
            f"<WorldcatQuery(nid='{self.nid}', "
            f"resourceId='{self.resourceId}', "
            f"libraryId='{self.libraryId}', "
            f"match='{self.match}', "
            f"timestamp='{self.timestamp}')>"
        )
