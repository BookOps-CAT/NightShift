"""
NightShift's database schema.
"""

import datetime
import os
from contextlib import contextmanager
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    PickleType,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.dialects.postgresql import BYTEA, ENUM, JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)


class Base(DeclarativeBase):
    pass


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
    Retrieves db configuration from env variables

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

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.datetime.now(datetime.timezone.utc)
    )
    libraryId: Mapped[int] = mapped_column(
        Integer, ForeignKey("library.nid"), nullable=False
    )
    sierraId: Mapped[int] = mapped_column(Integer, nullable=False)
    bibDate: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    resourceCategoryId: Mapped[int] = mapped_column(
        Integer, ForeignKey("resource_category.nid"), nullable=False
    )
    status: Mapped[str] = mapped_column(STATUS, nullable=False)

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

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(3), unique=True)

    def __repr__(self):
        return f"<Library(nid='{self.nid}', code='{self.code}')>"


class OutputFile(Base):
    """
    Output MARC file info.
    """

    __tablename__ = "output_file"
    __table_args__ = (UniqueConstraint("handle", "libraryId"),)

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    libraryId: Mapped[int] = mapped_column(
        Integer, ForeignKey("library.nid"), nullable=False
    )
    handle: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=datetime.datetime.now(datetime.timezone.utc)
    )

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

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    sierraId: Mapped[int] = mapped_column(Integer, nullable=False)
    libraryId: Mapped[int] = mapped_column(
        Integer, ForeignKey("library.nid"), nullable=False
    )
    resourceCategoryId: Mapped[int] = mapped_column(
        Integer, ForeignKey("resource_category.nid"), nullable=False
    )

    bibDate: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    author: Mapped[Optional[str]] = mapped_column(String)
    title: Mapped[Optional[str]] = mapped_column(String)
    pubDate: Mapped[Optional[str]] = mapped_column(String)

    congressNumber: Mapped[Optional[str]] = mapped_column(String)
    controlNumber: Mapped[Optional[str]] = mapped_column(String)
    distributorNumber: Mapped[Optional[str]] = mapped_column(String)
    otherNumber: Mapped[Optional[str]] = mapped_column(String)
    sourceId: Mapped[int] = mapped_column(
        Integer, ForeignKey("source_file.nid"), nullable=False
    )
    srcFieldsToKeep: Mapped[Optional[str]] = mapped_column(PickleType)
    standardNumber: Mapped[Optional[str]] = mapped_column(String)
    suppressed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    oclcMatchNumber: Mapped[Optional[str]] = mapped_column(String)
    fullBib: Mapped[Optional[bytes]] = mapped_column(BYTEA)
    outputId: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("output_file.nid")
    )
    status: Mapped[Optional[str]] = mapped_column(STATUS)
    enhanceTimestamp: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True
    )

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

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    description: Mapped[Optional[str]] = mapped_column(String)
    sierraBibFormatBpl: Mapped[str] = mapped_column(String, nullable=False)
    sierraBibFormatNyp: Mapped[str] = mapped_column(String, nullable=False)
    srcTags2Keep: Mapped[str] = mapped_column(String, nullable=False)
    dstTags2Delete: Mapped[str] = mapped_column(String, nullable=False)
    queryDays: Mapped[str] = mapped_column(String, nullable=False)

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

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String, nullable=False, unique=True)

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

    rottenAppleId: Mapped[int] = mapped_column(
        Integer, ForeignKey("rotten_apple.nid"), primary_key=True
    )
    resourceCategoryId: Mapped[int] = mapped_column(
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

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    libraryId: Mapped[int] = mapped_column(
        Integer, ForeignKey("library.nid"), nullable=False
    )
    handle: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=datetime.datetime.now(datetime.timezone.utc)
    )

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

    nid: Mapped[int] = mapped_column(Integer, primary_key=True)
    resourceId: Mapped[int] = mapped_column(
        Integer, ForeignKey("resource.nid", ondelete="CASCADE"), nullable=False
    )
    match: Mapped[bool] = mapped_column(Boolean, nullable=False)
    response: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=datetime.datetime.now(datetime.timezone.utc), nullable=False
    )

    def __repr__(self):
        return (
            f"<WorldcatQuery(nid='{self.nid}', "
            f"resourceId='{self.resourceId}', "
            f"match='{self.match}', "
            f"timestamp='{self.timestamp}')>"
        )
