# -*- coding: utf-8 -*-

"""
NightShift's database schema.


dialect+driver://username:password@host:port/database
# psycopg2
engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase', client_encoding='utf8')
"""
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    PickleType,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


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
    __table_args__ = (
        PrimaryKeyConstraint("sierraId", "libraryId"),
        {},
    )

    sierraId = Column(Integer, nullable=False)
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    resourceCategoryId = Column(
        Integer, ForeignKey("resource_category.nid"), nullable=False
    )

    archived = Column(Boolean, default=False)
    bibDate = Column(Date)
    author = Column(String(collation="uft8"))
    title = Column(String(collation="utf8"))
    pubDate = Column(String)

    congressNumber = Column(String)
    controlNumber = Column(String)
    distributorNumber = Column(String)
    otherNumber = Column(String)
    outputId = Column(Integer, ForeignKey("output_file.nid"))
    sourceId = Column(Integer, ForeignKey("source_file.nid"), nullable=False)
    srcFieldsToKeep = Column(PickleType)
    standardNumber = Column(String)
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

    oclcMatchNumber = Column(Integer)
    upgradeTimestamp = Column(DateTime)

    queries = relationship("WorldcatQuery", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"<Resource(sierraId='{self.sierraId}', libraryId='{self.libraryId}', "
            f"sourceId='{self.sourceId}', "
            f"resourceCategoryId='{self.resourceCategoryId}', "
            f"archived='{self.archived}', "
            f"bibDate='{self.bibDate}', "
            f"author='{self.author}', "
            f"title='{self.title}', "
            f"pubDate='{self.pubDate}', "
            f"controlNumber='{self.controlNumber}', "
            f"congressNumber='{self.congressNumber}', "
            f"standardNumber='{self.standardNumber}', "
            f"distributorNumber='{self.distributorNumber}', "
            f"status='{self.status}', "
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
    code = Column(String, unique=True)
    description = Column(String)

    def __repr__(self):
        return (
            f"<ResourceCategory(nid='{self.nid}', "
            f"code='{self.code}', "
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
    resourceId = Column(Integer, ForeignKey("resource.sierraId"), nullable=False)
    libraryId = Column(Integer, ForeignKey("library.nid"), nullable=False)
    match = Column(Boolean, nullable=False)
    responseCode = Column(Integer)
    response = Column(PickleType)  # save as requests.Response object?

    def __repr__(self):
        return (
            f"<WorldcatQuery(nid='{self.nid}', "
            f"resourceId='{self.resourceId}', "
            f"libraryId='{self.libraryId}', "
            f"match='{self.match}', "
            f"responseCode='{self.responseCode}')>"
        )
