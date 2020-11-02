"""
Defines and initiates NightShift SQLite database. Contains data model and session.
"""

from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    create_engine,
    Column,
    ForeignKey,
    Integer,
    PickleType,
    String,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import relationship, sessionmaker


Base = declarative_base()


class LibrarySystem(Base):
    __tablename__ = "library_system"

    lsid = Column(Integer, primary_key=True, autoincrement=False)
    code = Column(String(3), nullable=False, unique=True)
    name = Column(String(25))

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<LibrarySystem({attrs})>"


class BibCategory(Base):
    __tablename__ = "bib_category"

    bcid = Column(Integer, primary_key=True, autoincrement=False)
    code = Column(String(5), nullable=False, unique=True)
    description = Column(String(50))

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<BibCategory({attrs})>"


class ExportFile(Base):
    __tablename__ = "export_file"

    efid = Column(Integer, primary_key=True)
    handle = Column(String(50), nullable=False, unique=True)
    dateCreated = Column(DateTime, nullable=False, default=datetime.now())

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<ExportFile({attrs})>"


class OutputFile(Base):
    __tablename__ = "output_file"

    ofid = Column(Integer, primary_key=True)
    handle = Column(String(50), nullable=False)
    dateCreated = Column(DateTime, nullable=False, default=datetime.now())

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<OutputFile({attrs})>"


class UpgradeSource(Base):
    __tablename__ = "upgrade_source"

    usid = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String(8), nullable=False, unique=True)
    description = Column(String(50))

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<UpgradeSource({attrs})>"


class UrlType(Base):
    __tablename__ = "url_type"

    utid = Column(Integer, primary_key=True, autoincrement=False)
    utype = Column(String(50), unique=False, nullable=False)

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<UrlType({attrs})>"


class UrlField(Base):
    __tablename__ = "url_field"

    ufid = Column(Integer, primary_key=True)
    sBibId = Column(Integer, ForeignKey("resource.sbid"), nullable=False)
    uTypeId = Column(Integer, ForeignKey("url_type.utid"), nullable=False)
    url = Column(String(120), nullable=False)

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<UrlField({attrs})>"


class Resource(Base):
    __tablename__ = "resource"
    __table_args__ = (UniqueConstraint("sbid", "librarySystemId", name="uix_resource"),)

    sbid = Column(Integer, primary_key=True, autoincrement=False)
    librarySystemId = Column(Integer, ForeignKey("library_system.lsid"), nullable=False)
    bibCategoryId = Column(Integer, ForeignKey("bib_category.bcid"), nullable=False)
    exportFileId = Column(Integer, ForeignKey("export_file.efid"), nullable=False)
    outputFileId = Column(Integer, ForeignKey("output_file.ofid"))
    cno = Column(String(20))  # bib control number
    sbn = Column(String(13))  # isbn
    lcn = Column(String(15))  # lccn
    did = Column(String(50))  # distributor number, ex. Overdrive reserve no.
    sid = Column(String(15))  # other standard number, ex. UPC
    wcn = Column(Integer)  # Worldcat OCLC number
    bibDate = Column(Date, nullable=False)
    title = Column(String(50))
    author = Column(String(50))
    pubDate = Column(Date)
    upgradeStamp = Column(DateTime)
    upgraded = Column(Boolean, default=False)
    upgradeSourceId = Column(Integer, ForeignKey("upgrade_source.usid"))

    urls = relationship("UrlField", cascade="all, delete-orphan")
    wqueries = relationship("WorldcatQuery", cascade="all, delete-orphan")

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<Resource({attrs})>"


class WorldcatQuery(Base):
    __tablename__ = "worldcat_query"

    wqid = Column(Integer, primary_key=True)
    sBibId = Column(Integer, ForeignKey("resource.sbid"), nullable=False)
    queryStamp = Column(DateTime, nullable=False, default=datetime.now())
    found = Column(Boolean, nullable=False, default=False)
    wcResponse = Column(PickleType)

    def __repr__(self):
        state = inspect(self)
        attrs = ", ".join([f"{attr.key}={attr.loaded_value!r}" for attr in state.attrs])
        return f"<WorldcatQuery({attrs})>"


class DataAccessLayer:
    def __init__(self):
        self.conn_string = "sqlite:///data.db"

    def connect(self):
        self.engine = create_engine(self.conn_string)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)


dal = DataAccessLayer()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
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
