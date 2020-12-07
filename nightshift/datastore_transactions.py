"""
This modules contains methods for transactions with data.db
"""
from collections import namedtuple
import datetime
from typing import List, Iterator, Type

from sqlalchemy import func
from sqlalchemy.orm.session import Session as DatastoreSession
from sqlalchemy.sql.selectable import Alias


from .datastore import (
    DataAccessLayer,
    BibCategory,
    ExportFile,
    LibrarySystem,
    SierraFormat,
    Resource,
    UpgradeSource,
    UrlField,
    UrlType,
    WorldcatQuery,
)
from .datastore_values import LIB_SYS, BIB_CAT, UPGRADE_SRC, URL_TYPE, SIERRA_FORMAT


def calculate_date_using_days_from_today(days: int) -> datetime.date:
    """
    Calculate date x days from today

    Args:
        days:               number of days

    Returns:
        datetime.date
    """
    return datetime.date.today() - datetime.timedelta(days=days)


def create_datastore(dal: DataAccessLayer):
    """
    Creates and prepopulates with initial data new datastore (data.db)
    Args:
        dal:                data access layer
    """
    dal.connect()

    session = dal.Session()

    # insert data
    for values in LIB_SYS.values():
        insert(session, LibrarySystem, **values)
    for values in BIB_CAT.values():
        insert(session, BibCategory, **values)
    for values in UPGRADE_SRC.values():
        insert(session, UpgradeSource, **values)
    for values in URL_TYPE.values():
        insert(session, UrlType, **values)
    for values in SIERRA_FORMAT.values():
        insert(session, SierraFormat, **values)

    session.commit()
    session.close()


def construct_url_records(sbid: int, lsid: int, data: List[dict]) -> UrlField:
    """
    Prepares list of url data dictionaries as datastore UrlField records

    Args:
        sbid:               sierra bib number
        lsid:               library system id
        data:               list of url dictionaries where key is uTypeId
                            and value is the url

    Returns:
        list of UrlField records
    """
    urls = [
        (
            UrlField(
                sBibId=sbid, librarySystemId=lsid, uTypeId=x["uTypeId"], url=x["url"]
            )
        )
        for x in data
    ]
    return urls


def enhance_resource(
    session: DatastoreSession,
    data: namedtuple,
    library_system: str,
):
    """
    Updates records in datastore wih extra data pulled from library API

    Args:
        session:            sqlalchemy db session
        data:               `nightshift.models.SierraMeta` record data
    """

    lsid = LIB_SYS[library_system]["lsid"]
    instance = (
        session.query(Resource).filter_by(sbid=data.sbid, librarySystemId=lsid).one()
    )

    urls = construct_url_records(data.sbid, lsid, data.urls)
    record = dict(
        sbn=data.sbn,
        lcn=data.lcn,
        did=data.did,
        sid=data.sid,
        wcn=data.wcn,
        deleted=data.deleted,
        title=data.title,
        author=data.author,
        pubDate=data.pubDate,
        upgradeStamp=data.upgradeStamp,
        upgraded=data.upgraded,
        upgradeSourceId=data.upgradeSourceId,
        urls=urls,
    )
    for key, value in record.items():
        setattr(instance, key, value)


def insert(session: DatastoreSession, model: object, **kwargs):
    """
    Inserts a record into the datastore

    Args:
        session:            sqlalchemy db session
        model:              datastore table

    Returns:
        instance
    """
    instance = model(**kwargs)
    session.add(instance)
    session.flush()
    return instance


def insert_resource(session: DatastoreSession, **kwargs) -> Resource:
    """
    Inserts to Resource table if new record or returns instance of exisitng

    Args:
        session:            sqlalchemy db session

    Returns:
        instance
    """
    instance = session.query(Resource).filter_by(sbid=kwargs["sbid"]).first()
    if not instance:
        instance = Resource(**kwargs)
        session.add(instance)
    return instance


def insert_export_file(session: DatastoreSession, **kwargs) -> ExportFile:
    """
    Inserts to ExportFile table
    """
    instance = session.query(ExportFile).filter_by(handle=kwargs["handle"]).first()
    if not instance:
        instance = ExportFile(**kwargs)
        session.add(instance)
    return instance


def retrieve_records(session: DatastoreSession, model: object, **kwargs) -> List:
    instances = session.query(model).filter_by(**kwargs).all()
    return instances


def retrieve_brief_records_bibnos(
    session: DatastoreSession, lsid: int, bcid: int
) -> List[int]:
    """
    Retrieves records from datastore with matching library system id, bib category id,
    and missing full Sierra data

    Args:
        session:            sqlalchemy session
        lsid:               LibrarySytstem lsid
        bcid:               BibCategory bcid

    Returns:
        list of Sierra bibNos
    """
    sbids = []
    records = retrieve_records(
        session,
        Resource,
        librarySystemId=lsid,
        bibCategoryId=bcid,
        title=None,
        deleted=False,
    )
    for rec in records:
        sbids.append(rec.sbid)

    return sbids


def retrieve_never_queried_records(
    session: DatastoreSession,
    lsid: int,
    bcid: int,
) -> List[Resource]:
    """
    Retrieves records from datastore that need full
    Worldcat bib and were never queried before

    Args:
        session:            sqlalchemy session
        lsid:               library system id
        bcid:               bib category id

    Returns:
        records
    """
    records = (
        session.query(Resource)
        .outerjoin(WorldcatQuery)
        .filter(
            Resource.librarySystemId == lsid,
            Resource.bibCategoryId == bcid,
            Resource.wcn == None,
            Resource.deleted == False,
            WorldcatQuery.sBibId == None,
        )
        .all()
    )
    return records


def recent_worldcat_query_records(
    session: DatastoreSession,
) -> Alias:
    """
    Creates a subquery with only the most recent worldcat query record

    Args:
        session:            sqlalchemy session

    Returns:
        subquery
    """
    subq = (
        session.query(
            WorldcatQuery.sBibId,
            WorldcatQuery.wqid.label("wqid"),
            func.max(WorldcatQuery.queryStamp).label("wqStamp"),
        )
        .filter(
            WorldcatQuery.found == False,
        )
        .group_by(WorldcatQuery.sBibId)
        .subquery()
    )
    return subq


def retrieve_records_not_queried_in_days(
    session: DatastoreSession,
    lsid: int,
    bcid: int,
    bib_min_age: int = 0,
    bib_max_age: int = 28,
    query_cutoff_age: int = 6,
) -> List:
    """
    Retrieves records from datastore that were not queried in n days.
    By default finds 28 days old or younger records not queried for
    the past week (7 days).

    Args:
        session:            sqlalchemy session
        lsid:               library system id
        bcid:               bib category id
        bib_min_age:        minimal age of record in days to limit the search
        bib_max_age:        max age of the record in days to limit the search
        query_cutoff_age:   number of days since today for the most
                            recent Worldcat query (to be excluded from
                            the search)

    Returns:
        list of records
    """
    cutoff_date = calculate_date_using_days_from_today(query_cutoff_age)
    last_query = recent_worldcat_query_records(session)

    query = (
        session.query(
            Resource.sbid, Resource.did, last_query.c.wqid, last_query.c.wqStamp
        )
        .join(last_query)
        .filter(
            Resource.librarySystemId == lsid,
            Resource.bibCategoryId == bcid,
            Resource.upgraded == False,
            Resource.bibDate
            <= datetime.date.today() - datetime.timedelta(days=bib_min_age),
            Resource.bibDate
            > datetime.date.today() - datetime.timedelta(days=bib_max_age),
            last_query.c.wqStamp <= cutoff_date,
        )
    )
    # print(str(query.statement.compile(compile_kwargs={"literal_binds": True})))
    records = query.all()

    return records


def update_resource(
    session: DatastoreSession,
    sbid: int,
    library_system: str,
    upgrade_src: str,
    oclcNumber: str,
    bib: str,
):
    """
    Updates Resource & WorldcatQuery records after running queries in Worldcat

    Args:
        sbid:                   Resource record id (Sierra bib number)
        oclcNumber:             OCLC number as string without a prefix
        bib:                    MARC XML string of full bib encoded as UTF-8
    """
    upgrade_src_id = UPGRADE_SRC[upgrade_src]["usid"]
    lsid = LIB_SYS[library_system]["lsid"]

    if oclcNumber:
        query = session.query(Resource).filter_by(sbid=sbid, librarySystemId=lsid)
        query.update(
            dict(
                wcn=oclcNumber,
                upgradeStamp=datetime.datetime.now(),
                upgraded=True,
                upgradeSourceId=upgrade_src_id,
            )
        )
        insert(session, WorldcatQuery, **dict(sBibId=sbid, found=True, record=bib))

    else:
        insert(session, WorldcatQuery, **dict(sBibId=sbid))


def retrieve_resources_with_new_full_bib(
    session: DatastoreSession,
    library_system: str,
    bib_category: str,
) -> Iterator:
    lsid = LIB_SYS[library_system]["lsid"]
    bcid = BIB_CAT[bib_category]["bcid"]

    records = (
        session.query(Resource)
        .filter(
            Resource.librarySystemId == lsid,
            Resource.bibCategoryId == bcid,
            Resource.upgraded == True,
            Resource.upgradeSourceId != 2,
            Resource.outputFileId == None,
        )
        .all()
    )
    return records
