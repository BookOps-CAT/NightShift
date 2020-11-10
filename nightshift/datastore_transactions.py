"""
This modules contains methods for transactions with data.db
"""
import datetime

from typing import List, Type

import nightshift


import sqlalchemy

from .datastore import (
    ExportFile,
    LibrarySystem,
    BibCategory,
    UpgradeSource,
    UrlField,
    UrlType,
    Resource,
    WorldcatQuery,
)
from .datastore_values import LIB_SYS, BIB_CAT, UPGRADE_SRC, URL_TYPE


def calculate_date_using_days_from_today(days: int) -> datetime.date:
    """
    Calculate date x days from today

    Args:
        days:               number of days

    Returns:
        datetime.date
    """
    return datetime.date.today() - datetime.timedelta(days=days)


def construct_url_records(
    sbid: int, lsid: int, data: List[dict]
) -> List[nightshift.datastore.UrlField]:
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
    session,
    data,
    library_system,
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


def insert(session, model, **kwargs):
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


def insert_resource(session, **kwargs):
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


def insert_export_file(session, **kwargs):
    """
    Inserts to ExportFile table
    """
    instance = session.query(ExportFile).filter_by(handle=kwargs["handle"]).first()
    if not instance:
        instance = ExportFile(**kwargs)
        session.add(instance)
    return instance


def retrieve_records(session, model, **kwargs):
    instances = session.query(model).filter_by(**kwargs).all()
    return instances


def retrieve_bibnos(
    session: Type[sqlalchemy.orm.session.Session], lsid: int, bcid: int
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
        session, Resource, librarySystemId=lsid, bibCategoryId=bcid, title=None
    )
    for rec in records:
        sbids.append(rec.sbid)

    return sbids


def retrieve_never_queried_reserve_ids(
    session: Type[sqlalchemy.orm.session.Session], lsid: int
) -> List[Resource]:
    """
    Retrieves reserve ids of e-resource records from datastore that need full
    Worldcat bib and were never queried before

    Args:
        session:            sqlalchemy session
        lsid:               library system id

    Returns:
        records
    """
    records = (
        session.query(Resource)
        .outerjoin(WorldcatQuery)
        .filter(
            Resource.librarySystemId == lsid,
            Resource.bibCategoryId == 1,
            WorldcatQuery.sBibId == None,
        )
        .all()
    )
    return records


def create_datastore(dal):
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

    session.commit()
    session.close()
