# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import (
    DataAccessLayer,
    Library,
    Resource,
    ResourceCategory,
    WorldcatQuery,
)


def init_db() -> None:
    """
    Initiates the database and prepopulates needed tables

    Args:
        session:                `sqlalchemy.Session` instance
    """
    # make sure to start from scratch
    dal = DataAccessLayer()

    dal.engine = create_engine(dal.conn)
    dal.connect()
    session = dal.Session()

    # recreate schema & prepopulate needed tables
    for k, v in LIBRARIES.items():
        session.add(Library(nid=v["nid"], code=k))

    for k, v in RESOURCE_CATEGORIES.items():
        session.add(
            ResourceCategory(nid=v["nid"], name=k, description=v["description"]),
        )

    session.commit()
    session.close()


def insert_or_ignore(session, model, **kwargs):
    """
    Adds a new record to given table (model) or ignores if the same.

    Args:
        session:                `sqlalchemy.Session` instance
        model:                  one of datastore table classes
        kwargs:                 new record values as dictionary

    Returns:
        instance of inserted or duplicate record
    """
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if not instance:
        instance = model(**kwargs)
        session.add(instance)
        return instance
    else:
        return None


def retrieve_new_resources(session: Session, libraryId: int) -> list[Resource]:
    """
    Retrieves resources that have been added to the db
    but has not been processed yet.
    The results are grouped by the resource category and ordered by
    resource nid.

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `Library.nid`

    Returns:
        list of `Resource` instances
    """
    resources = (
        session.query(Resource)
        .filter_by(libraryId=libraryId, status="open", deleted=False, queries=None)
        .order_by(Resource.resourceCategoryId, Resource.nid)
        .all()
    )
    return resources


def retrieve_open_older_resources(
    session: Session, libraryId: int, minAge: int, maxAge: int
) -> list[Resource]:
    """
    Queries resources with open status that has not been queried in WorldCat
    betweeen minAge and maxAge

    Args:
        session:                `sqlalchemy.Session` instance
        minAge:                 min number of days since bib creation date
        maxAge:                 max numb of days since bib creation date

    Returns:
        list of `Row` instances
    """

    resources = (
        session.query(
            Resource,
        )
        .join(WorldcatQuery)
        .filter(
            Resource.libraryId == libraryId,
            Resource.status == "open",
            Resource.deleted == False,
            Resource.oclcMatchNumber == None,
            Resource.bibDate > datetime.utcnow() - timedelta(days=maxAge),
        )
        .filter(
            WorldcatQuery.timestamp > datetime.utcnow() - timedelta(days=maxAge),
            WorldcatQuery.timestamp < datetime.utcnow() - timedelta(days=minAge),
        )
        .all()
    )
    return resources


def retrieve_open_matched_resources_without_full_bib(
    session: Session, libraryId: int
) -> list[Resource]:
    """
    Retrieves resources that have been matched to records in WorldCat,
    but not upgraded yet.
    The results are grouped by resource category and ordered by resource nid

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `Library.nid`

    Returns:
        list of `Resource` instances
    """
    resources = (
        session.query(Resource)
        .filter(
            Resource.libraryId == libraryId,
            Resource.status == "open",
            Resource.deleted == False,
            Resource.oclcMatchNumber != None,
            Resource.fullBib == None,
        )
        .order_by(Resource.resourceCategoryId, Resource.nid)
        .all()
    )
    return resources


def retrieve_open_matched_resources_with_full_bib_obtained(
    session: Session, libraryId: int
) -> list[Resource]:
    """
    Retrieves resources that include MARC XML with full bib

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `Library.nid`

    Returns:
        list of `Resource` instances
    """
    resources = (
        session.query(Resource)
        .filter(
            Resource.libraryId == libraryId,
            Resource.status == "open",
            Resource.deleted == False,
            Resource.fullBib.isnot(None),
        )
        .all()
    )
    return resources


def update_resource(session, sierraId, libraryId, **kwargs) -> Optional[Resource]:
    """
    Updates Resource record.

    Args:
        session:                `sqlalchemy.Session` instance
        sierraId:               sierra 8 digit bib # (without prefix
                                or check digit)
        libraryId:              datastore.Library.nid
        kwargs:                 Resource table values to be updated as dictionary
    Returns:
        instance of updated record
    """
    instance = (
        session.query(Resource)
        .filter_by(sierraId=sierraId, libraryId=libraryId)
        .one_or_none()
    )
    if instance:
        for key, value in kwargs.items():
            setattr(instance, key, value)
        return instance
    else:
        return None


def add_resource(session: Session, resource: Resource) -> Optional[Resource]:
    """
    Adds Resource record to db.

    Args:
        session:                `sqlalchemy.Session` instance
        resource:               `datastore.Resource` object

    Returns:
        `Resource` instance updated with `nid`
    """
    instance = (
        session.query(Resource)
        .filter_by(sierraId=resource.sierraId, libraryId=resource.libraryId)
        .one_or_none()
    )
    if not instance:
        session.add(resource)
    return instance
