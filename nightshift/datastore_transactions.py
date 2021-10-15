# -*- coding: utf-8 -*-
from typing import List

from sqlalchemy import create_engine, func
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session

from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import (
    DataAccessLayer,
    Library,
    Resource,
    ResourceCategory,
    WorldcatQuery,
)


def init_db():
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


def last_worldcat_query(session: Session):
    """
    Creates a subquery with only the most recent WorldcatQuery db row

    Args:
        session:                `sqlalchemy.Session` instance

    Returns:
        subquery
    """
    subq = (
        session.query(
            WorldcatQuery.ResourceId,
            WorldcatQuery.nid.label("wq_nid"),
            func.max(WorldcatQuery.timestamp).label("wq_timestamp"),
        )
        .filter(WorldcatQuery.match == False)
        .subquery()
    )
    return subq


def retreive_full_bib_resources(session: Session, library):
    pass


def retrieve_new_resources(session: Session, libraryId: int) -> Result:
    """
    Retrieves resources that have been added to the db
    but has not been processed yet.
    The results are grouped by the resource category and ordered by
    resource nid.

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `Library.nid`

    Returns:
        `sqlalchemy.engine.Result` object
    """
    result = (
        session.query(Resource)
        .filter_by(libraryId=libraryId, status="open", deleted=False, queries=None)
        .group_by(Resource.resourceCategoryId, Resource.nid)
        .order_by(Resource.resourceCategoryId, Resource.nid)
        .all()
    )
    return result


def retrieve_matched_resources(session: Session, libraryId: int) -> Result:
    """
    Retrieves resources that have been matched to records in WorldCat,
    but not upgraded yet.
    The results are grouped by resource category and ordered by resource nid

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `Library.nid`

    Returns:
        `sqlalchemy.engine.Result` object
    """
    result = (
        session.query(Resource)
        .filter_by(libraryId=libraryId, status="matched", deleted=False)
        .group_by(Resource.resourceCategoryId, Resource.nid)
        .order_by(Resource.resourceCategoryId, Resource.nid)
        .all()
    )
    return result


def retrieve_scheduled_resources(
    session: Session, libraryId: int, resourceCategoryId: int, query_days: List[int]
) -> Result:
    """
    Retieves resources for a particular library and resource category that are scheduled
    for subsequent query based on query_days values

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `datastore.Library.nid`
        resourceCategoryId:     `datastore.ResourceCategory.nid`
        query_days:             list of days since creation of the resource to trigger
                                WorldCat query
    """
    result = (
        session.query(Resource)
        .filter_by(
            libraryId=libraryId,
            resourceCategoryId=resourceCategoryId,
            status="open",
            deleted=False,
        )
        .all()
    )
    return result


def update_resource(session, sierraId, libraryId, **kwargs):
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
