# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session

from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import Library, Resource, ResourceCategory, DataAccessLayer


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


def retrieve_new_resources(session: Session) -> Result:
    """
    Retrieves resources that have been added to the db
    but has not been processed yet

    Args:
        session:                `sqlalchemy.Session` instance

    Returns:
        `sqlalchemy.engine.Result` object
    """
    result = (
        session.query(Resource)
        .filter_by(status="open", deleted=False, queries=None)
        .all()
    )
    return result
