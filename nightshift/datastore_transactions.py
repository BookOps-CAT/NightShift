# -*- coding: utf-8 -*-

from nightshift.datastore import Resource


def insert_or_ignore(session, model, **kwargs):
    """
    Adds a new record to given table (model) or ignores if the same.

    Args:
        session:                sqlalchemy.Session instance
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
        session:                sqlalchemy.Session instance
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
