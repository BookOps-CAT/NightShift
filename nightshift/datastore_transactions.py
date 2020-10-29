"""
This modules contains methods for transactions with data.db
"""

from .datastore import dal, LibrarySystem, BibCategory, UpgradeSource, Resource
from .datastore_values import LIB_SYS, BIB_CAT, UPGRADE_SRC


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


def create_datastore(prod: bool = False):
    """
    Creates and prepopulates with initial data new datastore (data.db)

    Args:
        prod:               if True creates production data.db,
                            if False creates in-memody database
    """

    if not prod:
        dal.conn_string = "sqlite:///:memory:"
    dal.connect()

    session = dal.Session()

    # insert data
    for values in LIB_SYS.values():
        insert(session, LibrarySystem, **values)
    for values in BIB_CAT.values():
        insert(session, BibCategory, **values)
    for values in UPGRADE_SRC.values():
        insert(session, UpgradeSource, **values)

    session.commit()
    session.close()
