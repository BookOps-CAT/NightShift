"""
This modules contains methods for transactions with data.db
"""

from .datastore import dal, LibrarySystem, BibCategory, UpgradeSource
from .datastore_values import LIB_SYS, BIB_CAT, UPGRADE_SRC


def insert(session, model, **kwargs):
    instance = model(**kwargs)
    session.add(instance)
    session.flush()
    return instance


def create_datastore(prod: bool = False):
    """
    Creates and prepopulates with initial data new datastore (data.db)
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
