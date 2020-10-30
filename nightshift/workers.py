# -*- coding: utf-8 -*-

"""
This module includes individual bot's operations
"""
import os
from typing import List, Type

import sqlalchemy

from .export_file_parser import SierraExportReader
from .datastore_transactions import insert_resource, insert_export_file, retrieve_bibnos
from .datastore_values import LIB_SYS, BIB_CAT


def record_export_file_data(fh: str, session: Type[sqlalchemy.orm.session.Session]):
    """
    Inserts export file record to ExportFile table

    Args:
        fh:                     Sierra export file handle as path
        session:                sqlalchemy session

    Returns:
        `nightshift.datastore.ExportFile` instance
    """
    handle = os.path.basename(fh)
    record = insert_export_file(session, handle=handle)
    session.commit()
    return record


def import_sierra_data(fh: str, session: Type[sqlalchemy.orm.session.Session]):
    """
    Reads content of Sierra export and imports it to data.db

    Args:
        fh:                     Sierra export file handle as path
        session:                sqlalchemy session
    """
    # record and get table id of export file
    fh_rec = record_export_file_data(fh, session)

    data = SierraExportReader(fh)
    for res in data:
        insert_resource(session, **res._asdict(), exportFileId=fh_rec.efid)
    session.commit()


def retrieve_bibnos_for_enhancement(
    lib_sys: str, bib_cat: str, session: Type[sqlalchemy.orm.session.Session]
) -> List[int]:
    """
    Queries datastore and returns Sierra bib numbers (sbid) of records
    that are missing Overdrive resource id

    Args:
        lib_sys:                library system: 'nyp' or 'bpl'
        bib_cat:                bib category: 'ere' (e-resouces) or
                                'pre' (English print)
        session:                sqlalchemy session

    Returns:
        list of bibNos
    """
    lsid = LIB_SYS[lib_sys]["lsid"]
    bcid = BIB_CAT[bib_cat]["bcid"]

    sierra_bibnos = retrieve_bibnos(session, lsid, bcid)

    return sierra_bibnos
