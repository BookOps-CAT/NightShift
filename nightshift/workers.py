"""
This module includes individual bot's operations
"""
import os
from typing import Type

import sqlalchemy

from .export_file_parser import SierraExportReader
from .datastore_transactions import insert_resource, insert_export_file


def record_export_file_data(fh: str, session: Type[sqlalchemy.orm.session.Session]):
    """
    Inserts export file record to ExportFile table

    Args:
        fh:
    """
    handle = os.path.basename(fh)
    record = insert_export_file(session, handle=handle)
    session.commit()
    return record


def import_sierra_data(fh: str, session: Type[sqlalchemy.orm.session.Session]):
    # record and get table id of export file
    fh_rec = record_export_file_data(fh, session)

    data = SierraExportReader(fh)
    for res in data:
        insert_resource(session, **res._asdict(), exportFileId=fh_rec.efid)
    session.commit()
