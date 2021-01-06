# -*- coding: utf-8 -*-

"""
This module includes individual bot's operations
"""
import os
from typing import List, Tuple

from sqlalchemy.orm.session import Session as DatastoreSession
from bookops_worldcat import MetadataSession

import nightshift
from nightshift.bibs import name_marc_file, prepare_output_record, save2marc
from nightshift.datastore import ExportFile
from nightshift.api_nyp import get_nyp_sierra_bib_data
from nightshift.api_bpl import get_bpl_sierra_bib_data

from .export_file_parser import SierraExportReader
from .datastore_transactions import (
    enhance_resource,
    insert_resource,
    insert_export_file,
    retrieve_brief_records_bibnos,
    retrieve_never_queried_records,
    retrieve_records_not_queried_in_days,
    retrieve_resources_with_new_full_bib,
    update_resource,
)
from .datastore_values import LIB_SYS, BIB_CAT


def import_export_file_data(fh: str, session: DatastoreSession) -> ExportFile:
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


def import_platform_data(bibnos: List[int], session: DatastoreSession) -> None:
    """
    Queries NYPL Platform retrieving records with particular Sierra bib numbers
    and imports data to datastore

    Args:
        bibNos:                 list of Sierra bib numbers without 'b' prefix and
                                last digit check
    """
    datas = get_nyp_sierra_bib_data(bibnos)
    for d in datas:
        enhance_resource(session, data=d, library_system="nyp")


def import_solr_data(bibnos: List[int], session: DatastoreSession) -> None:
    """
    Queries BPL Solr retrieving records with particular Sierra bib numbers
    and imports data to datastore

    Args:
        bibNos:                 list of Sierra bib numbers without 'b' prefix and
                                without last digit check
    """
    datas = get_bpl_sierra_bib_data(bibnos)
    for d in datas:
        enhance_resource(session, data=d, library_system="bpl")


def import_sierra_data(fh: str, session: DatastoreSession) -> None:
    """
    Reads content of Sierra export and imports it to data.db

    Args:
        fh:                     Sierra export file handle as path
        session:                sqlalchemy session
    """
    # record and get table id of export file
    fh_rec = import_export_file_data(fh, session)

    data = SierraExportReader(fh)
    for res in data:
        insert_resource(session, **res._asdict(), exportFileId=fh_rec.efid)
    session.commit()


def retrieve_bibnos_for_enhancement(
    lib_sys: str, bib_cat: str, session: DatastoreSession
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

    sierra_bibnos = retrieve_brief_records_bibnos(session, lsid, bcid)

    return sierra_bibnos


def retrieve_eresource_records_for_worldcat_queries(
    lib_sys: str, session: DatastoreSession
) -> Tuple[int, str]:
    """
    Retrieves library system's records of indicated age that require
    update to full bib

    Args:
        lib_sys:                library system: 'nyp' or 'bpl'
        session:                sqlalchemy session

    Returns:
        list of sbid & did tuples
    """

    # determine ids of library system and bib category
    lsid = LIB_SYS[lib_sys]["lsid"]
    bcid = BIB_CAT["ere"]["bcid"]

    # 1st batch - never queried records
    for record in retrieve_never_queried_records(session, lsid, bcid):
        yield (record.sbid, record.did)

    # 2nd batch - one month old records
    for record in retrieve_records_not_queried_in_days(session, lsid, bcid):
        yield (record.sbid, record.did)

    # 3rd batch - 2 to 5 months records
    for record in retrieve_records_not_queried_in_days(
        session, lsid, bcid, bib_min_age=29, bib_max_age=173, query_cutoff_age=29
    ):
        yield (record.sbid, record.did)


def query_and_store_worldcat_eresources(
    query_data: Tuple,
    library_system: str,
    db_session: DatastoreSession,
    wc_session: MetadataSession,
):
    """
    Retrieves eligible records from datastore and runs queries in Worldcat

    Args:
        query_data:             tuple Resourse.sbid, Resource.did
        lib_sys:                library system: 'nyp' or 'bpl'
        db_session:             sqlalechemy session
        wc_session:             bookops_worldcat MetadataSession

    """
    bibNo, reserve_id = query_data
    response = nightshift.api_worldcat.find_matching_eresource(wc_session, reserve_id)
    if response:
        oclcNumber, full_bib = response
        # save full bib & update Resource record of given bibNo
        update_resource(db_session, bibNo, library_system, "bot", oclcNumber, full_bib)

    else:
        update_resource(db_session, bibNo, library_system, "bot", None, None)


def save_upgraded_records(
    db_session: DatastoreSession, library_system: str, bib_category: str
) -> None:
    """
    Retrieves ready/upgraded full records from datastore, manipulates them and
    saves MARC21 records to a file

    Args:
        db_session:             sqlalchemy session
        library_system:         library system: 'nyp' or 'bpl'
        bib_category:           category of bibs: 'ere' (e-resources) or 'pre' (print)
    """
    resources = retrieve_resources_with_new_full_bib(
        db_session, library_system, bib_category
    )

    print(f"Retrieved {len(resources)} to be written to a file.")
    for resource in resources:
        marc_record = prepare_output_record(resource)
        outfile = name_marc_file(library_system, resource.sierraFormatId)
        save2marc(f"./temp_files/{outfile}", marc_record)

        # update resource
