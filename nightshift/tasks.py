# -*- coding: utf-8 -*-

"""
This module provides methods for the manager to execute for a particular process
"""
from datetime import datetime
import logging
import os

from sqlalchemy.orm.session import Session

from nightshift.comms.worldcat import Worldcat
from nightshift.comms.sierra_search_platform import NypPlatform, BplSolr
from nightshift.comms.storage import get_credentials, Drive
from nightshift.datastore import Resource, WorldcatQuery
from nightshift.datastore_transactions import update_resource
from nightshift.marc.marc_writer import BibEnhancer


logger = logging.getLogger("nightshift")


def check_resources_sierra_state(
    db_session: Session, library: str, resources: list[Resource]
) -> None:
    """
    Checks and updates status & suppression of records using NYPL Platform & BPL Solr.
    This method updates resources in the databse.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL'
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    if library == "NYP":
        sierra_platform = NypPlatform()
    elif library == "BPL":
        sierra_platform = BplSolr()
    else:
        logger.error(
            f"Invalid library argument passed: '{library}'. Must be 'NYP' or 'BPL'."
        )
        raise ValueError("Invalid library argument. Must be 'NYP' or 'BPL'")

    for resource in resources:
        response = sierra_platform.get_sierra_bib(resource.sierraId)
        resource.suppressed = response.is_suppressed()
        resource.status = response.get_status()

    sierra_platform.close()

    # persist changes
    db_session.commit()


def get_worldcat_brief_bib_matches(
    db_session: Session, worldcat: Worldcat, resources: list[Resource]
) -> None:
    """
    Queries Worldcat for given resources and persists responses
    in the database.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        worldcat:                       `nightshift.comms.worldcat.Worldcat` instance
        resources:                      list of `nigthtshift.datastore.Resource`
                                        instances
    """
    logger.info(f"Searching Worldcat.")

    results = worldcat.get_brief_bibs(resources=resources)
    for resource, response in results:
        if response.is_match:
            instance = update_resource(
                db_session,
                resource.sierraId,
                resource.libraryId,
                oclcMatchNumber=response.oclc_number,
            )
            instance.queries.append(
                WorldcatQuery(
                    resourceId=resource.nid,
                    match=True,
                    response=response.as_json,
                )
            )
        else:
            resource.queries.append(
                WorldcatQuery(match=False, response=response.as_json)
            )

        db_session.commit()


def get_worldcat_full_bibs(
    db_session: Session, worldcat: Worldcat, resources: list[Resource]
) -> None:
    """
    Requests full bibliographic records from MetadataAPI service and
    stores the responses in the db.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        worldcat:                       `nightshift.comms.worldcat.Worldcat` instance
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    results = worldcat.get_full_bibs(resources)
    for resource, response in results:
        update_resource(
            db_session, resource.sierraId, resource.libraryId, fullBib=response
        )
        db_session.commit()


def enhance_and_output_bibs(resources: list[Resource]) -> None:
    """
    Manipulates downloaded WorldCat records and serializes them
    into MARC21 format

    Args:
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    for resource in resources:
        be = BibEnhancer(resource)
        be.manipulate()
        be.save2file()


def transfer_to_drive(library, resource_category: str) -> None:
    """
    Transfers local temporary MARC21 file to network drive.
    Argument `resource_category` is used to name the MARC file on the
    network drive.
    After successful transfer the temporary file is deleted.

    Args:
        library:                        library code
        resource_category:              name of resource category being processed.
    """
    # def construct_remote_file_handle(library, resource_category, timestamp):

    timestamp = datetime.now().date()
    remote_file_handle = f"{library}{resource_category}{timestamp}"

    creds = get_credentials()
    with Drive(*creds) as drive:
        # try:
        drive.output_file("temp.mrc", remote_file_handle)

    os.remove("temp.mrc")


def update_status_to_upgraded(db_session: Session, resources: list[Resource]) -> None:
    """
    Upgrades resources status to "upgraded_bot"

    Args:
        db_session:                     `sqlalchemy.Session` instance
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    for resource in resources:
        update_resource(
            db_session, resource.sierraId, resource.libraryId, status="upgraded_bot"
        )
    db_session.commit()
