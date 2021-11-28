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
from nightshift.ns_exceptions import DriveError


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

    logging.info(f"Checking {library} Sierra status for {len(resources)} resources.")

    for resource in resources:
        response = sierra_platform.get_sierra_bib(resource.sierraId)
        resource.suppressed = response.is_suppressed()
        resource.status = response.get_status()

    sierra_platform.close()

    # persist changes
    db_session.commit()


def enhance_and_output_bibs(library: str, resources: list[Resource]) -> None:
    """
    Manipulates downloaded WorldCat records and serializes them
    into MARC21 format

    Args:
        library:                        'NYP' or 'BPL'
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    logging.info(f"Enhancing {library} {len(resources)} resources.")
    for resource in resources:
        be = BibEnhancer(resource)
        be.manipulate()
        be.save2file()
        logger.debug(f"{library} b{resource.sierraId}a has been output to 'temp.mrc'.")


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
    logger.info(f"Searching Worldcat for brief records for {len(resources)} resources.")

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
    logger.info(
        f"Downloading full records from WorldCat for {len(resources)} resources."
    )
    results = worldcat.get_full_bibs(resources)
    for resource, response in results:
        update_resource(
            db_session, resource.sierraId, resource.libraryId, fullBib=response
        )
        db_session.commit()


def transfer_to_drive(
    library: str, resource_category: str, temp_file: str = "temp.mrc"
) -> None:
    """
    Transfers local temporary MARC21 file to network drive.
    Arguments `library` and `resource_category` is used to name the MARC file on the
    network drive.
    After successful transfer the temporary file is deleted.

    Args:
        library:                        library code
        resource_category:              name of resource category being processed
        temp_file:                      temporary local file to be transfered
    """
    # def construct_remote_file_handle(library, resource_category, timestamp):

    today = datetime.now().date()
    remote_file_name_base = f"{today:%y%m%d}-{library}-{resource_category}"

    creds = get_credentials()
    with Drive(*creds) as drive:
        drive.output_file(temp_file, remote_file_name_base)
        logger.info(
            f"{library} {resource_category} records have been output to remote "
            f"'{remote_file_name_base}'."
        )

    # clean up after job completed
    try:
        os.remove(temp_file)
    except OSError as exc:
        logger.error(
            f"Unable to delete '{temp_file}' file after completing the job. Error {exc}"
        )
        raise


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
