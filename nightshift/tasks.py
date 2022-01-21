# -*- coding: utf-8 -*-

"""
This module provides the manager methods to perform particular tasks
"""
from datetime import datetime
import logging
import os
from typing import Optional

from sqlalchemy.orm.session import Session

from nightshift.comms.worldcat import Worldcat
from nightshift.comms.sierra_search_platform import NypPlatform, BplSolr
from nightshift.comms.storage import get_credentials, Drive
from nightshift.datastore import Resource, WorldcatQuery
from nightshift.datastore_transactions import (
    add_event,
    add_output_file,
    add_resource,
    add_source_file,
    retrieve_processed_files,
    update_resource,
)
from nightshift.marc.marc_parser import BibReader
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

        if resource.status in ("staff_enhanced", "staff_deleted"):
            add_event(db_session, resource, outcome=resource.status)

    sierra_platform.close()

    # persist changes
    db_session.commit()


def enhance_and_output_bibs(
    db_session: Session,
    library: str,
    libraryId: int,
    resource_category: str,
    resources: list[Resource],
) -> None:
    """
    Manipulates downloaded WorldCat records, serializes them into MARC21 format
    and saves produced file to SFTP

    Args:
        db_session:                     'sqlalchemy.Session' instance
        library:                        'NYP' or 'BPL'
        libraryId:                      `nightshift.datastore.Library.nid`
        resource_category:              name of resource category being processed
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    # manipulate records and save to a temporary file
    temp_file, enhanced_resources = manipulate_and_serialize_bibs(
        db_session, library, resource_category, resources
    )

    # move temporary file to SFTP
    remote_file = transfer_to_drive(library, resource_category, temp_file)

    # finalize datastore resource status
    update_status_to_upgraded(db_session, libraryId, remote_file, enhanced_resources)


def get_worldcat_brief_bib_matches(
    db_session: Session, library: str, resources: list[Resource]
) -> None:
    """
    Queries Worldcat for given resources and persists responses
    in the database.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL'
        resources:                      list of `nigthtshift.datastore.Resource`
                                        instances
    """
    logger.info(f"Searching Worldcat for brief records for {len(resources)} resources.")

    with Worldcat(library) as worldcat:
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

                # add event for stats
                add_event(db_session, resource, outcome="worldcat_hit")
            else:
                resource.queries.append(
                    WorldcatQuery(match=False, response=response.as_json)
                )
                add_event(db_session, resource, outcome="worldcat_miss")
            db_session.commit()


def get_worldcat_full_bibs(
    db_session: Session, library: str, resources: list[Resource]
) -> None:
    """
    Requests full bibliographic records from MetadataAPI service and
    stores the responses in the db.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL'
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    logger.info(
        f"Downloading full records from WorldCat for {len(resources)} resources."
    )
    with Worldcat(library) as worldcat:
        results = worldcat.get_full_bibs(resources)
        for resource, response in results:
            update_resource(
                db_session, resource.sierraId, resource.libraryId, fullBib=response
            )

            # commit each full bib response in case something
            # breaks during this lenthy process;
            # this should save time if process need to be restarted
            db_session.commit()


def ingest_new_files(db_session: Session, library: str, library_id: int) -> None:
    """
    Imports to the database resources in a newly added files on the SFTP.

    Files on the remote server must have 'NYP' or 'BPL' in their names to be
    considered by this process (Sierra bib themselves do not have any system identifying
    data to rely on).

    Sierra Scheduler will be configured to output data dumps following this
    practice.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL' code
        libary_id:                      datastore library nid
    """
    drive_creds = get_credentials()
    with Drive(*drive_creds) as drive:

        # find files that have not been processed
        unproc_files = isolate_unprocessed_files(db_session, drive, library, library_id)
        logging.info(f"Found following unprocessed files: {unproc_files}.")

        # add records data to the db
        for handle in unproc_files:
            file_record = add_source_file(db_session, library_id, handle)
            logging.debug(f"Added SourceFile record for '{handle}': {file_record}")
            marc_target = drive.fetch_file(handle)
            marc_reader = BibReader(marc_target, library)

            n = 0
            for resource in marc_reader:
                n += 1
                resource.sourceId = file_record.nid
                add_resource(db_session, resource)

            db_session.commit()
            logging.info(f"Ingested {n} records from the file '{handle}'.")


def isolate_unprocessed_files(
    db_session: Session, drive: Drive, library: str, library_id: int
) -> list[str]:
    """
    Finds unprocessed files on the network drive but comparing db SourceFile
    record handles with handles retrieved from the drive.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        drive:                          `nightshfit.comms.storage.Drive` client instance
        library:                        'NYP' or 'BPL'
        library_id:                     datastore library id

    Returns:
        list of remote unprocessed file handles
    """
    remote_files = drive.list_src_directory()

    # select only files with library code and .pout extension
    library_remote_files = [
        file for file in remote_files if library in file and ".pout" in file
    ]
    logging.debug(
        f"Found following remote files for {library}: {library_remote_files}."
    )

    proc_files = retrieve_processed_files(db_session, library_id)

    return [f for f in library_remote_files if f not in proc_files]


def manipulate_and_serialize_bibs(
    db_session: Session,
    library: str,
    resource_category: str,
    resources: list[Resource],
    file_path: str = "temp.mrc",
) -> tuple[str, list[Resource]]:
    """
    Merges Sierra brief bibs data with WroldCat full bib,
    and serializes them into MARC21 format

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL'
        resource_category:              name of resource cateogry ('ebook', etc.)
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
        file_path:                      path of the file where records are saved

    Returns:
        tuple (output file, list of enhanced resources)
    """
    enhanced_resources = []
    skipped_resources = []

    for resource in resources:
        be = BibEnhancer(resource)
        be.manipulate()
        if be.bib is not None:
            be.save2file(file_path)
            logger.debug(
                f"{library} b{resource.sierraId}a has been output to '{file_path}'."
            )
            enhanced_resources.append(resource)
        else:
            # update to blank state to allow later date query
            update_resource(
                db_session,
                resource.sierraId,
                resource.libraryId,
                oclcMatchNumber=None,
                fullBib=None,
            )
            logger.warning(
                f"{library} b{resource.sierraId}a enhancement incomplete. Skipping."
            )
            skipped_resources.append(resource)

    logger.info(
        f"Enhanced and serialized {len(enhanced_resources)} and skipped "
        f"{len(skipped_resources)} {library} {resource_category} record(s)."
    )
    db_session.commit()

    # check if any resources have been actually saved to a file
    if len(enhanced_resources) > 0:
        out_file = file_path
    else:
        out_file = None

    return (out_file, enhanced_resources)


def transfer_to_drive(
    library: str, resource_category: str, src_file: str
) -> Optional[str]:
    """
    Transfers local temporary MARC21 file to network drive.
    Arguments `library` and `resource_category` is used to name the MARC file on the
    network drive.
    After successful transfer the temporary file is deleted.

    Args:
        library:                        library code
        resource_category:              name of resource category being processed
        src_file:                      temporary local file to be transfered

    Returns:
        SFTP file handle
    """
    today = datetime.now().date()
    remote_file_name_base = f"{today:%y%m%d}-{library}-{resource_category}"
    remote_file = None

    creds = get_credentials()
    with Drive(*creds) as drive:
        try:
            if src_file is not None:
                remote_file = drive.output_file(src_file, remote_file_name_base)
            else:
                logger.info("No source file to output to SFTP.")
        except DriveError:
            raise

    # clean up temp/src file after job completed
    try:
        os.remove(src_file)
    except (FileNotFoundError, TypeError):
        pass
    except OSError as exc:
        logger.error(
            f"Unable to delete '{src_file}' file after completing the job. Error {exc}"
        )
        raise
    else:
        return remote_file


def update_status_to_upgraded(
    db_session: Session,
    library_id: int,
    out_file_handle: str,
    resources: list[Resource],
) -> None:
    """
    Upgrades given resources status to "bot_enhanced" and records output file id.
    Adds appropriate event record to the database.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library_id:                     datastore Library id
        out_file_handle:                handle of the output file
        resources:                      list of `nightshift.datastore.Resource`
                                        instances
    """
    if out_file_handle is not None:

        logging.info(f"Updating {len(resources)} resources status to 'bot_enhanced'.")

        out_file_record = add_output_file(db_session, library_id, out_file_handle)

        for resource in resources:
            update_resource(
                db_session,
                resource.sierraId,
                resource.libraryId,
                status="bot_enhanced",
                outputId=out_file_record.nid,
                enhanceTimestamp=datetime.utcnow(),
            )

            add_event(db_session, resource, outcome="bot_enhanced")

        db_session.commit()
    else:
        logging.info(
            "Skipping resources enhancement status update. "
            "No SFTP output file this time."
        )
