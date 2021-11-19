# -*- coding: utf-8 -*-

"""
This module provides methods for the manager to execute for a particular process
"""
import logging

from sqlalchemy.orm.session import Session

from nightshift.comms.worldcat import Worldcat
from nightshift.comms.sierra_search_platform import NypPlatform, BplSolr
from nightshift.datastore import Resource, WorldcatQuery
from nightshift.datastore_transactions import update_resource


logger = logging.getLogger("nightshift")


def check_resources_sierra_state(
    db_session: Session, library: str, resources: list[Resource]
) -> None:
    """
    Checks and updates status & suppression of records using NYPL Platform & BPL Solr

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL'
        resources:                      list of `datastore.Resource` instances
    """
    if library == "NYP":
        sierra_platform = NypPlatform()
    elif library == "BPL":
        sierra_platform = BplSolr()
    else:
        logger.error(
            f"Invalid library argument passed: '{library}'. Must be 'NYP' or 'BPL'."
        )
        raise ValueError("Invalid library argument.")

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
    stores the responses.

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
