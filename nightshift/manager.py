"""
This module incldues top level processes to be performed by the app
"""
import logging
from typing import List

from bookops_worldcat.errors import WorldcatSessionError
from sqlalchemy.orm.session import Session

from nightshift.comms.worldcat import Worldcat
from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import session_scope, Resource, WorldcatQuery
from nightshift.datastore_transactions import (
    retrieve_full_bib_resources,
    retrieve_matched_resources,
    retrieve_new_resources,
    retrieve_older_open_resources,
    update_resource,
)


logger = logging.getLogger("nightshift")


def get_worldcat_brief_bib_matches(
    db_session: Session, worldcat: Worldcat, resources: List[Resource]
) -> None:
    """
    Queries Worldcat for given resources and persists responses
    in the database.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        worldcat:                       `nightshift.comms.worldcat.Worldcat` instance
        resources:                      `sqlachemy.engine.Result` instance
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
                status="matched",
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
    db_session: Session, worldcat: Worldcat, resources: List[Resource]
) -> None:
    """
    Requests full bibliographic records from MetadataAPI service and
    stores the responses.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        worldcat:                       `nightshift.comms.worldcat.Worldcat` instance
        resources:                      `sqlalchemy.engine.Result` instance
    """
    results = worldcat.get_full_bibs(resources)
    for resource, response in results:
        update_resource(
            db_session, resource.sierraId, resource.libraryId, fullBib=response
        )
        db_session.commit()


# def process_resources() -> None:
#     """
#     Processes newly added and older open resources.
#     """
#     with session_scope() as db_session:
#         # retrieve today's resouces & search WorldCat
#         for library, libdata in LIBRARIES.items():
#             logger.info(f"Processing {(library).upper()} new resources.")

#             # search newly added resources
#             resources = retrieve_new_resources(db_session, libdata["nid"])
#             logger.debug(
#                 f"Retrieved {len(resources)} new {(library).upper()} for querying."
#             )

#             # perform searches for each resource and store results
#             get_worldcat_brief_bib_matches(db_session, library, resources)

#             # update status of older resources
#             for res_category, catdata in RESOURCE_CATEGORIES.items():
#                 for ageMin, ageMax in catdata["query_days"]:
#                     resources = retrieve_older_open_resources(
#                         db_session,
#                         libdata["nid"],
#                         minAge,
#                         maxAge,
#                     )
#                     # query Sierra to update their status if changed
#                     # here
#                     # this will be particulary important for print materials
#                     pass

#             # search again older resources dropping any resources already cataloged
#             # or deleted/suppessed
#             for res_category, catdata in RESOURCE_CATEGORIES.items():
#                 for ageMin, ageMax in catdata["query_days"]:
#                     resources = retrieve_older_open_resources(
#                         db_session,
#                         libdata["nid"],
#                         catdata["nid"],
#                         catdata["query_days"],
#                     )

#                 # perform download of full records for matched resources
#                 resources = retrieve_matched_resources(db_session, libdata["nid"])
#                 get_worldcat_full_bibs(db_session, library, resources)

#                 # serialize as MARC21 and output to a file of enhanced bibs
#                 resources = retrieve_full_bib_resources(db_session, library)
#                 ...

#                 # output MARC records to the network drive
#                 # notify (loggly?)

#         # perform db maintenance
#         # mark resources as expired
