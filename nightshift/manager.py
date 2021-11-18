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
    retrieve_new_resources,
    retrieve_open_matched_resources_with_full_bib_obtained,
    retrieve_open_matched_resources_without_full_bib,
    retrieve_open_older_resources,
    update_resource,
)
from nightshift.tasks import get_worldcat_brief_bib_matches, get_worldcat_full_bibs


logger = logging.getLogger("nightshift")


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
