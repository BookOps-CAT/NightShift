"""
This module incldues top level processes to be performed by the app
"""
import logging


from nightshift.constants import library_by_id, RESOURCE_CATEGORIES
from nightshift.datastore import session_scope
from nightshift.datastore_transactions import (
    retrieve_new_resources,
    retrieve_open_matched_resources_with_full_bib_obtained,
    retrieve_open_matched_resources_without_full_bib,
    retrieve_open_older_resources,
)
from nightshift.tasks import (
    check_resources_sierra_state,
    enhance_and_output_bibs,
    get_worldcat_brief_bib_matches,
    get_worldcat_full_bibs,
    transfer_to_drive,
    upgrade_status_to_upgraded,
)


logger = logging.getLogger("nightshift")


def process_resources() -> None:
    """
    Processes newly added and older open resources.


    """
    with session_scope() as db_session:

        for lib_nid, library in library_by_id():

            # ingest new resources if any
            # here

            # search newly added resources
            resources = retrieve_new_resources(db_session, lib_nid)
            logger.info(
                f"Retrieved {len(resources)} new {library} resources for "
                "querying WorldCat."
            )
            # perform searches for each resource and store results
            get_worldcat_brief_bib_matches(db_session, library, resources)

            # check & update status of older resources if changed in Sierra
            for res_category, res_cat_data in RESOURCE_CATEGORIES.items():
                for age_min, age_max in res_cat_data["query_days"]:
                    resources = retrieve_open_older_resources(
                        db_session,
                        lib_nid,
                        res_cat_data["nid"],
                        age_min,
                        age_max,
                    )
                    logger.info(
                        f"Retrieved {len(resources)} {library} {res_category} "
                        "older resources to query Sierra for state change."
                    )
                    # query Sierra platform to update their status if changed
                    check_resources_sierra_state(db_session, library, resources)

            # search again older resources dropping any resources already cataloged
            # or deleted
            for res_category, res_cat_data in RESOURCE_CATEGORIES.items():
                for ageMin, ageMax in res_cat_data["query_days"]:
                    resources = retrieve_open_older_resources(
                        db_session,
                        lib_nid,
                        res_cat_data["nid"],
                        age_min,
                        age_max,
                    )
                    logger.info(
                        f"Retrieved {len(resources)} {library} {res_category} "
                        "ready to be queried in WorldCat."
                    )

                    # perform WorldCat searches for open older resources
                    get_worldcat_brief_bib_matches(db_session, library, resources)

            # perform download of full records for matched resources
            resources = retrieve_open_matched_resources_without_full_bib(
                db_session, lib_nid
            )
            logger.info(
                f"Retrieved {len(resources)} {library} {res_category} resources ready "
                "for downloading full WorldCat records."
            )
            get_worldcat_full_bibs(db_session, library, resources)

            # serialize as MARC21 and output to a file of enhanced bibs
            for res_category, res_cat_data in RESOURCE_CATEGORIES.items():
                resources = retrieve_open_matched_resources_with_full_bib_obtained(
                    db_session, library, res_cat_data["nid"]
                )

                # manipulate Worldcat bibs
                enhance_and_output_bibs(resources)
                logger.info(
                    f"Enhancing {len(resources)} {library} {res_category} records."
                )

                # output MARC records to the network drive
                transfer_to_drive(library, res_category)

                # notify (loggly?)

                # update resources as upgraded
                upgrade_status_to_upgraded(db_session, resources)


def perform_db_maintenance():
    pass
    # mark resources as expired
