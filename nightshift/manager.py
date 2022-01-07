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


from nightshift import tasks


logger = logging.getLogger("nightshift")


def process_resources() -> None:
    """
    Processes newly added and older open resources.

    temp.mrc deleted after transfering content to SFTP (`tasks.transfer_to_drive()`)

    """
    with session_scope() as db_session:

        for lib_nid, library in library_by_id().items():

            logger.info(f"Processing {library} resources.")

            # ingest new resources
            tasks.ingest_new_files(db_session, library, lib_nid)
            logger.info(f"New {library} remote files have been ingested.")

            # search newly added resources
            resources = retrieve_new_resources(db_session, lib_nid)

            # perform searches for each resource and store results
            if resources:
                tasks.get_worldcat_brief_bib_matches(db_session, library, resources)
                logger.info(
                    f"Obtaining Worldcat matches for {len(resources)} {library} "
                    "new resources completed."
                )

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
                    # query Sierra platform to update their status if changed
                    if resources:
                        tasks.check_resources_sierra_state(
                            db_session, library, resources
                        )
                        logger.info(
                            f"Checking Sierra status of {len(resources)} {library} "
                            f"{res_category} older resources completed."
                        )

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

                    # perform WorldCat searches for open older resources
                    if resources:
                        tasks.get_worldcat_brief_bib_matches(
                            db_session, library, resources
                        )
                        logger.info(
                            f"Obtainig WorldCat matches for {len(resources)} "
                            f"{library} {res_category} older resources completed."
                        )

            # perform download of full records for matched resources
            resources = retrieve_open_matched_resources_without_full_bib(
                db_session, lib_nid
            )
            if resources:
                tasks.get_worldcat_full_bibs(db_session, library, resources)
                logger.info(
                    f"Downloading {len(resources)} {library} {res_category} "
                    "full records from WorldCat completed."
                )

            # serialize as MARC21 and output to a file of enhanced bibs
            for res_category, res_cat_data in RESOURCE_CATEGORIES.items():
                resources = retrieve_open_matched_resources_with_full_bib_obtained(
                    db_session, lib_nid, res_cat_data["nid"]
                )

                # manipulate Worldcat bibs
                if resources:
                    tasks.enhance_and_output_bibs(library, resources)
                    logger.info(
                        f"Enhancing {len(resources)} {library} {res_category} "
                        "resources completed."
                    )

                    # output MARC records to the network drive
                    file = tasks.transfer_to_drive(library, res_category)
                    logger.info(
                        f"Transfering {len(resources)} {library} {res_category} "
                        f"resources to the network drive completed ({file})."
                    )

                    # update resources as upgraded
                    tasks.update_status_to_upgraded(
                        db_session, lib_nid, file, resources
                    )
                    logger.info(
                        f"Upgrading status of {len(resources)} {library} "
                        f"{res_category} resources completed."
                    )


def perform_db_maintenance():
    pass
    # mark resources as expired
