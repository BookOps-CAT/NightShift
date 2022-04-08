"""
This module incldues top level processes to be performed by the app
"""
import logging


from nightshift.constants import RESOURCE_CATEGORIES
from nightshift.datastore import session_scope
from nightshift.datastore_transactions import (
    add_event,
    delete_resources,
    library_by_id,
    resource_category_by_name,
    retrieve_new_resources,
    retrieve_expired_resources,
    retrieve_open_matched_resources_with_full_bib_obtained,
    retrieve_open_matched_resources_without_full_bib,
    retrieve_open_older_resources,
    set_resources_to_expired,
)


from nightshift.tasks import Tasks


logger = logging.getLogger("nightshift")


def process_resources() -> None:
    """
    Processes newly added and older not enhanced yet resources.

    Ingests brief bibliographic and order records exported from Sierra and searches
    WorldCat to find good full records to be used to upgrade Sierra bib. Performs
    such searches several times until a match is found or a record ages out from the
    process. Finally, outputs enhanced records to SFTP/shared drive as a MARC21 file.

    1. Discovers new Sierra dump files on SFTP and adds records to the database.
        A Sierra Scheduler job should be configured to create a list of newly added
        records that needs automated cataloging, and to export such records to SFTP.
    2. Searches WorldCat for these newly added resources and records any matching
        OCLC numbers.
    3. Selects older, not enhanced yet resources that can be queried in WorldCat
        according to their schedule (encoded in 'queryDays' of the
        `constants.RESOURCE_CATEGORIES`) and checks via NYPL Platform or
        BPL Solr API if their status have changed since previous query (enhanced
        by staff, deleted, or suppressed). Records changes in status in the database.
    4. Selects again older and not enhanced resources and searches for matches in
        WorldCat. Records any matching OCLC numbers.
    5. Downloads full bibliographic records for resources that were successfully matched
    6. Manipulates, serializes to MARC21 and outputs to SFTP resources with full bibs
        from WorldCat
    7. Updates status of resources that were succesfully ouput to SFTP completing the
        process.

    """
    with session_scope() as db_session:

        lib_idx = library_by_id(db_session)
        res_cat = resource_category_by_name(db_session)

        for lib_nid, library in lib_idx.items():

            logger.info(f"Processing {library} resources.")

            # initiate Task client for the library
            tasks = Tasks(db_session, library, lib_nid, res_cat)

            # ingest new resources
            tasks.ingest_new_files()
            logger.info(f"New {library} remote files have been ingested.")

            # search newly added resources
            resources = retrieve_new_resources(db_session, lib_nid)

            # perform searches for each resource and store results
            if resources:
                tasks.get_worldcat_brief_bib_matches(resources)
                logger.info(
                    f"Obtaining Worldcat matches for {len(resources)} {library} "
                    "new resources completed."
                )

            # check & update status of older resources if changed in Sierra
            for res_category, res_cat_data in res_cat.items():
                for age_min, age_max in res_cat_data.queryDays:
                    resources = retrieve_open_older_resources(
                        db_session,
                        lib_nid,
                        res_cat_data.nid,
                        age_min,
                        age_max,
                    )
                    # query Sierra platform to update their status if changed
                    if resources:
                        tasks.check_resources_sierra_state(resources)
                        logger.info(
                            f"Checking Sierra status of {len(resources)} {library} "
                            f"{res_category} older resources completed."
                        )

            # search again older resources dropping any resources already enhanced
            # or deleted
            for res_category, res_cat_data in res_cat.items():
                for ageMin, ageMax in res_cat_data.queryDays:
                    resources = retrieve_open_older_resources(
                        db_session,
                        lib_nid,
                        res_cat_data.nid,
                        age_min,
                        age_max,
                    )

                    # perform WorldCat searches for open older resources
                    if resources:
                        tasks.get_worldcat_brief_bib_matches(resources)
                        logger.info(
                            f"Obtainig WorldCat matches for {len(resources)} "
                            f"{library} {res_category} older resources completed."
                        )

            # perform download of full records for matched resources
            resources = retrieve_open_matched_resources_without_full_bib(
                db_session, lib_nid
            )
            if resources:
                tasks.get_worldcat_full_bibs(resources)
                logger.info(
                    f"Downloading {len(resources)} {library} {res_category} "
                    "full records from WorldCat completed."
                )

            # serialize as MARC21 and output to a file of enhanced bibs
            for res_category, res_cat_data in res_cat.items():
                resources = retrieve_open_matched_resources_with_full_bib_obtained(
                    db_session, lib_nid, res_cat_data.nid
                )

                # manipulate Worldcat bibs, serialize to MARC21 and save to SFTP
                if resources:
                    tasks.enhance_and_output_bibs(res_category, resources)

                    logger.info(
                        f"Enhancement and serializaiton of {library} {res_category} "
                        "complete."
                    )


def perform_db_maintenance() -> None:
    """
    Marks resources as expired or deletes them if past certain age.
    """
    with session_scope() as db_session:

        res_cat = resource_category_by_name(db_session)

        for res_category, res_cat_data in res_cat.items():

            # set to expired
            expiration_age = res_cat_data.queryDays[-1][1]

            # record status change for statistical purposes in Event table
            resources = retrieve_expired_resources(
                db_session, res_cat_data.nid, expiration_age
            )
            for resource in resources:
                add_event(db_session, resource, status="expired")

            # change status in Resource table
            tally = set_resources_to_expired(
                db_session, res_cat_data.nid, age=expiration_age
            )
            db_session.commit()
            logger.info(
                f"Changed {tally} {res_category} resource(s) status to 'expired'."
            )

            # delete resources 3 months older after they expired
            deletion_age = expiration_age + 90
            tally = delete_resources(db_session, res_cat_data.nid, deletion_age)
            db_session.commit()
            logger.info(
                f"Deleted {tally} {res_category} resource(s) older than "
                f"{deletion_age} days from the database."
            )
