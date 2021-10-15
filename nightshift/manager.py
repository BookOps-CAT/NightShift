"""
This module incldues top level processes to be performed by the app
"""
from bookops_worldcat.errors import WorldcatSessionError


from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift.datastore import session_scope
from nightshfit.datastore_transactions import (
    retrieve_matched_resources,
    retrieve_new_resources,
    retrieve_scheduled_resources,
    update_resource,
)
from nightshift import worldcat
from nightshift.marc.marc_parser import BibReader


def search_worldcat(db_session, library, resources) -> None:
    """
    Queries Worldcat for given resources and persists responses
    in the database.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL'
        resources:                      `sqlachemy.engine.Result` instance
    """
    results = worldcat.search_batch(library=library, resources=resources)
    try:
        for resource, response in results:
            if response is not None:
                oclcNumber = worldcat.get_oclc_number(response)
                queries = resource.queries.append(
                    resourceId=resource.nid,
                    match=True,
                    response=response.json(),
                )
                instance = update_resource(
                    db_session,
                    resource.sierraId,
                    resource.libraryId,
                    oclcMatchNumber=oclcNumber,
                    status="matched",
                    queries=queries,
                )

            else:
                queries = resource.queries.append(resourceId=resource.nid, match=False)
                instance = update_resource(
                    db_session,
                    resource.sierraId,
                    resource.libraryId,
                    queries=queries,
                )
            db_session.commit()
            if instance is None:
                # log failed update
                pass
    except WorldcatSessionError:
        # log exception
        break


def get_worldcat_full_bibs(db_session, library, resources):
    """
    Requests full bibliographic records from MetadataAPI service and
    stores the responses.

    Args:
        db_session:                     `sqlalchemy.Session` instance
        library:                        'NYP' or 'BPL'
        resources:                      `sqlalchemy.engine.Result` instance
    """
    results = worldcat.get_full_bibs(library, resources)
    for resource, response in results:
        instance = update_resource(
            db_session, resource.sierraId, resource.libraryId, fullBib=response.content
        )
        if instance is None:
            # log error
            continue


def process_resources():
    """
    Processes any newly added resources.
    """
    with session_scope() as db_session:
        # retrieve today's resouces & search WorldCat
        for library, libdata in LIBRARIES.items():

            # search newly added resources
            resources = retrieve_new_resources(db_session, libdata["nid"])
            # perform searches for each resource and store results
            search_worldcat(db_session, library, resources)

            # update status of older resources
            for res_category, catdata in RESOURCE_CATEGORIES.items():
                resources = retrieve_scheduled_resources(
                    db_session, libdata["nid"], catdata["nid"], catdata["query_days"]
                )
                # query Sierra to update their status if changed
                # here
                pass

            # search again older resources
            for res_category, catdata in RESOURCE_CATEGORIES.items():
                resources = retrieve_scheduled_resources(
                    db_session, libdata["nid"], catdata["nid"], catdata["query_days"]
                )

            # perform download of full records for matched resources
            resources = retrieve_matched_resources(db_session, libdata["nid"])
            get_worldcat_full_bibs(db_session, library, resources)

            # serialize as MARC21 and output to a file upgraded resources
            resources = retrieve_full_bib_resources(db_session, library)

    # produce MARC records
    # output MARC records to the network drive
    # notify

    # perform db maintenance
    # mark resources as expired
