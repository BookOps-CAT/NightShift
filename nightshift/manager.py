"""
This module incldues top level processes to be performed by the app
"""
from bookops_worldcat.errors import WorldcatSessionError


from nightshift.constants import LIBRARIES
from nightshift.datastore import session_scope
from nightshfit.datastore_transactions import (
    retrieve_matched_resources,
    retrieve_new_resources,
    update_resource,
)
from nightshift import worldcat
from nightshift.marc.marc_parser import BibReader


def process_new_resources():
    """
    Processes any newly added resources.
    """
    with session_scope() as db_session:
        # retrieve today's resouces & search WorldCat
        for library, libdata in LIBRARIES.items():
            resources = retrieve_new_resources(db_session, libdata["nid"])

            try:
                results = worldcat.search_batch(library=library, resources=resources)
                for resource, response in results:
                    if response is not None:
                        oclcNumber = worldcat.get_oclc_number(response)
                        queries = resource.queries.append(
                            resourceId=resource.nid,
                            match=True,
                            response=response.json(),
                        )
                        result = update_resource(
                            db_session,
                            resource.sierraId,
                            resource.libraryId,
                            oclcMatchNumber=oclcNumber,
                            status="matched",
                            queries=queries,
                        )

                    else:
                        queries = resource.queries.append(
                            resourceId=resource.nid, match=False
                        )
                        result = update_resource(
                            db_session,
                            resource.sierraId,
                            resource.libraryId,
                            queries=queries,
                        )
                    db_session.commit()
                    if not result:
                        # log the issue
                        pass
            except WorldcatSessionError:
                # log the problem
                pass

            resources = retrieve_matched_resources(db_session, libdata["nid"])
            for resource in resources:
                try:
                    response = worldcat.full_bib_request(resource.oclcMatchNumber)
                    result = update_resource(
                        db_session,
                        resource.sierraId,
                        resource.libraryId,
                        fullBib=response.content,
                    )
                    if not result:
                        # log error
                        pass
                except WorldcatSessionError:
                    # log, try another library?
                    break

            resources = retrieve_upgrade_ready_resources(db_session, libdata["nid"])

    # produce MARC records
    # output MARC records to the network drive
    # notify
