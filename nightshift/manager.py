"""
This module incldues top level processes to be performed by the app
"""
from bookops_worldcat.errors import WorldcatSessionError


from nightshift.constants import LIBRARIES
from nightshift.datastore import session_scope
from nightshfit.datastore_transactions import retrieve_new_resources, update_resource
from nightshift import worldcat
from nightshift.marc.marc_parser import BibReader


def process_new_files():
    """
    Processes any newly added files.
    """

    # find if any MARC files have been dropped from Sierra to the
    # network drive
    # parse them and save data into the db
    with session_scope() as db_session:
        for library, v in LIBRARIES.items():
            resources = BibReader(marc_fh="tests/nyp-ebook-sample.mrc", library=library)
            for resource in resources:
                insert_or_ignore(db_session, Resource, resource)
                db_session.commit()

        # retrieve today's resouces & search WorldCat
        for library, v in LIBRARIES.items():
            resources = retrieve_new_resources(db_session, v["nid"])

            try:
                results = worldcat.search_batch(library=library, resources=resources)
                for resource, response in results:
                    if response is not None:
                        oclcNumber = parse_oclc_number(response)
                        queries = resource.queries.append(
                            resourceId=resource.nid,
                            match=True,
                            response=response.json(),
                        )
                        res = update_resource(
                            db_session,
                            resource.sierraId,
                            resource.libraryId,
                            oclcMatchNumber=oclcNumber,
                            queries=queries,
                        )
                    else:
                        queries = resource.queries.append(
                            resourceId=resource.nid, match=False
                        )
                        res = update_resource(
                            db_session,
                            resource.sierraId,
                            resource.libraryId,
                            queries=queries,
                        )
                    db_session.commit()
            except WorldcatSessionError:
                # log the problem
                pass

    # search db for successfuly matched resources
    # produce MARC records
    # output MARC records to the network drive
    # notify
