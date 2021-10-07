"""
This module incldues top level processes to be performed by the app
"""
from bookops_worldcat.errors import WorldcatSessionError


from nightshift.datastore import session_scope
from nightshfit.datastore_transactions import retrieve_new_resources
from nightshift.worldcat import search_batch


def process_new_files():
    # find if any MARC files have been dropped from Sierra to the
    # network drive
    # parse them and save data into the db

    # retrieve today's resouces

    # search today's resources
    with session_scope() as db_session:
        resources = retrieve_new_resources(db_session)

        try:
            results = search_batch(library=library, resources=resources)
            for res in results:
                # save results
                pass
        except WorldcatSessionError:
            # log the problem
            pass

    # search db for successfuly matched resources
    # produce MARC records
    # output MARC records to the network drive
    # notify
