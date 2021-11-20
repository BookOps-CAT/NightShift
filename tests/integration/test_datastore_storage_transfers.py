# -*- coding: utf-8 -*-

import pytest

from nightshift.datastore import session_scope, SourceFile, Resource
from nightshift.datastore_transactions import insert_or_ignore, add_resource
from nightshift.comms.storage import get_credentials, Drive
from nightshift.marc.marc_parser import BibReader
from nightshift.constants import LIBRARIES


@pytest.mark.firewalled
def test_fetch_file_and_add_to_db(env_var, test_db):
    test_file = "NYPeres210701.pout"
    library = "NYP"

    # open drive and stream content
    drive_creds = get_credentials()
    with Drive(*drive_creds) as drive:
        file_data = drive.fetch_file(test_file)

    # open db session and add SourceFile record
    with session_scope() as db_session:
        sf = insert_or_ignore(
            db_session,
            SourceFile,
            libraryId=LIBRARIES[library]["nid"],
            handle=test_file,
        )
        db_session.commit()

        # add records from file to db
        reader = BibReader(file_data, "NYP")
        for resource in reader:
            resource.sourceId = sf.nid
            add_resource(db_session, resource)

        db_session.commit()

        # verify
        results = db_session.query(Resource).all()
        assert len(results) == 1

        bib = results[0]
        assert bib.sierraId == 21642892
        assert bib.status == "open"
        assert bib.resourceCategoryId == 1
