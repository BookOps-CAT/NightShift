# -*- coding: utf-8 -*-
import datetime
import logging
import os

import pytest

from nightshift.datastore import session_scope, SourceFile, Resource
from nightshift.datastore_transactions import insert_or_ignore, add_resource
from nightshift.comms.storage import get_credentials, Drive
from nightshift.marc.marc_parser import BibReader
from nightshift.constants import LIBRARIES
from nightshift.tasks import enhance_and_output_bibs, transfer_to_drive


@pytest.mark.firewalled
def test_fetch_file_and_add_to_db(env_var, test_data):
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
        assert len(results) == 2

        bib = results[1]
        assert bib.sierraId == 21642892
        assert bib.status == "open"
        assert bib.resourceCategoryId == 1


@pytest.mark.firewalled
def test_enhance_and_transfer_to_drive(caplog, env_var, test_data, stub_resource):
    new_resource = Resource(
        sierraId=22222222,
        libraryId=1,
        resourceCategoryId=1,
        sourceId=1,
        bibDate=datetime.datetime.utcnow().date(),
        title="Test title",
        status="open",
        fullBib=b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<entry xmlns="http://www.w3.org/2005/Atom">\n  <content type="application/xml">\n    <response xmlns="http://worldcat.org/rb" mimeType="application/vnd.oclc.marc21+xml">\n      <record xmlns="http://www.loc.gov/MARC21/slim">\n        <leader>00000cam a2200000Ia 4500</leader>\n        <controlfield tag="001">ocn850939580</controlfield>\n        <controlfield tag="003">OCoLC</controlfield>\n        <controlfield tag="005">20190426152409.0</controlfield>\n        <controlfield tag="008">120827s2012    nyua   a      000 f eng d</controlfield>\n        <datafield tag="040" ind1=" " ind2=" ">\n          <subfield code="a">OCPSB</subfield>\n          <subfield code="b">eng</subfield>\n          <subfield code="c">OCPSB</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">OCPSB</subfield>\n          <subfield code="d">OCLCQ</subfield>\n          <subfield code="d">NYP</subfield>\n    </datafield>\n        <datafield tag="035" ind1=" " ind2=" ">\n          <subfield code="a">(OCoLC)850939580</subfield>\n    </datafield>\n        <datafield tag="020" ind1=" " ind2=" ">\n          <subfield code="a">some isbn</subfield>\n    </datafield>\n        <datafield tag="049" ind1=" " ind2=" ">\n          <subfield code="a">NYPP</subfield>\n    </datafield>\n        <datafield tag="100" ind1="0" ind2=" ">\n          <subfield code="a">OCLC RecordBuilder.</subfield>\n    </datafield>\n        <datafield tag="245" ind1="1" ind2="0">\n          <subfield code="a">Record Builder Added This Test Record On 06/26/2013 13:06:26.</subfield>\n    </datafield>\n        <datafield tag="336" ind1=" " ind2=" ">\n          <subfield code="a">text</subfield>\n          <subfield code="b">txt</subfield>\n          <subfield code="2">rdacontent</subfield>\n    </datafield>\n        <datafield tag="337" ind1=" " ind2=" ">\n          <subfield code="a">unmediated</subfield>\n          <subfield code="b">n</subfield>\n          <subfield code="2">rdamedia</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">TEST RECORD -- DO NOT USE.</subfield>\n    </datafield>\n        <datafield tag="500" ind1=" " ind2=" ">\n          <subfield code="a">Added Field by MarcEdit.</subfield>\n    </datafield>\n  </record>\n    </response>\n  </content>\n  <id>http://worldcat.org/oclc/850939580</id>\n  <link href="http://worldcat.org/oclc/850939580"/>\n</entry>',
    )
    with session_scope() as db_session:
        db_session.add(new_resource)
        db_session.commit()

        resources = (
            db_session.query(Resource).where(Resource.sierraId == 22222222).all()
        )

        with caplog.at_level(logging.DEBUG):
            enhance_and_output_bibs("NYP", resources)

        assert "NYP b22222222a has been output to 'temp.mrc'." in caplog.text

        with caplog.at_level(logging.INFO):
            transfer_to_drive("NYP", "ebook", "temp.mrc")

        assert "NYP ebook records have been output to remote " in caplog.text

        assert not os.path.exists("temp.mrc")
