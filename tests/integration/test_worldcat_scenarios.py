"""
Use this module to test various Worldcat search strategies to improve
recall and quality of retrieved records.
"""
from contextlib import nullcontext as does_not_raise
import os

from bookops_worldcat import MetadataSession
import pytest
import yaml

from nightshift.comms.worldcat import Worldcat
from nightshift.marc.marc_parser import worldcat_response_to_pymarc
from nightshift.marc.marc_writer import BibEnhancer


@pytest.fixture(scope="class")
def local_nypl_env_var():
    with open("tests/envar.yaml", "r") as f:
        data = yaml.safe_load(f)
        for k, v in data.items():
            os.environ[k] = v


@pytest.fixture(scope="class")
def wcat(local_nypl_env_var):
    with Worldcat("NYP") as worldcat:
        yield worldcat


@pytest.mark.local
class TestWorldcatSearch:
    @pytest.mark.parametrize(
        "q,hits,match_number",
        [
            pytest.param(
                "sn=8B24FBF1-9CD3-4FD4-89BD-5782B6B5E1BC AND (dx:rda OR dx:pn)",
                0,
                None,
                id="no rda and no pn",
            ),
            pytest.param(
                "sn=9C24F5B1-3AC2-4197-AAE0-7E5B10DBEBE9 AND (dx:rda OR dx:pn)",
                2,
                "936069070",
                id="1 rda and 2 pn available",
            ),
            pytest.param(
                "sn=19B05CB6-0C5A-4432-8785-AAE0977B1877 AND (dx:rda OR dx:pn)",
                1,
                "1132404223",
                id="only pn available",
            ),
            pytest.param(
                "sn=C0DDF2F1-5943-45BB-BB1E-0667EA3B8229 NOT lv:3 NOT cs=UKAHL",
                0,
                None,
                id="test",
            ),
            pytest.param(
                "sn=92F1B031-8366-44C3-97F5-8896E827E892",
                2,
                "53305308",
                id="The Comet / Du Bois",
            ),
            pytest.param(
                "no:1035408434",
                1,
                "1035408434",
                id="garbled diacritics",
            ),
        ],
    )
    def test_filtering_out_poor_quality_eresource_records(
        self, wcat, q, hits, match_number
    ):
        payload = dict(
            q=q,
            itemType="book",
            itemSubType="book-digital",
        )

        response = wcat.session.search_brief_bibs(
            **payload, inCatalogLanguage="eng", orderBy="mostWidelyHeld", limit=1
        )
        print(response.url)
        print(response.json())
        assert response.json()["numberOfRecords"] == hits

        if hits:
            assert response.json()["briefRecords"][0]["oclcNumber"] == match_number

    def test_get_full_bib_garbled_diacritics(self, wcat, stub_resource):

        stub_resource.oclcMatchNumber = "1035408434"
        stub_resource.fullBib = None
        responses = wcat.get_full_bibs([stub_resource])
        for r in responses:
            # with open("temp.txt", "wb") as f:
            #     f.write(r[1])
            marcxml = r[1]
            stub_resource.fullBib = marcxml
            break

        be = BibEnhancer(stub_resource)
        assert be._meets_minimum_criteria() is False


@pytest.mark.local
def test_creds():
    with open("nightshift/config/config.yaml", "r") as f:
        data = yaml.safe_load(f)
        for k, v in data.items():
            os.environ[k] = v

        with does_not_raise():
            with Worldcat("BPL") as worldcat:
                assert isinstance(worldcat.session, MetadataSession)

            with Worldcat("NYP") as worldcat:
                assert isinstance(worldcat.session, MetadataSession)
