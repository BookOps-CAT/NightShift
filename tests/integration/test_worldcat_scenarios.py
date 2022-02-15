"""
Use this module to test various Worldcat search strategies to improve
recall and quality of retrieved records.
"""
import os

import pytest
import yaml

from nightshift.comms.worldcat import Worldcat


@pytest.fixture(scope="class")
def local_env_var():
    with open("tests/envar.yaml", "r") as f:
        data = yaml.safe_load(f)
        for k, v in data.items():
            os.environ[k] = v


@pytest.fixture(scope="class")
def wcat(local_env_var):
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
