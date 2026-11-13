from contextlib import nullcontext as does_not_raise

import pytest

from nightshift.comms.worldcat import BriefBibResponse, Worldcat
from nightshift.datastore import Resource


class TestWorldcatMocked:
    """Tests Worldcat class methods with mocking all interactions with OCLC service"""

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            ("NYP", does_not_raise()),
            ("BPL", does_not_raise()),
            ("nyp", does_not_raise()),
            ("bpl", does_not_raise()),
            (
                "QPL",
                pytest.raises(
                    ValueError,
                    match="Invalid library argument provided. Must be 'NYP' or 'BPL'.",
                ),
            ),
        ],
    )
    def test_session(self, arg, expectation, mock_token):
        with expectation:
            Worldcat(arg)

    def test_get_full_bibs(self, caplog, stub_worldcat_session):
        resource1 = Resource(
            nid=1, sierraId=22222222, title="TEST TITLE", oclcMatchNumber=None
        )
        resource2 = Resource(
            nid=2, sierraId=33333333, title="TEST TITLE", oclcMatchNumber="123"
        )
        list(stub_worldcat_session.get_full_bibs([resource1, resource2]))
        assert len(caplog.records) == 1
        assert (
            "Full bib Worldcat request for NYP Sierra bib # b33333333a: request_url_here."
            in caplog.text
        )

    @pytest.mark.parametrize(
        "resource",
        [
            pytest.param(
                Resource(
                    nid=1,
                    sierraId=22222222,
                    resourceCategoryId=1,
                    libraryId=1,
                    title="TEST TITLE",
                    distributorNumber="111",
                ),
                id="ebook",
            ),
            pytest.param(
                Resource(
                    nid=1,
                    sierraId=22222222,
                    resourceCategoryId=2,
                    libraryId=1,
                    title="TEST TITLE",
                    distributorNumber="111",
                ),
                id="eaudio",
            ),
            pytest.param(
                Resource(
                    nid=1,
                    sierraId=22222222,
                    resourceCategoryId=3,
                    libraryId=1,
                    title="TEST TITLE",
                    distributorNumber="111",
                ),
                id="evideo",
            ),
            pytest.param(
                Resource(
                    nid=1,
                    sierraId=22222222,
                    resourceCategoryId=4,
                    libraryId=1,
                    title="TEST TITLE",
                    standardNumber="111",
                ),
                id="book-isbn",
            ),
            pytest.param(
                Resource(
                    nid=1,
                    sierraId=22222222,
                    resourceCategoryId=4,
                    libraryId=1,
                    title="TEST TITLE",
                    congressNumber="111",
                ),
                id="book-lccn",
            ),
        ],
    )
    def test_get_brief_bibs(self, caplog, stub_worldcat_session, resource):
        query = stub_worldcat_session.get_brief_bibs(
            [resource], rotten_apples={1: ["FOO", "BAR"]}
        )
        all_results = list(query)
        result = all_results[0]
        assert isinstance(result, tuple)

        resource, data = result
        assert isinstance(resource, Resource)
        assert isinstance(data, BriefBibResponse)
        assert resource.nid == 1
        assert data.is_match
        assert data.as_json["numberOfRecords"] == 1
        assert data.oclc_number == "44959645"
        assert (
            "Brief bib Worldcat query for NYP Sierra bib # b22222222a: request_url_here"
            in caplog.text
        )

    def test_get_brief_bibs_no_matches_found(
        self, caplog, stub_worldcat_session, mock_worldcat_response_no_matches
    ):
        resource = Resource(
            nid=1,
            sierraId=22222222,
            resourceCategoryId=1,
            libraryId=1,
            title="TEST TITLE",
            distributorNumber="111",
        )
        all_results = list(stub_worldcat_session.get_brief_bibs([resource]))
        result = all_results[0]

        assert isinstance(result, tuple)

        res, data = result

        assert isinstance(res, Resource)
        assert isinstance(data, BriefBibResponse)
        assert resource.nid == 1
        assert data.as_json == {"numberOfRecords": 0}
        assert not data.is_match
        assert data.oclc_number is None

        assert (
            "No matches found for NYP Sierra bib # b22222222a: request_url_here"
            in caplog.text
        )

    def test_get_brief_bibs_no_payload_available(self, caplog, stub_worldcat_session):
        """
        In a rare situation resource may not have any identifiers to be used for
        searching
        """
        resource = Resource(
            nid=1,
            sierraId=22222222,
            resourceCategoryId=13,
            libraryId=1,
            title="TEST TITLE",
        )

        results = list(stub_worldcat_session.get_brief_bibs([resource]))
        list(results)
        assert (
            "Unable to create a payload for brief bib query for NYP resource nid=1, sierraId=b22222222a."
            in caplog.text
        )
