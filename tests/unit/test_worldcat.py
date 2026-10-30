import pytest

from nightshift.comms.worldcat import BriefBibResponse, Worldcat
from nightshift.datastore import Resource


class TestBriefBibResponse:
    def test_successful_match_to_worldcat_record(self, mock_worldcat_response):
        data = BriefBibResponse(mock_worldcat_response)
        assert data.is_match
        assert data.oclc_number == "44959645"

    def test_failed_match_to_worldcat_record(self, mock_worldcat_response_no_matches):
        data = BriefBibResponse(mock_worldcat_response_no_matches)
        assert data.is_match is False
        assert data.oclc_number is None


class TestWorldcatMocked:
    """Tests Worldcat class methods with mocking all interactions with OCLC service"""

    def test_invalid_library_argument(self):
        with pytest.raises(ValueError) as exc:
            Worldcat("QPL")
        assert (
            str(exc.value)
            == "Invalid library argument provided. Must be 'NYP' or 'BPL'."
        )

    @pytest.mark.parametrize("arg", ["NYP", "BPL", "nyp", "bpl"])
    def test_create_worldcat_session(self, arg, mock_env_vars, mock_token):
        reader = Worldcat(arg)
        assert reader.library == arg.upper()

    def test_get_full_bibs(self, caplog, stub_worldcat_session):
        resource = Resource(
            nid=1, sierraId=22222222, title="TEST TITLE", oclcMatchNumber="123"
        )
        result = next(stub_worldcat_session.get_full_bibs([resource]))

        resource, response = result
        assert isinstance(result, tuple)
        assert isinstance(resource, Resource)
        assert resource.nid == 1
        assert isinstance(response, bytes)
        assert (
            "Full bib Worldcat request for NYP Sierra bib # b22222222a: request_url_here."
            in caplog.text
        )

    def test_get_full_bibs_no_oclc_number(self, caplog, stub_worldcat_session):
        resource1 = Resource(
            nid=1, sierraId=22222222, title="TEST TITLE", oclcMatchNumber=None
        )
        resource2 = Resource(
            nid=2, sierraId=33333333, title="TEST TITLE", oclcMatchNumber="123"
        )
        [i for i in stub_worldcat_session.get_full_bibs([resource1, resource2])]
        assert len(caplog.records) == 1
        assert (
            "Full bib Worldcat request for NYP Sierra bib # b33333333a: request_url_here."
            in caplog.text
        )

    @pytest.mark.parametrize(
        "resource_cat_id,rotten_apples,expectation",
        [
            pytest.param(
                1,
                {1: ["FOO", "BAR"]},
                [
                    {
                        "q": "sn=111 NOT lv:3 NOT cs=FOO NOT cs=BAR",
                        "itemType": "book",
                        "itemSubType": "book-digital",
                    }
                ],
                id="ebook",
            ),
            pytest.param(
                2,
                {},
                [
                    {
                        "q": "sn=111 NOT lv:3",
                        "itemType": "audiobook",
                        "itemSubType": "audiobook-digital",
                    }
                ],
                id="eaudio",
            ),
            pytest.param(
                3,
                {1: ["FOO", "BAR"]},
                [
                    {
                        "q": "sn=111 NOT lv:3 NOT lv:M",
                        "itemType": "video",
                        "itemSubType": "video-digital",
                    }
                ],
                id="evideo",
            ),
            pytest.param(
                4,
                {},
                [
                    {
                        "q": "bn:222",
                        "itemType": "book",
                        "itemSubType": "book-printbook",
                        "catalogSource": "DLC",
                    },
                    {
                        "q": "ln:333",
                        "itemType": "book",
                        "itemSubType": "book-printbook",
                        "catalogSource": "DLC",
                    },
                ],
                id="book",
            ),
        ],
    )
    def test_prep_resource_queries_payloads(
        self, caplog, resource_cat_id, rotten_apples, expectation, stub_worldcat_session
    ):
        resource = Resource(
            resourceCategoryId=resource_cat_id,
            sierraId=22222222,
            distributorNumber=111,
            standardNumber=222,
            congressNumber=333,
        )
        payloads = stub_worldcat_session._prep_resource_queries_payloads(
            resource, rotten_apples
        )
        assert payloads == expectation
        assert (
            f"Query payload for NYP Sierra bib # b22222222a: {expectation}."
            in caplog.text
        )

    @pytest.mark.parametrize(
        "resource_cat_id,rotten_apples",
        [
            pytest.param(1, {1: ["FOO", "BAR"]}, id="ebook"),
            pytest.param(2, {}, id="eaudio"),
            pytest.param(3, {1: ["FOO", "BAR"]}, id="evideo"),
            pytest.param(4, {}, id="book"),
        ],
    )
    def test_prep_resource_queries_payloads_no_payload(
        self, caplog, resource_cat_id, rotten_apples, stub_worldcat_session
    ):
        resource = Resource(resourceCategoryId=resource_cat_id, sierraId=22222222)
        payloads = stub_worldcat_session._prep_resource_queries_payloads(
            resource, rotten_apples
        )
        assert payloads == []
        assert "Query payload for NYP Sierra bib # b22222222a: []." in caplog.text

    def test_get_brief_bibs(self, caplog, stub_worldcat_session):
        resource = Resource(
            nid=1,
            sierraId=22222222,
            resourceCategoryId=1,  # ebook category id
            libraryId=1,
            title="TEST TITLE",
            distributorNumber="111",
        )
        result = next(stub_worldcat_session.get_brief_bibs([resource]))

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
        result = next(stub_worldcat_session.get_brief_bibs([resource]))

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
        resource1 = Resource(nid=1, resourceCategoryId=1, sierraId=22222222)
        resource2 = Resource(
            nid=2, resourceCategoryId=1, sierraId=22222223, distributorNumber="123"
        )

        results = [
            i for i in stub_worldcat_session.get_brief_bibs([resource1, resource2])
        ]
        [i for i in results]
        assert (
            "Unable to create a payload for brief bib query for NYP resource nid=1, sierraId=b22222222a."
            in caplog.text
        )
