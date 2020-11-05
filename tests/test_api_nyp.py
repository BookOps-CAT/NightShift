# -*- coding: utf-8 -*-

"""
Tests bot's NYPL Platform request methods
"""
import datetime

import pytest

from nightshift.api_nyp import split_into_batches, PlatformResponseReader
from nightshift.errors import NightShiftError


@pytest.mark.parametrize(
    "arg,size,expectation",
    [
        (
            [1, 2, 3, 4, 5, 6, 7],
            2,
            [
                [1, 2],
                [3, 4],
                [5, 6],
                [7],
            ],
        ),
        ([1], 2, [[1]]),
        ([1, 2], 2, [[1, 2]]),
    ],
)
def test_split_into_batches(arg, size, expectation):
    assert split_into_batches(arg, size) == expectation


def test_split_into_batches_default_size():
    assert split_into_batches([1] * 125) == [[1] * 50, [1] * 50, [1] * 25]


class TestPlatformResponseReader:
    """Tests Platorm respnses reader"""

    def test_successful_response(
        self, stub_nyp_platform_200_response, stub_nyp_responses
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader.datas == stub_nyp_responses["data"]

    def test_404_response(self, stub_nyp_platform_404_response):
        reader = PlatformResponseReader(stub_nyp_platform_404_response)
        assert reader.datas == []

    def test_other_error_response(self, stub_nyp_platform_401_response):
        err_msg = "Platform 401 error: {'statusCode': 401, 'type': 'unauthorized', 'message': 'Unauthorized'}"
        with pytest.raises(NightShiftError) as exc:
            PlatformResponseReader(stub_nyp_platform_401_response)
            assert err_msg in str(exc.value)

    @pytest.mark.parametrize(
        "tag,sub,expectation",
        [
            ("084", "a", ["BIO007000", "HIS036050", "LIT004020"]),
            ("037", "a", ["40CC3B3F-4C30-4685-B391-DB7B2EA91455"]),
            ("856", "3", ["Excerpt", "Image", "Thumbnail"]),
            ("020", "a", ["9781631491719", "1631491719"]),
            ("010", "a", ["2017022370"]),
            (
                "856",
                "u",
                [
                    "http://link.overdrive.com/?content",
                    "https://samples.overdrive.com/?sample_url",
                    "https://img1.od-cdn.com/ImageType-100/image.jpg",
                    "https://img1.od-cdn.com/ImageType-200/thumbnail.jpg",
                ],
            ),
        ],
    )
    def test_get_variable_field_content(
        self, tag, sub, expectation, stub_nyp_platform_200_response
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert (
            reader._get_variable_field_content(tag, sub, reader.datas[0]) == expectation
        )

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            ("bt123456", (None, None, False, None)),
            (
                "00012345",
                ("00012345", datetime.datetime(2019, 1, 1, 17, 0, 0), True, 2),
            ),
        ],
    )
    def test_is_upgraded(
        self, arg, expectation, stub_nyp_platform_200_response, mock_datetime_now
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._is_upgraded(arg) == expectation

    def test_parse_isbns(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_isbns(reader.datas[0]) == "9781631491719,1631491719"

    def test_parse_isbns_no_field(
        self, stub_nyp_platform_200_response, stub_platform_record_missing
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_isbns(stub_platform_record_missing) is None

    def test_parse_control_number(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_control_number(reader.datas[0]) == "ODN0005077214"

    def test_parse_lccn(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_lccn(reader.datas[0]) == "2017022370"

    def test_parses_lccn_missing(
        self, stub_nyp_platform_200_response, stub_platform_record_missing
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_lccn(stub_platform_record_missing) is None

    def test_parse_distributor_number(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert (
            reader._parse_distributor_number(reader.datas[0])
            == "40CC3B3F-4C30-4685-B391-DB7B2EA91455"
        )

    def test_parse_distributor_number_missing(
        self, stub_nyp_platform_200_response, stub_platform_record_missing
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_distributor_number(stub_platform_record_missing) is None

    def test_parse_standard_nubers(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert (
            reader._parse_standard_numbers(reader.datas[0]) == "1111111111,2222222222"
        )

    def test_parse_standard_nubers_missing(
        self, stub_nyp_platform_200_response, stub_platform_record_missing
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_standard_numbers(stub_platform_record_missing) is None

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            ("bt123456", None),
            ("123456789", "123456789"),
        ],
    )
    def test_parse_worldcat_number(
        self, arg, expectation, stub_nyp_platform_200_response
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_worldcat_number(arg) == expectation

    def test_parse_title(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert (
            reader._parse_title(reader.datas[0])
            == "saddest words electronic resource william faulkners civil war"
        )

    def test_parse_author(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_author(reader.datas[0]) == "gorra michael"

    def test_parse_publication_date(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_publication_date(reader.datas[0]) == "2020"

    @pytest.mark.parametrize(
        "url,expectation",
        [
            ("http://link.overdrive.com/?content", 1),
            (
                "https://samples.overdrive.com/?sample_url",
                2,
            ),
            (
                "https://img1.od-cdn.com/ImageType-100/image.jpg",
                3,
            ),
            (
                "https://img1.od-cdn.com/ImageType-200/thumbnail.jpg",
                4,
            ),
            ("http://example.com", None),
        ],
    )
    def test_determine_url_type_id(
        self, url, expectation, stub_nyp_platform_200_response
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._determine_url_type_id(url) == expectation

    def test_parse_urls(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        urls = reader._parse_urls(reader.datas[0])

        loop = 1
        for u in urls:
            if loop == 1:
                assert u == {
                    "urlTypeId": 1,
                    "url": "http://link.overdrive.com/?content",
                }
            elif loop == 2:
                assert u == {
                    "urlTypeId": 2,
                    "url": "https://samples.overdrive.com/?sample_url",
                }
            elif loop == 3:
                assert u == {
                    "urlTypeId": 3,
                    "url": "https://img1.od-cdn.com/ImageType-100/image.jpg",
                }
            elif loop == 4:
                assert u == {
                    "urlTypeId": 4,
                    "url": "https://img1.od-cdn.com/ImageType-200/thumbnail.jpg",
                }
            loop += 1

    def test_parse_urls_no_urls(
        self, stub_nyp_platform_200_response, stub_platform_record_missing
    ):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        assert reader._parse_urls(stub_platform_record_missing) == []

    def test_map_data(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        meta = reader._map_data(reader.datas[0])
        assert meta.sbn == "9781631491719,1631491719"
        assert meta.lcn == "2017022370"
        assert meta.did == "40CC3B3F-4C30-4685-B391-DB7B2EA91455"
        assert meta.sid == "1111111111,2222222222"
        assert meta.wcn is None
        assert (
            meta.title
            == "saddest words electronic resource william faulkners civil war"
        )
        assert meta.author == "gorra michael"
        assert meta.pubDate == "2020"
        assert meta.upgradeStamp is None
        assert meta.upgraded is False
        assert meta.upgradeSourceId is None
        assert meta.urls == [
            {
                "urlTypeId": 1,
                "url": "http://link.overdrive.com/?content",
            },
            {
                "urlTypeId": 2,
                "url": "https://samples.overdrive.com/?sample_url",
            },
            {
                "urlTypeId": 3,
                "url": "https://img1.od-cdn.com/ImageType-100/image.jpg",
            },
            {
                "urlTypeId": 4,
                "url": "https://img1.od-cdn.com/ImageType-200/thumbnail.jpg",
            },
        ]

    def test_generator(self, stub_nyp_platform_200_response):
        reader = PlatformResponseReader(stub_nyp_platform_200_response)
        loop = 0
        for record in reader:
            loop += 1
            if loop == 1:
                assert record.did == "40CC3B3F-4C30-4685-B391-DB7B2EA91455"
            elif loop == 2:
                assert record.did == "4E7547BF-6D42-43F4-9180-8FCF302497F3"
        assert loop == 2
