# -*- coding: utf-8 -*-

"""
Tests bot's BPl Solr request methods
"""
import datetime

import pytest


from nightshift.api_bpl import SolrResponseReader, get_bpl_sierra_bib_data
from nightshift.errors import NightShiftError


class TestSolrResponseReader:
    """Test BPL Solr response reader"""

    def test_successful_response(self, stub_bpl_solr_200_response, stub_solr_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader.data == stub_solr_response["response"]["docs"][0]

    def test_no_hits_response(self, stub_bpl_solr_no_hits_response):
        reader = SolrResponseReader(stub_bpl_solr_no_hits_response)
        assert reader.data is None

    def test_error_response(self, stub_bpl_solr_401_response):
        err_msg = "BPL Solr 401 error:"
        with pytest.raises(NightShiftError) as exc:
            SolrResponseReader(stub_bpl_solr_401_response)

        assert err_msg in str(exc.value)

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            (
                "https://img1.od-cdn.com/ImageType-100/2016-1/%7B6B3176C1-D157-43B7-86C8-67A373834B65%7DImg100.jpg",
                "https://img1.od-cdn.com/ImageType-200/2016-1/%7B6B3176C1-D157-43B7-86C8-67A373834B65%7DImg200.jpg",
            ),
            ("https://img1.without_identifying_str/100.jpg", None),
        ],
    )
    def test_construct_thumbnail_url(
        self, arg, expectation, stub_bpl_solr_200_response
    ):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._construct_thumbnail_url(arg) == expectation

    def test_is_deleted(self, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._is_deleted(reader.data) is False

    def test_is_upgraded_true(self, stub_bpl_solr_200_response, mock_datetime_now):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._is_upgraded("ocn971018433") == (
            "971018433",
            datetime.datetime.now(),
            True,
            2,
        )

    @pytest.mark.parametrize("arg", ["ODN971018433", None])
    def test_is_upgraded_false(
        self, arg, stub_bpl_solr_200_response, mock_datetime_now
    ):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._is_upgraded(arg) == (
            None,
            None,
            False,
            None,
        )

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            ({"ss_marc_tag_001": "ocn971018433"}, "ocn971018433"),
            ({}, None),
        ],
    )
    def test_parse_control_number(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_control_number(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation",
        [("ocm0001", "0001"), ("ocn0002", "0002"), ("on2222", "2222")],
    )
    def test_parse_worldcat_number(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_worldcat_number(arg) == expectation

    def test_parse_bib_id(self, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_bib_id(reader.data) == 12014671

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            (
                {"isbn": ["9780300226348", "978-0300226348", "0300226349"]},
                "9780300226348,0300226349",
            ),
            ({}, None),
        ],
    )
    def test_parse_isbns(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_isbns(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation", [({"ss_marc_tag_010_a": "  11111"}, "11111"), ({}, None)]
    )
    def test_parse_lccn(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_lccn(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation", [({"econtrolnumber": "aaaa-bbbb"}, "aaaa-bbbb"), ({}, None)]
    )
    def test_parse_distributor_number(
        self, arg, expectation, stub_bpl_solr_200_response
    ):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_distributor_number(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation",
        [({"sm_marc_tag_024_a": ["1111", "2222"]}, "1111,2222"), ({}, None)],
    )
    def test_parse_standard_number(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_standard_numbers(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            ({"title": "Test title: subtitle"}, "test title: subtitle"),
            ({"title": "A" * 55}, "a" * 50),
        ],
    )
    def test_parse_title(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_title(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            ({"author_raw": "Doe, John, 1976-"}, "doe, john, 1976-"),
            ({"author_raw": "A" * 55}, "a" * 50),
            ({}, None),
        ],
    )
    def test_parse_author(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_author(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation", [({"publishYear": 2017}, "2017"), ({}, None)]
    )
    def test_parse_publication_date(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_publication_date(arg) == expectation

    @pytest.mark.parametrize(
        "arg,expectation",
        [
            ({"eurl": "url1"}, [{"uTypeId": 1, "url": "url1"}]),
            ({"esampleurl": "url2"}, [{"uTypeId": 2, "url": "url2"}]),
            ({"digital_cover_image": "url3"}, [{"uTypeId": 3, "url": "url3"}]),
            (
                {"digital_cover_image": "https://ImageType-100/100.jpg"},
                [
                    {"uTypeId": 3, "url": "https://ImageType-100/100.jpg"},
                    {"uTypeId": 4, "url": "https://ImageType-200/200.jpg"},
                ],
            ),
            (
                {
                    "eurl": "url1",
                    "esampleurl": "url2",
                    "digital_cover_image": "https://ImageType-100/100.jpg",
                },
                [
                    {"uTypeId": 1, "url": "url1"},
                    {"uTypeId": 2, "url": "url2"},
                    {"uTypeId": 3, "url": "https://ImageType-100/100.jpg"},
                    {"uTypeId": 4, "url": "https://ImageType-200/200.jpg"},
                ],
            ),
        ],
    )
    def test_parse_urls(self, arg, expectation, stub_bpl_solr_200_response):
        reader = SolrResponseReader(stub_bpl_solr_200_response)
        assert reader._parse_urls(arg) == expectation

    def test_map_data(self, stub_bpl_solr_200_response, mock_datetime_now):
        meta = SolrResponseReader(stub_bpl_solr_200_response).meta
        assert meta.sbid == 12014671
        assert meta.sbn == "9780300226348,0300226349"
        assert meta.lcn == "111111"
        assert meta.did == "8A4A1B86-E456-48D3-99DA-DF8FAD8946F1"
        assert meta.sid == "222222,333333"
        assert meta.wcn == "971018433"
        assert meta.deleted is False
        assert meta.title == "reporting war : how foreign correspondents risked"
        assert meta.author == "moseley, ray, 1932-"
        assert meta.pubDate == "2017"
        assert meta.upgradeStamp == datetime.datetime.now()
        assert meta.upgraded is True
        assert meta.upgradeSourceId == 2
        assert meta.urls == [
            {
                "uTypeId": 1,
                "url": "https://link.overdrive.com/?websiteID=89&titleID=3151352",
            },
            {
                "uTypeId": 2,
                "url": "https://samples.overdrive.com/?crid=8a4a1b86-e456-48d3-99da-df8fad8946f1&.epub-sample.overdrive.com",
            },
            {
                "uTypeId": 3,
                "url": "https://img1.od-cdn.com/ImageType-100/2363-1/{8A4A1B86-E456-48D3-99DA-DF8FAD8946F1}Img100.jpg",
            },
            {
                "uTypeId": 4,
                "url": "https://img1.od-cdn.com/ImageType-200/2363-1/{8A4A1B86-E456-48D3-99DA-DF8FAD8946F1}Img200.jpg",
            },
        ]
