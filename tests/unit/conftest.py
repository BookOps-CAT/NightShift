# -*- coding: utf-8 -*-

from io import BytesIO
import os

from bookops_marc import Bib
from pymarc import Field
import pytest

from nightshift.marc.marc_parser import BibReader


class MockOSError:
    def __init__(self, *args, **kwargs):
        raise OSError


@pytest.fixture
def mock_os_error(monkeypatch):
    monkeypatch.setattr("builtins.open", MockOSError)


@pytest.fixture
def mock_os_error_on_remove(monkeypatch):
    monkeypatch.setattr("os.remove", MockOSError)


@pytest.fixture
def mock_log_env(monkeypatch):
    monkeypatch.setenv("LOGGLY_TOKEN", "ns_token_here")
    monkeypatch.setenv("LOG_HANDLERS", "console,file,loggly")


@pytest.fixture(scope="function")
def test_log(monkeypatch, local_test_config):
    if not os.getenv("TRAVIS"):
        monkeypatch.setenv("LOGGLY_TOKEN", local_test_config["LOGGLY_TOKEN"])
        monkeypatch.setenv("LOG_HANDLERS", local_test_config["LOG_HANDLERS"])


# DB fixtures ############


@pytest.fixture
def test_data_rich(stub_resource, test_session, test_data_core):
    test_session.add(stub_resource)

    test_session.commit()


# Bibs fixtures ##############


@pytest.fixture
def stub_marc():
    bib = Bib()
    bib.leader = "02866pam  2200517 i 4500"
    bib.add_field(Field(tag="008", data="190306s2017    ht a   j      000 1 hat d"))
    bib.add_field(
        Field(
            tag="100",
            indicators=["1", " "],
            subfields=["a", "Adams, John,", "e", "author."],
        )
    )
    bib.add_field(
        Field(
            tag="245",
            indicators=["1", "4"],
            subfields=["a", "The foo /", "c", "by John Adams."],
        )
    )
    bib.add_field(
        Field(
            tag="264",
            indicators=[" ", "1"],
            subfields=["a", "Bar :", "b", "New York,", "c", "2021"],
        )
    )
    bib.add_field(
        Field(
            tag="907",
            indicators=[" ", " "],
            subfields=["a", ".b22222222x", "b", "07-01-21", "c", "07-01-2021 19:07"],
        )
    )

    return bib


@pytest.fixture
def fake_BibReader():
    return BibReader(BytesIO(b"some records"), "NYP")
