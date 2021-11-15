# -*- coding: utf-8 -*-

"""
Tests `marc.marc_writer.py` module
"""
from contextlib import nullcontext as does_not_raise
import logging
import pickle

from pymarc import Record, Field
import pytest

from nightshift import __title__, __version__
from nightshift.datastore import Resource
from nightshift.marc.marc_writer import BibEnhancer


class TestBibEnhancer:
    def test_init(self, caplog, stub_resource):
        with does_not_raise():
            with caplog.at_level(logging.INFO):
                result = BibEnhancer(stub_resource)

        assert result.library == "nyp"
        assert isinstance(result.resource, Resource)
        assert isinstance(result.bib, Record)
        assert "Enhancing NYP Sierra bib # b11111111a." in caplog.text

    def test_add_local_tags(self, stub_resource):
        fields = [
            Field(tag="020", indicators=[" ", " "], subfields=["a", "978123456789x"]),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=["a", "123", "b", "Overdrive Inc."],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=["u", "url_here", "2", "opac msg"],
            ),
        ]
        pickled_fields = pickle.dumps(fields)
        stub_resource.srcFieldsToKeep = pickled_fields
        be = BibEnhancer(stub_resource)
        be._add_local_tags()
        bib = be.bib

        assert str(bib["020"]) == "=020  \\\\$a978123456789x"
        assert str(bib["037"]) == "=037  \\\\$a123$bOverdrive Inc."
        assert str(bib["856"]) == "=856  04$uurl_here$2opac msg"

    def test_purge_tags(self, stub_resource):
        be = BibEnhancer(stub_resource)
        fields = [
            Field(tag="020", indicators=[" ", " "], subfields=["a", "978123456789x"]),
            Field(
                tag="037",
                indicators=[" ", " "],
                subfields=["a", "123", "b", "Overdrive Inc."],
            ),
            Field(
                tag="856",
                indicators=["0", "4"],
                subfields=["u", "url_here", "2", "opac msg"],
            ),
        ]
        for field in fields:
            be.bib.add_field(field)

        # make sure added tags are present
        for field in fields:
            assert field.tag in be.bib

        be._purge_tags()

        # test if they were removed
        for field in fields:
            assert field.tag not in be.bib

    @pytest.mark.parametrize(
        "library,tag",
        [
            ("nyp", "901"),
            ("bpl", "947"),
        ],
    )
    def test_add_initials_tag(self, library, tag, stub_resource):
        be = BibEnhancer(stub_resource)
        be.library = library
        be._add_initials_tag()
        assert str(be.bib[tag]) == f"={tag}  \\\\$a{__title__}/{__version__}"

    def test_add_initials_tag_invalid_library(self, stub_resource):
        be = BibEnhancer(stub_resource)
        bib_before = str(be.bib)
        be.library = "foo"
        be._add_initials_tag()
        assert str(be.bib) == bib_before
