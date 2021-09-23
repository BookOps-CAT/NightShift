# -*- coding: utf-8 -*-

from nightshift.marc_parser import BibReader


def test_BibReader_pickle_obj():
    reader = BibReader("foo.mrc", "nyp", "e_resource")
