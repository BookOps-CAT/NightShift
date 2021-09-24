# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise

from nightshift.marc_parser import BibReader


def test_BibReader_iterator():
    reader = BibReader("tests/nyp-ebook-sample.mrc", "nyp")
    with does_not_raise():
        for bib in reader:
            pass
