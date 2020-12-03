# -*- coding: utf-8 -*-

"""
This module provides methods to generate MARC records

MARC manipulation rules:
Use OCLC matching record as a base

    shared:
        - remove 020 from OCLC full bib
        - remove 024 from OCLc full bib
        - remove 037s from OCLC full bib
        - remove local fields 019, 084, 938
        - remove subject heading tags different than LCSH, LCGN, GSAFD, FAST
        - remove 856s tags from full OCLC bib

        - add 020 based on MarcExpress bib (ME) with $q (electronic bk.)
        - add 024 based on ME bib
        - add 037$a based on ME bib

    NYPL only:
        - 001  includes OCLC number without a prefix
        - create 091  $a eBOOK/eAudio/eVideo based on Sierra format value
        - create 949 command line to set Sierra format, target bib
        - add NYPL specific 856s based on ME 

    BPL only:
        - 001  includes OCLC number with a prefix
        - create 099  $a eBook/eAudio/eVideo based on Sierra format value
        - create 949 command line to set Sierra format, target bib
        - add NYPL specific 856s based on ME 

"""

from io import BytesIO
from typing import Type
import xml.etree.ElementTree as ET

import pymarc
from pymarc import XmlHandler, parse_xml


import nightshift


class NightShiftXmlHandler(XmlHandler):
    """
    Subclass of pymarc's XmlHandler that deals with single record only.
    """

    def __init__(self, strict=False, normalize_form=None):

        super().__init__(strict, normalize_form)

    def process_record(self, record):
        self.records = record


def _parse_xml_record(xml_file, strict=False, normalize_form=None):
    """Parse XML data."""
    handler = NightShiftXmlHandler(strict, normalize_form)
    parse_xml(xml_file, handler)
    return handler.records


def response2pymarc(response_content: bytes) -> Type[pymarc.record.Record]:
    """
    Serializes full record xml Worldcat response to pymarc record object

    Args:
        response_content:           full record response content (bytes)
    returns:
        `pymarc.record.Record`
    """
    record = BytesIO(response_content)
    return _parse_xml_record(record)


def remove_unwanted_tags(record: Type[pymarc.record.Record]):
    """Removes tags that will be replaced with MarcExpress tags or are
    simply not desired

    Args:
        record:                 full Worldcat record as pymarc object
    """
    record.remove_fields(
        "019", "020", "024", "037", "084", "099", "091", "856", "938", "949"
    )


# def prepare_output_record(resource_record: Type[nightshift.datastore.Resource]):
#     record = response2pymarc(resource_record.wqueries[0].record)

#     # clean up record from data that will not be passed to the catalog
#     remove_unwanted_tags(record)

#     return record
