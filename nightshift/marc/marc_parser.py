# -*- coding: utf-8 -*-

"""
This module reads and parses MARC Sierra records (bibliographic and order data)
to be inserted into the DB.
In case of e-resouces only bib informatin is considered.
Source MARC files for e-resources will have a mix of various formats (ebooks, eaudio,
evideo)
"""
from io import BytesIO
import logging
import pickle
from typing import Any, BinaryIO, Iterator, Optional, Union

from bookops_marc import SierraBibReader, Bib
from pymarc import parse_xml_to_array, Record


from ..constants import LIBRARIES, RESOURCE_CATEGORIES
from ..datastore import Resource


logger = logging.getLogger("nightshift")


def worldcat_response_to_pymarc(response: bytes) -> Record:
    """
    Converts MetadataApi responses into `pymarc.Record` objects.

    Args:
        response:                           MARC XML in binary format

    Returns:
        `pymarc.Record` instance

    Raises:
        TypeError
    """
    logger.debug("Converting Worldcat response to pymarc object.")
    if not isinstance(response, bytes):
        logger.error(
            f"Invalid MARC data format: {type(response).__name__}. Not able to convert to pymarc object."
        )
        raise TypeError("Invalid MARC data format. Must be bytes.")
    else:
        data = BytesIO(response)
        return parse_xml_to_array(data)[0]


class BibReader:
    """
    An iterator class for extracting Sierra bib data from a file or bytes
    stream of MARC21 records
    """

    def __init__(
        self,
        marc_target: Union[BytesIO, BinaryIO],
        library: str,
        hide_utf8_warnings: bool = True,
    ) -> None:
        """
        The constructor.
        Args:
            marc_target:                    MARC file or file-like object
            library:                        'nyp' or 'bpl'
            hide_utf8_warnings:             hides character encoding warnings
        """
        logger.info(f"Initating BibReader.")
        if library not in LIBRARIES.keys():
            raise ValueError("Invalid 'library' argument. Must be 'nyp' or 'bpl'.")

        if isinstance(marc_target, BytesIO):
            self.marc_target = marc_target
        elif isinstance(marc_target, str):
            self.marc_target = open(marc_target, "rb")
        else:
            logger.error(
                f"Invalid 'marc_target' argument: {marc_target} ({type(marc_target).__name__})"
            )
            raise TypeError("Invalid 'marc_target' argument. Must be file-like object.")

        self.library = library
        self.hide_utf8_warnings = hide_utf8_warnings

    def __iter__(self) -> Iterator[Resource]:
        reader = SierraBibReader(
            self.marc_target, hide_utf8_warnings=self.hide_utf8_warnings
        )
        for bib in reader:
            resource_category = self._determine_resource_category(bib)

            # skip any unmapped resource types from processing
            if not resource_category:
                continue
            else:
                bib_info = self._map_data(bib, resource_category)
                yield bib_info

        self.marc_target.close()

    def _determine_resource_category(self, bib: Bib) -> Optional[str]:
        """
        Determies resource category based on bib and order information.

        Args:
            bib:                            `bookops_marc.Bib` instance

        Returns:
            resource_category
        """
        # Overdrive MarcExpress records control number starts with ODN
        control_number = bib.control_number()
        if control_number and control_number.startswith("ODN"):
            rec_type = bib.record_type()
            if rec_type == "a":
                return "ebook"
            elif rec_type == "i":
                return "eaudio"
            elif rec_type == "g":
                return "evideo"
            else:
                return None
        else:
            # future hook
            # determine particular resource category for print material here
            # based it on order information from the 960/961 tags
            logger.warning(
                f"Unsuppported bib type. Unable to ingest {self.library.upper()} bib # {bib.sierra_bib_id()}."
            )
            return None

    def _pickle_obj(self, obj: Any) -> bytes:
        return pickle.dumps(obj)

    def _fields2keep(self, bib: Bib, resource_category: str) -> bytes:
        """
        Resource category specific MARC tags to be carried over to output records
        """
        keep = []
        tags2keep = RESOURCE_CATEGORIES[resource_category]["src_tags2keep"]
        keep.extend(bib.get_fields(*tags2keep))
        pickled_keep = self._pickle_obj(keep)

        return pickled_keep

    def _map_data(self, bib: Bib, resource_category: str) -> Resource:
        """
        Maps selected data from bib to `datastore.Resource` table columns.

        Args:
            bib:                            `bookops_marc.Bib` instance

        Returns:
            Resource:                       `datastore.Resource` instance
        """
        sierraId = bib.sierra_bib_id_normalized()
        libraryId = LIBRARIES[self.library]["nid"]
        resourceCategoryId = RESOURCE_CATEGORIES[resource_category]["nid"]
        bibDate = bib.created_date()
        author = bib.author()
        title = bib.title()
        pubDate = bib.pubyear()
        congressNumber = bib.lccn()
        controlNumber = bib.control_number()
        distributorNumber = bib.overdrive_number()
        suppressed = bib.suppressed()
        otherNumber = bib.upc_number()
        srcFieldsToKeep = self._fields2keep(bib, resource_category)
        standardNumber = bib.isbn()

        resource = Resource(
            sierraId=sierraId,
            libraryId=libraryId,
            resourceCategoryId=resourceCategoryId,
            bibDate=bibDate,
            author=author,
            title=title,
            pubDate=pubDate,
            congressNumber=congressNumber,
            controlNumber=controlNumber,
            distributorNumber=distributorNumber,
            suppressed=suppressed,
            otherNumber=otherNumber,
            srcFieldsToKeep=srcFieldsToKeep,
            standardNumber=standardNumber,
            status="open",
        )
        logger.debug(f"Parsed resource: {resource}")

        return resource
