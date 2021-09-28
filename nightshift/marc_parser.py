# -*- coding: utf-8 -*-

"""
This module reads and parses MARC Sierra records (bibliographic and order data)
to be inserted into the DB.
In case of e-resouces only bib informatin is considered.
Source MARC files for e-resources will have a mix of various formats (ebooks, eaudio,
evideo)
"""
# from collections import namedtuple
import pickle
from typing import Any, Iterator, Optional

from pymarc import Field
from bookops_marc import SierraBibReader, Bib


from .constants import LIBRARIES, RESOURCE_CATEGORIES
from .datastore import Resource


class BibReader:
    def __init__(
        self,
        marc_fh: str,
        library: str,
        hide_utf8_warnings: bool = True,
    ) -> None:
        """
        Iterator that yields bib information extracted from MARC records
        Args:
            marc_fh:                        MARC file's path
            library:                        'nyp' or 'bpl'
            hide_utf8_warnings:             hides character encoding warnings
        """

        if library not in LIBRARIES.keys():
            raise ValueError("Invalid 'library' argument. Must be 'nyp' or 'bpl'.")

        self.marc_fh = marc_fh
        self.library = library
        self.hide_utf8_warnings = hide_utf8_warnings

    def __iter__(self) -> Iterator[Resource]:
        with open(self.marc_fh, "rb") as marcfile:
            reader = SierraBibReader(
                marcfile, hide_utf8_warnings=self.hide_utf8_warnings
            )
            for bib in reader:
                resource_category = self._determine_resource_category(bib)

                # skip any unmapped resource types from processing
                if not resource_category:
                    continue
                else:
                    bib_info = self._map_data(bib, resource_category)
                    yield bib_info
                    # log a warning

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
        if control_number.startswith("ODN"):
            return "eresource"
        else:
            # future hook
            # determine particular resource category for print material here
            # based it on order information from the 960/961 tags
            return None

    def _pickle_obj(self, obj: Any) -> bytes:
        return pickle.dumps(obj)

    def _fields2keep(self, bib: Bib, resource_category: str) -> list[Field]:
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
        otherNumber = bib.upc_number()
        srcFieldsToKeep = self._fields2keep(bib, resource_category)
        standardNumber = bib.isbn()

        return Resource(
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
            otherNumber=otherNumber,
            srcFieldsToKeep=srcFieldsToKeep,
            standardNumber=standardNumber,
            status="open",
        )
