"""Read and parse MARC Sierra records (bib and order data) to be inserted into the DB.

In case of e-resources only bib information is considered. Source MARC files
for e-resources will have a mix of various formats (ebooks, eaudio, evideo).
"""

import logging
import pickle
from collections.abc import Iterator
from io import BytesIO
from typing import BinaryIO

from bookops_marc import Bib, SierraBibReader

from ..datastore import Resource
from ..datastore_transactions import ResCatByName

logger = logging.getLogger("nightshift")


class BibReader:
    """Iterator class that extracts Sierra bib data from a file or bytes stream."""

    def __init__(
        self,
        marc_target: str | BytesIO | BinaryIO,
        library: str,
        libraryId: int,
        resource_categories: dict[str, ResCatByName],
    ) -> None:
        """Initialize a `BibReader` instance.

        Args:
            marc_target:                    MARC file or file-like object
            library:                        'NYP' or 'BPL'
            libraryId:                     `datastore.Library.nid`
            resource_categories:            a dictionary of resource categories and
                                            data associated them as namedtuple.

        """
        logger.info("Initiating BibReader.")

        if not isinstance(marc_target, str) and not hasattr(marc_target, "read"):
            logger.error(
                f"Invalid 'marc_target' argument: '{marc_target}' "
                f"({type(marc_target).__name__})"
            )
            raise TypeError(
                "Invalid 'marc_target' argument. Must be path or file-like object."
            )

        if not isinstance(resource_categories, dict):
            logger.error("Invalid 'resource_categories' argument.")
            raise TypeError(
                "Invalid 'resource_categories' argument. Must be a dictionary with a "
                "name as key, and value as a namedtuple (ResCatName)."
            )

        self.library = library
        self.libraryId = libraryId
        self.marc_target = marc_target
        self._res_cat = resource_categories

    def __iter__(self) -> Iterator[Resource]:
        """Yield parsed `Resource` instances from the MARC target."""
        if isinstance(self.marc_target, str):
            with open(self.marc_target, "rb") as fh:
                yield from self._read_bibs(fh)
        else:
            yield from self._read_bibs(self.marc_target)

    def _read_bibs(self, fh: BytesIO | BinaryIO) -> Iterator[Resource]:
        for bib in SierraBibReader(fh, hide_utf8_warnings=True):
            if category := self._determine_resource_category(bib):
                yield self._map_data(bib, self._res_cat[category])

    def _determine_resource_category(self, bib: Bib) -> str | None:
        """Determine resource category based on bib and order information."""
        record_types = {"a": "ebook", "i": "eaudio", "g": "evideo", None: None}

        control_number = getattr(bib, "control_number", None)

        if not (control_number and control_number.startswith("ODN")):
            # Future hook: determine res cat for print material via 960/961 tags
            logger.warning(
                f"Unsupported bib type. Unable to ingest {self.library} bib # "
                f"{bib.sierra_bib_id}."
            )
            return None

        return record_types.get(bib.record_type)

    def _map_data(self, bib: Bib, category_info: ResCatByName) -> Resource:
        """Map selected data from `Bib` to `datastore.Resource` table columns."""
        tags2keep = list(bib.get_fields(*category_info.srcTags2Keep))

        resource = Resource(
            sierraId=bib.sierra_bib_id_normalized,
            libraryId=self.libraryId,
            resourceCategoryId=category_info.nid,
            bibDate=bib.created_date,
            author=bib.author,
            title=bib.title,
            pubDate=bib.pubyear,
            congressNumber=bib.lccn,
            controlNumber=bib.control_number,
            distributorNumber=bib.overdrive_number,
            suppressed=bib.suppressed,
            otherNumber=bib.upc_number,
            srcFieldsToKeep=pickle.dumps(tags2keep),
            standardNumber=bib.isbn,
            status="open",
        )
        logger.debug(f"Parsed resource: {resource}")
        return resource
