# -*- coding: utf-8 -*-

"""
This module provides methods for manipulation and serialization of Worldcat responses
into MARC21.
"""
import logging
import pickle


from ..constants import library_by_nid, tags2delete
from ..datastore import Resource
from .marc_parser import worldcat_response_to_pymarc


logger = logging.getLogger("nightshift")


DELETE_TAGS = tags2delete()
LIB_IDX = library_by_nid()


class BibEnhancer:
    """
    A class representing an enhanced MARC record configured specifically
    for import to Sierra.
    """

    def __init__(self, resource: Resource) -> None:

        self.library = LIB_IDX[resource.libraryId]

        logger.info(
            f"Enhancing {self.library.upper()} Sierra bib # b{resource.sierraId}a."
        )

        self.resource = resource
        self.bib = worldcat_response_to_pymarc(resource.fullBib)

    def manipulate(self) -> None:
        """
        Manipulates WorldCat record according to `nightshift.constants` module
        specs.
        """
        # delete unwanted MARC tags
        self._purge_tags()

        # add tags from the local bib
        self._add_local_tags()

        # add call number field
        self._add_call_number()

        # add Sierra import command tag
        self._add_command_tag()

        # add bot's initials
        self._add_initials_tag()

    def save2file(self) -> None:
        """
        Appends bib to a temporary dump file.
        """
        with open("temp.mrc" "ab") as out:  # what the location should be?
            out.write(self.bib.as_marc())

    def _add_call_number(self) -> None:
        """
        Adds a call number MARC tag specific to resource category and each library.
        """
        pass

    def _add_command_tag() -> None:
        """
        Adds Sierra's command MARC tag specific to resource category and each library.
        Includes Sierra bib format, suppression, etc.
        """
        pass

    def _add_local_tags(self) -> None:
        """
        Adds local tags to the WorldCat bib.
        """
        tags2keep = pickle.load(self.resource.srcFieldsToKeep)
        self.bib.add_ordered_field(tags2keep)

    def _add_initials_tag(self) -> None:
        """
        Marks records as produced by the NightShift bot.
        """
        pass

    def _purge_tags(self) -> None:
        """
        Removes MARC tags indicated in `constants.RESOURCE_CATEGORIES` from the WorldCat bib.
        """
        self.bib.remove_fields(DELETE_TAGS[self.resource.resourceCategoryId])
