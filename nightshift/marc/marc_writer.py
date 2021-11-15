# -*- coding: utf-8 -*-

"""
This module provides methods for manipulation and serialization of Worldcat responses
into MARC21.
"""
import logging
import pickle

from pymarc import Field
from pymarc.exceptions import FieldNotFound

from .. import __title__, __version__
from ..constants import (
    library_by_nid,
    resource_category_by_nid,
    sierra_format_code,
    tags2delete,
)
from ..datastore import Resource
from .marc_parser import worldcat_response_to_pymarc


logger = logging.getLogger("nightshift")


DELETE_TAGS = tags2delete()
LIB_IDX = library_by_nid()
RES_IDX = resource_category_by_nid()
SIERRA_FORMAT = sierra_format_code()


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
        Creation of call numbers will be moved to a separate module when print materials
        will be incorporated into the process (due to complexity).
        """
        resource_cat = RES_IDX[self.resource.resourceCategoryId]
        if self.library == "nyp":
            tag = "091"
            if resource_cat == "ebook":
                value = "eNYPL Book"
            elif resource_cat == "eaudio":
                value = "eNYPL Audio"
            elif resource_cat == "evideo":
                value = "eNYPL Video"
            else:
                value = None

        elif self.library == "bpl":
            tag = "099"
            if resource_cat == "ebook":
                value = "eBOOK"
            elif resource_cat == "eaudio":
                value = "eAUDIO"
            elif resource_cat == "evideo":
                value = "eVIDEO"
            else:
                value = None
        else:
            tag = None
            value = None

        if tag and value:
            self.bib.add_field(
                Field(tag=tag, indicators=[" ", " "], subfields=["a", value])
            )
        else:
            logger.warning(
                f"Attempting to create a call number for unsupported resource category for "
                f"{self.library.upper()} b{self.resource.sierraId}a."
            )

    def _add_command_tag(self) -> None:
        """
        Adds Sierra's command MARC tag specific to resource category and each library.
        Includes Sierra bib format, suppression, etc.
        """
        commands = []

        # Sierra bib # matching point
        commands.append(f"ov=b{self.resource.sierraId}a")

        # Sierra bib format
        sierra_format_code = SIERRA_FORMAT[self.resource.resourceCategoryId][
            self.library
        ]
        commands.append(f"b2={sierra_format_code}")

        # Sierra suppression code
        if self.resource.suppressed:
            commands.append("b3=n")

        command_str = ";".join(commands)

        # add command to bib
        self.bib.add_field(
            Field(
                tag="949",
                indicators=[" ", " "],
                subfields=[
                    "a",
                    f"*{command_str};",
                ],
            )
        )

    def _add_local_tags(self) -> None:
        """
        Adds local tags to the WorldCat bib.
        """
        if self.resource.srcFieldsToKeep:
            tags2keep = pickle.loads(self.resource.srcFieldsToKeep)
            for tag in tags2keep:
                self.bib.add_ordered_field(tag)

    def _add_initials_tag(self) -> None:
        """
        Marks records as produced by the NightShift bot.
        """
        if self.library == "nyp":
            tag = "901"
        elif self.library == "bpl":
            tag = "947"
        else:
            return

        self.bib.add_field(
            Field(
                tag=tag,
                indicators=[" ", " "],
                subfields=["a", f"{__title__}/{__version__}"],
            )
        )

    def _purge_tags(self) -> None:
        """
        Removes MARC tags indicated in `constants.RESOURCE_CATEGORIES`
        from the WorldCat bib.
        """
        for tag in DELETE_TAGS[self.resource.resourceCategoryId]:
            try:
                self.bib.remove_fields(tag)
            except FieldNotFound:
                pass
