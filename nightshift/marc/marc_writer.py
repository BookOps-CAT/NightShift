# -*- coding: utf-8 -*-

"""
This module provides methods for manipulation and serialization of Worldcat responses
into MARC21.
"""
import logging
import pickle

from pymarc import Field

from .. import __title__, __version__
from ..constants import (
    library_by_id,
    resource_category_by_id,
    sierra_format_code,
    tags2delete,
)
from ..datastore import Resource
from .marc_parser import worldcat_response_to_pymarc


logger = logging.getLogger("nightshift")


DELETE_TAGS = tags2delete()
LIB_IDX = library_by_id()
RES_IDX = resource_category_by_id()
SIERRA_FORMAT = sierra_format_code()


class BibEnhancer:
    """
    A class used for upgrading MARC records.
    """

    def __init__(self, resource: Resource) -> None:
        """
        Initiates BibEnhancer.

        Args:
            resource:                       `datastore.Resource` instance
        """

        self.library = LIB_IDX[resource.libraryId]

        logger.info(f"Enhancing {self.library} Sierra bib # b{resource.sierraId}a.")

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

        Raises:
            OSError
        """
        try:
            with open("temp.mrc", "ab") as out:
                out.write(self.bib.as_marc())
                logger.debug(
                    f"Saving to file {self.library} record b{self.resource.sierraId}a."
                )
        except OSError as exc:
            logger.error(f"Unable to save record to a temp file. Error {exc}.")
            raise

    def _add_call_number(self) -> None:
        """
        Adds a call number MARC tag specific to resource category and each library.

        !!Creation of call numbers will be moved to a separate module or even package
        when print materials will be incorporated into the process (due to complexity)!!
        """
        resource_cat = RES_IDX[self.resource.resourceCategoryId]
        if self.library == "NYP":
            tag = "091"
            if resource_cat == "ebook":
                value = "eNYPL Book"
            elif resource_cat == "eaudio":
                value = "eNYPL Audio"
            elif resource_cat == "evideo":
                value = "eNYPL Video"
            else:
                value = None

        elif self.library == "BPL":
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
            callno = Field(tag=tag, indicators=[" ", " "], subfields=["a", value])
            self.bib.add_field(callno)
            logger.debug(
                f"Added {callno.value()} to {self.library} b{self.resource.sierraId}a."
            )
        else:
            logger.warning(
                f"Attempting to create a call number for unsupported resource category for "
                f"{self.library} b{self.resource.sierraId}a."
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
        command_tag = Field(
            tag="949",
            indicators=[" ", " "],
            subfields=[
                "a",
                f"*{command_str};",
            ],
        )
        self.bib.add_field(command_tag)
        logger.debug(
            f"Added 949 command tag: {command_tag.value()} to {self.library} b{self.resource.sierraId}a."
        )

    def _add_local_tags(self) -> None:
        """
        Adds local tags to the WorldCat bib.
        """
        if self.resource.srcFieldsToKeep:
            tags2keep = pickle.loads(self.resource.srcFieldsToKeep)
            fields = []
            for tag in tags2keep:
                self.bib.add_ordered_field(tag)
                fields.append(tag.tag)
            logger.debug(
                f"Added following local fields {fields} to {self.library} b{self.resource.sierraId}a."
            )
        else:
            logger.debug(
                f"No local tags to keep were found for {self.library} b{self.resource.sierraId}a."
            )

    def _add_initials_tag(self) -> None:
        """
        Marks records as produced by the NightShift bot.
        """
        if self.library == "NYP":
            tag = "901"
        elif self.library == "BPL":
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
        logger.debug(
            f"Added initials tag {tag} to {self.library} b{self.resource.sierraId}a."
        )

    def _purge_tags(self) -> None:
        """
        Removes MARC tags indicated in `constants.RESOURCE_CATEGORIES`
        from the WorldCat bib.
        """
        for tag in DELETE_TAGS[self.resource.resourceCategoryId]:
            if tag in self.bib:
                self.bib.remove_fields(tag)
        logger.debug(
            f"Removed {DELETE_TAGS[self.resource.resourceCategoryId]} from {self.library} b{self.resource.sierraId}a."
        )
