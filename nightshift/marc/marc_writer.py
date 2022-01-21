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

    To initiate the class pass `nightshift.datastore.Resource` instance that
    was updated with WorldCat MARC XML record.

    Invoking `manipulate()` method on the instance of this class does the following:
     - encodes matching Sierra bib to be overlaid (matching bib # in 907 - BPL
        or 945 - NYPL)
     - removes unwanted MARC tags specified in `constants.RESOURCE_CATEGORIES`,
     - deletes 6xx from unsupported thesauri,
     - adds local tags preserved from the original Sierra bib specified in
        `constants.RESOURCE_CATEGORIES`
     - creates for each resource type an appropriate call number tag (099 for BPL or
        091 for NYPL)
     - adds a Sierra command tag in the 949 tag that specifies Sierra bib format
        code and optionally Sierra bib code 3 for records suppressed from public view
    - adds Nightshift name and version to bib Sierra initials MARC tag (947 for BPL,
        901 for NYPL)
    - removes OCLC prefix from the 001 tag for NYPL records

    Calling `save2file()` method on the instance of this class serializes pymarc object
    into MARC 21 and saves it to a temporary file.
    """

    def __init__(self, resource: Resource) -> None:
        """
        Initiates BibEnhancer by parsing WorldCat MARC XML byte string received
        from MetadataAPI service.

        Args:
            resource:                       `datastore.Resource` instance

        Raises:
            TypeError
        """

        self.library = LIB_IDX[resource.libraryId]

        logger.info(f"Enhancing {self.library} Sierra bib # b{resource.sierraId}a.")

        self.resource = resource
        try:
            self.bib = worldcat_response_to_pymarc(resource.fullBib)
        except TypeError:
            logger.error("Unable to serialize Worldcat response to pymarc object.")
            raise

    def manipulate(self) -> None:
        """
        Manipulates WorldCat record according to `nightshift.constants` module
        specs.

        Full manipulation happens only if call number can be cosntructed.
        """

        # add call number field
        call_number = self._add_call_number()

        if call_number:
            # delete unwanted MARC tags
            self._purge_tags()

            # remove 6xx tags with terms from unsupported thesauri
            self._remove_unsupported_subject_tags()

            # add tags from the local bib
            self._add_local_tags()

            # add Sierra bib # for overlaying
            self._add_sierraId()

            # add Sierra import command tag
            self._add_command_tag()

            # add bot's initials
            self._add_initials_tag()

            # prep OCLC control number
            if self.library == "NYP":
                self._digits_only_in_tag_001()

    def save2file(self, file_path: str = "temp.mrc") -> None:
        """
        Appends bib as MARC21 to a temporary dump file.

        Args:
            file_path:                    path of the file to output records

        Raises:
            OSError
        """
        if self.bib is not None:
            try:
                with open(file_path, "ab") as out:
                    out.write(self.bib.as_marc())
                    logger.debug(
                        f"Saving to file {self.library} record "
                        f"b{self.resource.sierraId}a."
                    )
            except OSError as exc:
                logger.error(f"Unable to save record to a temp file. Error {exc}.")
                raise
        else:
            logger.warning("No pymarc object to serialize to MARC21.")

    def _add_call_number(self) -> bool:
        """
        Adds a call number MARC tag specific to resource category and each library.

        !!Creation of call numbers will be moved to a separate module or even package
        when print materials will be incorporated into the process (due to complexity)!!

        Returns:
            bool
        """
        try:
            resource_cat = RES_IDX[self.resource.resourceCategoryId]
        except KeyError:
            resource_cat = None

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
            call_number = Field(tag=tag, indicators=[" ", " "], subfields=["a", value])
            self.bib.add_field(call_number)
            logger.debug(
                f"Added {call_number.value()} to {self.library} "
                f"b{self.resource.sierraId}a."
            )
            return True
        else:
            self.bib = None
            logger.warning(
                f"Unable to create call number for {self.library} "
                f"b{self.resource.sierraId}a."
            )
            return False

    def _add_command_tag(self) -> None:
        """
        Adds Sierra's command MARC tag (949) specific to resource category and
        each library.

        Includes commands for matching Sierra bib # (ov=) Sierra bib format (b2=), and
        optional suppression (b2=) code.
        """
        commands = []

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
            f"Added 949 command tag: {command_tag.value()} to {self.library} "
            f"b{self.resource.sierraId}a."
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
                f"Added following local fields {fields} to {self.library} "
                f"b{self.resource.sierraId}a."
            )
        else:
            logger.debug(
                f"No local tags to keep were found for {self.library} "
                f"b{self.resource.sierraId}a."
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

    def _add_sierraId(self) -> None:
        """
        Adds 907 (BPL) or 945 (NYP) tag to manipulated MARC record for
        matching/overlaying purposes.
        """
        if self.library == "NYP":
            overlay_tag = "945"
        elif self.library == "BPL":
            overlay_tag = "907"
        self.bib.add_field(
            Field(
                tag=overlay_tag,
                indicators=[" ", " "],
                subfields=["a", f".b{self.resource.sierraId}a"],
            )
        )

    def _digits_only_in_tag_001(self) -> None:
        """
        Removes OCLC control number prefix from the 001 tag
        """
        controlNo = self.bib["001"].data
        controlNo_without_prefix = self._remove_oclc_prefix(controlNo)
        self.bib["001"].data = controlNo_without_prefix

    def _purge_tags(self) -> None:
        """
        Removes MARC tags indicated in `constants.RESOURCE_CATEGORIES`
        from the WorldCat bib.
        """
        for tag in DELETE_TAGS[self.resource.resourceCategoryId]:
            if tag in self.bib:
                self.bib.remove_fields(tag)
        logger.debug(
            f"Removed {DELETE_TAGS[self.resource.resourceCategoryId]} from "
            f"{self.library} b{self.resource.sierraId}a."
        )

    def _remove_oclc_prefix(self, controlNo: str) -> str:
        """
        Returns control number that consist only of digits
        """
        if controlNo.startswith("ocm") or controlNo.startswith("ocn"):
            return controlNo[3:]
        elif controlNo.startswith("on"):
            return controlNo[2:]
        else:
            return controlNo

    def _remove_unsupported_subject_tags(self) -> None:
        """
        Removes from the bib any 6xx tags that include terms from unsupported
        thesauri.
        Acceptable terms: LCSH, FAST, GSFAD, LCGFT, lCTGM
        """
        for field in self.bib.subjects():
            # delete locally coded tags
            if field.tag.startswith("69"):
                self.bib.remove_field(field)
                logger.debug(
                    "Local term in subject tag. "
                    f"Removed {field.tag} from {self.library} "
                    f"b{self.resource.sierraId}a."
                )

            elif field.indicator2 == "0":  # LCSH
                pass
            elif field.indicator2 == "7":
                term_src = field["2"]
                if term_src.lower() in ("lcsh", "fast", "gsafd", "lcgft", "lctgm"):
                    pass
                else:
                    logger.debug(
                        "Unsupported thesaurus. "
                        f"Removed {field} from {self.library} "
                        f"b{self.resource.sierraId}a."
                    )
                    self.bib.remove_field(field)
            else:
                self.bib.remove_field(field)
