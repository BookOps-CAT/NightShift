# -*- coding: utf-8 -*-

"""
This module provides methods for manipulation and serialization of Worldcat responses
into MARC21.
"""
import logging
import pickle

from pymarc import Field, Subfield

from .. import __title__, __version__
from ..datastore import Resource
from ..datastore_transactions import ResCatById
from .marc_parser import worldcat_response_to_bib


logger = logging.getLogger("nightshift")


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
     - enforces presence of appropriate genre heading in 655 ('Electronic books', etc.)
     - adds a Sierra command tag in the 949 tag that specifies Sierra bib format
        code and optionally Sierra bib code 3 for records suppressed from public view
    - adds Nightshift name and version to bib Sierra initials MARC tag (947 for BPL,
        901 for NYPL)
    - removes OCLC prefix from the 001 tag for NYPL records

    Calling `save2file()` method on the instance of this class serializes pymarc object
    into MARC 21 and saves it to a temporary file.
    """

    def __init__(
        self,
        resource: Resource,
        library: str,
        resource_categories: dict[int, ResCatById],
    ) -> None:
        """
        Initiates BibEnhancer by parsing WorldCat MARC XML byte string received
        from MetadataAPI service.

        Args:
            resource:                       `datastore.Resource` instance
            library:                        'NYP' or 'BPL'
                                            as a key and code as value
            resource_categories:            resource categories data with
                                            `datastore.ResourceCategory.nid` as key

        Raises:
            TypeError
        """
        self.resource = resource
        self.library = library
        self._res_cat = resource_categories

        logger.info(f"Enhancing {self.library} Sierra bib # b{resource.sierraId}a.")

        self.bib = worldcat_response_to_bib(resource.fullBib, self.library)

    def manipulate(self) -> None:
        """
        Manipulates WorldCat record according to `nightshift.constants` module
        specs.

        Full manipulation happens only if records meets minimum requirements and
        a call number can be constructed.
        """

        # delete unwanted MARC tags
        self._purge_tags()

        # remove 6xx tags with terms from unsupported thesauri
        self.bib.remove_unsupported_subjects()

        # remove e-resources vendor tags
        self._remove_eresource_vendors()

        # if does not meet criteria delete Worldcat bib
        if not self._is_acceptable():
            logger.info(
                f"Worldcat record # {self.resource.oclcMatchNumber} is rejected. "
                "Does not meet minimum requirements."
            )
            self.bib = None
        else:
            logger.info(
                f"Worldcat record # {self.resource.oclcMatchNumber} is acceptable. "
                "Meets minimum requirements."
            )

            # genre tags
            self._clean_up_genre_tags()

            # add tags from the local bib
            self._add_local_tags()

            # add Sierra bib # for overlaying
            self._add_sierraId()

            # add Sierra import command tags
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
            resource_cat = self._res_cat[self.resource.resourceCategoryId].name
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
            call_number = Field(
                tag=tag, indicators=[" ", " "], subfields=[Subfield("a", value)]
            )
            self.bib.add_field(call_number)
            logger.debug(
                f"Added {call_number.value()} to {self.library} "
                f"b{self.resource.sierraId}a."
            )
            return True
        else:
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
        if self.library == "NYP":
            sierra_format_code = self._res_cat[
                self.resource.resourceCategoryId
            ].sierraBibFormatNyp
        elif self.library == "BPL":
            sierra_format_code = self._res_cat[
                self.resource.resourceCategoryId
            ].sierraBibFormatBpl

        commands.append(f"b2={sierra_format_code}")

        # Sierra suppression code
        if self.resource.suppressed:
            commands.append("b3=n")

        # set default location
        if self.library == "NYP":
            commands.append("bn=ia")
        elif self.library == "BPL":
            commands.append("bn=elres")

        command_str = ";".join(commands)

        # add command to bib
        command_tag = Field(
            tag="949",
            indicators=[" ", " "],
            subfields=[
                Subfield("a", f"*{command_str};"),
            ],
        )
        self.bib.add_field(command_tag)
        logger.debug(
            f"Added 949 command tag: {command_tag.value()} to {self.library} "
            f"b{self.resource.sierraId}a."
        )

    def _clean_up_genre_tags(self) -> None:
        """
        Adds genre tags to e-resources.
        """
        try:
            resource_cat = self._res_cat[self.resource.resourceCategoryId].name
        except KeyError:
            resource_cat = None

        if resource_cat == "ebook":
            for field in self.bib.subjects:
                if "electronic books" in field.value().lower():
                    self.bib.remove_field(field)

        elif resource_cat == "eaudio":

            # 'Audiobooks' term
            # remove electronic audiobooks
            for field in self.bib.subjects:
                if "electronic audiobooks" in field.value().lower():
                    self.bib.remove_field(field)

            # but keep lcgft audiobooks
            found = False
            for field in self.bib.subjects:
                if "audiobooks." in field.value().lower():
                    found = True
                    break
            if not found:
                self.bib.add_field(
                    Field(
                        tag="655",
                        indicators=[" ", "7"],
                        subfields=[
                            Subfield("a", "Audiobooks."),
                            Subfield("2", "lcgft"),
                        ],
                    )
                )
                logger.debug("Added 'Audiobooks' LCGFT genre to 655 tag.")

        elif resource_cat == "evideo":
            found = False
            for field in self.bib.subjects:
                if "internet videos." in field.value().lower():
                    found = True
                    break
            if not found:
                self.bib.add_field(
                    Field(
                        tag="655",
                        indicators=[" ", "7"],
                        subfields=[
                            Subfield("a", "Internet videos."),
                            Subfield("2", "lcgft"),
                        ],
                    )
                )
                logger.debug("Added 'Internet videos' LCGFT genre to 655 tag.")

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
                subfields=[Subfield("a", f"{__title__}/{__version__}")],
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
                subfields=[Subfield("a", f".b{self.resource.sierraId}a")],
            )
        )

    def _digits_only_in_tag_001(self) -> None:
        """
        Removes OCLC control number prefix from the 001 tag
        """
        controlNo = self.bib["001"].data
        controlNo_without_prefix = self._remove_oclc_prefix(controlNo)
        self.bib["001"].data = controlNo_without_prefix

    def _is_acceptable(self) -> bool:
        """
        Checks if full Worldcat record meet minimum criteria and
        a valid call number can be constructed.
        """
        if self._meets_minimum_criteria() and self._add_call_number():
            return True
        else:
            return False

    def _meets_minimum_criteria(self) -> bool:
        """
        Checks if Worldcat record meets minimum criteria
        """
        # check uppercase title (indicates poor quality)
        if self.bib.title.isupper():
            logger.debug("Worldcat record failed uppercase title test.")
            return False

        # missing statement of responsibility
        if "c" not in self.bib["245"]:
            logger.debug("Worldcat record failed statement of resp. test.")
            return False

        # no physical description
        if "300" not in self.bib:
            logger.debug("Worldcat record failed physical desc. test.")
            return False

        # messed up diacritics indicated by presence of "©" (b"\xc2\xa9") or
        # "℗" (b"\xe2\x84\x97")
        try:
            diacritics_msg = "Worldcat record failed characters encoding test."
            if b"\xc2\xa9" in bytes(self.bib.author, "utf-8") or b"\xc2\xa9" in bytes(
                self.bib.title, "utf-8"
            ):
                logger.debug(diacritics_msg)
                return False

            if b"\xe2\x84\x97" in bytes(
                self.bib.author, "utf-8"
            ) or b"\xe2\x84\x97" in bytes(self.bib.title, "utf-8"):
                logger.debug(diacritics_msg)
                return False

        except TypeError:
            # hanldes records without the author
            pass

        # has at least one valid subject tag
        if not self.bib.subjects:
            logger.debug("Worldcat record failed subjects test.")
            return False

        logger.debug("Worldcat record meets minimum criteria.")
        return True

    def _purge_tags(self) -> None:
        """
        Removes MARC tags indicated in `constants.RESOURCE_CATEGORIES`
        from the WorldCat bib.
        """
        try:
            for tag in self._res_cat[self.resource.resourceCategoryId].dstTags2Delete:
                if tag in self.bib:
                    self.bib.remove_fields(tag)
            logger.debug(
                f"Removed {self._res_cat[self.resource.resourceCategoryId].dstTags2Delete} from "
                f"{self.library} b{self.resource.sierraId}a."
            )
        except KeyError:
            logger.warning("Encountered unsupported resource category.")

    def _remove_eresource_vendors(self) -> None:
        """
        Removes from e-resource bib any tags indicating distributor
        """
        vendors = ["overdrive", "cloudlibrary", "3m", "recorded books"]

        for tag in self.bib.get_fields("710"):
            for vendor in vendors:
                if vendor in tag.value().lower():
                    self.bib.remove_field(tag)

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
